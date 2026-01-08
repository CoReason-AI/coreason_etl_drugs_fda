# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import io
import zipfile
from typing import Any, Dict, Iterator, List

import dlt
import polars as pl
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from coreason_etl_drugs_fda.files import _get_lazy_df_from_zip, _read_file_from_zip
from coreason_etl_drugs_fda.gold import ProductGold
from coreason_etl_drugs_fda.silver import ProductSilver
from coreason_etl_drugs_fda.transform import (
    extract_orig_dates,
    prepare_gold_products,
    prepare_silver_products,
    to_snake_case,
)
from coreason_etl_drugs_fda.utils.logger import logger

# List of files to extract from the FDA ZIP archive
TARGET_FILES = [
    "Products.txt",
    "Applications.txt",
    "MarketingStatus.txt",
    "TE.txt",
    "Submissions.txt",
    "Exclusivity.txt",
    "MarketingStatus_Lookup.txt",
]


@dlt.source  # type: ignore[misc]
def drugs_fda_source(base_url: str = "https://www.fda.gov/media/89850/download") -> Iterator[DltResource]:
    """
    The dlt source for Drugs@FDA.
    Downloads the ZIP file and yields resources for each target TSV file.
    Also yields a 'silver_products' resource with enriched data.
    """
    logger.info(f"Starting Drugs@FDA download from {base_url}")
    # Use dlt's built-in requests helper for retries and timeouts
    response = requests.get(base_url, stream=True)
    response.raise_for_status()
    zip_bytes = response.content

    files_present = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        all_files = set(z.namelist())
        for target in TARGET_FILES:
            if target in all_files:
                files_present.append(target)
            else:
                logger.warning(f"Expected file {target} not found in ZIP archive.")

    logger.info(f"Found {len(files_present)} target files in archive.")

    # 1. Yield Raw Resources (Bronze)
    for filename in files_present:
        # Use explicit 'fda_drugs_' prefix to avoid 'fd_aa_drugs_' normalization
        # CORRECTED: Single underscore between 'fda' and product name.
        resource_name = f"fda_drugs_bronze_fda_{to_snake_case(filename.replace('.txt', ''))}"

        @dlt.resource(  # type: ignore[misc]
            name=resource_name,
            write_disposition="replace",
            schema_contract={"columns": "evolve"},
        )
        def file_resource(fname: str = filename, z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            # For Raw, we just yield what we read (clean but not normalized types)
            yield from _read_file_from_zip(z_content, fname)

        yield file_resource()

    # 2. Yield Silver Products Resource
    if "Products.txt" in files_present and "Submissions.txt" in files_present:

        @dlt.resource(  # type: ignore[misc]
            name="fda_drugs_silver_products",
            write_disposition="merge",
            primary_key="coreason_id",
            schema_contract={"columns": "evolve"},
        )
        def silver_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductSilver]:
            logger.info("Generating Silver Products layer...")

            # 1. Extract Dates Map
            submissions_lazy = _get_lazy_df_from_zip(z_content, "Submissions.txt")
            approval_map = extract_orig_dates(submissions_lazy)

            # 2. Get Products
            products_lazy = _get_lazy_df_from_zip(z_content, "Products.txt")
            if products_lazy.collect_schema().len() == 0:
                # Missing or empty products
                pass

            # 3. Transform
            # Re-inject map logic here
            dates_df_eager = pl.DataFrame(
                {"appl_no": list(approval_map.keys()), "original_approval_date": list(approval_map.values())}
            )
            if dates_df_eager.is_empty():
                dates_df_eager = pl.DataFrame(schema={"appl_no": pl.String, "original_approval_date": pl.String})
            else:
                dates_df_eager = dates_df_eager.with_columns(pl.col("appl_no").cast(pl.String))

            dates_df_lazy = dates_df_eager.lazy()

            df_lazy = prepare_silver_products(
                products_lazy, dates_df_lazy, approval_dates_map_exists=not dates_df_eager.is_empty()
            )

            # Collect before iteration
            df = df_lazy.collect()

            # Validate rows against Pydantic model
            for row in df.to_dicts():
                # Filter out rows with null ID keys (critical for Linkage)
                if not row.get("appl_no") or not row.get("product_no"):
                    appl = row.get("appl_no")
                    prod = row.get("product_no")
                    logger.warning(f"Skipping row with missing keys: ApplNo={appl}, ProductNo={prod}")
                    continue
                yield ProductSilver(**row)
            logger.info("Silver Products layer generation complete.")

        yield silver_products_resource()

    # 3. Yield Gold Products Resource
    if "Products.txt" in files_present:

        @dlt.resource(  # type: ignore[misc]
            name="fda_drugs_gold_drug_product",
            write_disposition="replace",
            schema_contract={"columns": "evolve"},
        )
        def gold_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductGold]:
            logger.info("Generating Gold Products layer...")

            # 1. Base Silver
            approval_map: dict[str, str] = {}
            if "Submissions.txt" in files_present:
                submissions_lazy = _get_lazy_df_from_zip(z_content, "Submissions.txt")
                approval_map = extract_orig_dates(submissions_lazy)

            dates_df_eager = pl.DataFrame(
                {"appl_no": list(approval_map.keys()), "original_approval_date": list(approval_map.values())}
            )
            if dates_df_eager.is_empty():
                dates_df_eager = pl.DataFrame(schema={"appl_no": pl.String, "original_approval_date": pl.String})
            else:
                dates_df_eager = dates_df_eager.with_columns(pl.col("appl_no").cast(pl.String))
            dates_df_lazy = dates_df_eager.lazy()

            products_lazy = _get_lazy_df_from_zip(z_content, "Products.txt")
            silver_df_lazy = prepare_silver_products(
                products_lazy, dates_df_lazy, approval_dates_map_exists=not dates_df_eager.is_empty()
            )

            # 2. Aux Files
            df_apps = _get_lazy_df_from_zip(z_content, "Applications.txt")
            df_marketing = _get_lazy_df_from_zip(z_content, "MarketingStatus.txt")
            df_te = _get_lazy_df_from_zip(z_content, "TE.txt")
            df_exclusivity = _get_lazy_df_from_zip(z_content, "Exclusivity.txt")
            df_marketing_lookup = _get_lazy_df_from_zip(z_content, "MarketingStatus_Lookup.txt")

            # 3. Transform Gold
            gold_df_lazy = prepare_gold_products(
                silver_df_lazy, df_apps, df_marketing, df_marketing_lookup, df_te, df_exclusivity
            )

            # Final Collect
            gold_df = gold_df_lazy.collect()

            if gold_df.is_empty():
                return

            for row in gold_df.to_dicts():
                yield ProductGold(**row)

        yield gold_products_resource()
