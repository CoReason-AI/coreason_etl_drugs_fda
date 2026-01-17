# Copyright (c) 2025 CoReason, Inc.
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import io
import zipfile
from typing import Any, Dict, Iterator, List, cast

import dlt
import polars as pl
from curl_cffi import requests as cffi_requests
from dlt.sources import DltResource

from coreason_etl_drugs_fda.gold import ProductGold
from coreason_etl_drugs_fda.silver import ProductSilver
from coreason_etl_drugs_fda.transform import (
    clean_dataframe,
    extract_orig_dates,
    prepare_gold_products,
    prepare_silver_products,
    to_snake_case,
)
from coreason_etl_drugs_fda.utils.logger import logger

TARGET_FILES = [
    "Products.txt",
    "Applications.txt",
    "MarketingStatus.txt",
    "TE.txt",
    "Submissions.txt",
    "Exclusivity.txt",
    "MarketingStatus_Lookup.txt",
]


def _read_csv_bytes(content: bytes) -> pl.DataFrame:
    if not content:
        return pl.DataFrame()
    return pl.read_csv(
        content,
        separator="\t",
        quote_char=None,
        encoding="cp1252",
        ignore_errors=True,
        truncate_ragged_lines=True,
        infer_schema_length=10000,
    )


def _read_file_from_zip(zip_content: bytes, filename: str) -> List[Dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if filename not in z.namelist():
            return []
        with z.open(filename) as f:
            df = _read_csv_bytes(f.read())
            df = clean_dataframe(df)
            return cast(List[Dict[str, Any]], df.to_dicts())


def _get_lazy_df_from_zip(zip_content: bytes, filename: str) -> pl.LazyFrame:
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if filename not in z.namelist():
            return pl.DataFrame().lazy()
        with z.open(filename) as f:
            df = _read_csv_bytes(f.read())
            return df.lazy()


@dlt.source(name="drugs_fda")  # type: ignore[misc]
def drugs_fda_source(
    base_url: str = "https://www.fda.gov/media/89850/download",
) -> Iterator[DltResource]:
    """
    The main DLT source for FDA Drugs data.
    Uses curl_cffi to impersonate Chrome and bypass FDA bot detection.
    """
    zip_bytes = b""
    landing_page = "https://www.fda.gov/drugs/drug-approvals-and-databases/drugsfda-data-files"

    logger.info(f"Starting Drugs@FDA download from {base_url} using curl_cffi...")

    try:
        # Impersonate Chrome 120 to pass FDA's TLS fingerprint check
        response = cffi_requests.get(
            base_url,
            impersonate="chrome120",
            headers={
                "Referer": landing_page,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            },
            timeout=120,
        )

        if response.status_code == 200:
            if response.content.startswith(b"PK"):
                zip_bytes = response.content
                logger.info("Download successful via curl_cffi.")
            else:
                # If we get 200 OK but it's text (like the abuse page), fail.
                snippet = response.content[:100].decode(errors="ignore")
                raise ValueError(f"Downloaded content is not a ZIP. Abuse detection triggered? Content: {snippet}")
        else:
            response.raise_for_status()

    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

    # Process ZIP Content
    files_present = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            all_files = set(z.namelist())
            for target in TARGET_FILES:
                if target in all_files:
                    files_present.append(target)
                else:
                    logger.warning(f"Expected file {target} not found in ZIP archive.")
    except zipfile.BadZipFile:
        logger.error("Data downloaded is not a valid ZIP file.")
        raise

    logger.info(f"Found {len(files_present)} target files in archive.")

    # 4. Yield Raw Resources (Bronze)
    for filename in files_present:
        clean_name = to_snake_case(filename.replace(".txt", ""))
        resource_name = f"fda_drugs_bronze_{clean_name}"

        @dlt.resource(
            name=resource_name,
            write_disposition="replace",
            schema_contract={"columns": "evolve"},
        )  # type: ignore[misc]
        def file_resource(fname: str = filename, z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            yield _read_file_from_zip(z_content, fname)

        yield file_resource()

    # 5. Yield Silver Products Resource
    if "Products.txt" in files_present and "Submissions.txt" in files_present:

        @dlt.resource(
            name="fda_drugs_silver_products",
            write_disposition="merge",
            primary_key="coreason_id",
            schema_contract={"columns": "evolve"},
            columns=ProductSilver,
        )  # type: ignore[misc]
        def silver_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductSilver]:
            logger.info("Generating Silver Products layer...")

            submissions_lazy = _get_lazy_df_from_zip(z_content, "Submissions.txt")
            approval_map = extract_orig_dates(submissions_lazy)

            products_lazy = _get_lazy_df_from_zip(z_content, "Products.txt")

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

            df = df_lazy.collect()

            for row in df.to_dicts():
                if not row.get("appl_no") or not row.get("product_no"):
                    continue
                yield cast(ProductSilver, row)
            logger.info("Silver Products layer generation complete.")

        yield silver_products_resource()

    # 6. Yield Gold Products Resource
    if "Products.txt" in files_present:

        @dlt.resource(
            name="fda_drugs_gold_products",
            write_disposition="replace",
            schema_contract={"columns": "evolve"},
            columns=ProductGold,
        )  # type: ignore[misc]
        def gold_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductGold]:
            logger.info("Generating Gold Products layer...")

            approval_map: Dict[str, str] = {}
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

            df_apps = _get_lazy_df_from_zip(z_content, "Applications.txt")
            df_marketing = _get_lazy_df_from_zip(z_content, "MarketingStatus.txt")
            df_te = _get_lazy_df_from_zip(z_content, "TE.txt")
            df_exclusivity = _get_lazy_df_from_zip(z_content, "Exclusivity.txt")
            df_marketing_lookup = _get_lazy_df_from_zip(z_content, "MarketingStatus_Lookup.txt")

            gold_df_lazy = prepare_gold_products(
                silver_df_lazy, df_apps, df_marketing, df_marketing_lookup, df_te, df_exclusivity
            )

            gold_df = gold_df_lazy.collect()

            if gold_df.is_empty():
                return

            for row in gold_df.to_dicts():
                yield cast(ProductGold, row)
            logger.info("Gold Products layer generation complete.")

        yield gold_products_resource()
