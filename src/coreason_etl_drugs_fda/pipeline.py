# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import dlt

from coreason_etl_drugs_fda.source import drugs_fda_source
from coreason_etl_drugs_fda.utils.logger import logger
from coreason_etl_drugs_fda.utils.medallion import organize_schemas


def create_pipeline(destination: str = "postgres", dataset_name: str = "fda_data") -> dlt.Pipeline:
    """
    Creates and configures the dlt pipeline.
    """
    pipeline = dlt.pipeline(
        pipeline_name="coreason_drugs_fda",
        destination=destination,
        dataset_name=dataset_name,
        dev_mode=True,  # For development, replace logic
    )
    return pipeline


@logger.catch  # type: ignore[misc]
def run_pipeline() -> None:
    """
    Main entry point to run the pipeline.
    """
    logger.info("Starting Pipeline Execution")
    pipeline = create_pipeline()

    # The source now includes both Raw (Bronze) and Silver resources
    source = drugs_fda_source()

    info = pipeline.run(source)
    logger.info(info)

    # Post-load hook: Organize schemas (for Postgres)
    organize_schemas(pipeline)


if __name__ == "__main__":  # pragma: no cover
    run_pipeline()
