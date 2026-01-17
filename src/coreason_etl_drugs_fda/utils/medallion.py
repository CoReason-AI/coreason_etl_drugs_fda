# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from dlt.pipeline.pipeline import Pipeline

from coreason_etl_drugs_fda.utils.logger import logger


def organize_schemas(pipeline: Pipeline) -> None:
    """
    Post-load hook to organize tables into 'bronze', 'silver', and 'gold' schemas
    in the destination (specifically for PostgreSQL).
    """
    # Only proceed if destination supports schemas (Postgres, Redshift, Snowflake, etc.)
    if pipeline.destination.destination_name != "postgres":
        logger.info(f"Skipping schema organization for destination: {pipeline.destination.destination_name}")
        return

    # --- FIX: Use 'with' context manager to open the connection ---
    with pipeline.sql_client() as client:
        # 1. Ensure schemas exist
        schemas = ["bronze", "silver", "gold"]
        for schema in schemas:
            client.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # 2. Get list of tables in the dataset
        dataset_name = pipeline.dataset_name
        loaded_tables = pipeline.default_schema.tables.keys()

        for table_name in loaded_tables:
            target_schema = None

            # Determine target schema based on table name patterns
            if "_bronze_" in table_name or table_name.startswith("bronze_") or "fda_drugs_bronze" in table_name:
                target_schema = "bronze"
            elif "_silver_" in table_name or table_name.startswith("silver_") or "fda_drugs_silver" in table_name:
                target_schema = "silver"
            elif "_gold_" in table_name or table_name.startswith("gold_") or "fda_drugs_gold" in table_name:
                target_schema = "gold"

            if target_schema:
                logger.info(f"Moving table {table_name} to schema {target_schema}")
                try:
                    # Move table to the target schema
                    sql = f'ALTER TABLE "{dataset_name}"."{table_name}" SET SCHEMA "{target_schema}";'
                    client.execute_sql(sql)
                except Exception as e:
                    logger.warning(f"Failed to move table {table_name}: {e}")
