# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from dlt.common.pipeline import Pipeline

from coreason_etl_drugs_fda.utils.logger import logger


def organize_schemas(pipeline: Pipeline) -> None:
    """
    Post-load hook to organize tables into Medallion Architecture schemas (Bronze, Silver, Gold)
    and enforce strict naming conventions (FDA@DRUGS_ prefix).
    """
    logger.info("Starting Medallion Schema Organization...")

    dataset_name = pipeline.dataset_name
    client = pipeline.sql_client()

    # 1. Ensure Target Schemas Exist
    # Note: DuckDB might treat schemas differently, but this is standard SQL/Postgres.
    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        client.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # 2. List Tables in the Source Schema (Dataset)
    # We use information_schema to find tables in the staging/load schema.
    # Note: dataset_name might be quoted or not depending on dialect, handling broadly here.
    query_tables = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{dataset_name}'
          AND table_type = 'BASE TABLE';
    """

    try:
        # execute_sql usually returns results if it's a SELECT?
        # In dlt, execute_sql might returns columns, rows?
        # Use execute_query for fetching.
        with client.execute_query(query_tables) as cursor:
            tables = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.warning(
            f"Could not list tables in schema '{dataset_name}'. "
            f"Ensure the destination supports information_schema. Error: {e}"
        )
        return

    if not tables:
        logger.warning(f"No tables found in schema '{dataset_name}'. Skipping organization.")
        return

    logger.info(f"Found {len(tables)} tables to organize in '{dataset_name}'.")

    # 3. Generate Move and Rename Logic
    sqls = []

    for table in tables:
        # Determine Target Schema
        target_schema = "bronze"
        if table.startswith("dim_"):
            target_schema = "gold"
        elif table.startswith("silver_"):
            target_schema = "silver"

        # Determine Target Name
        # Logic:
        # 1. Strip leading underscores (e.g. _dlt_loads -> dlt_loads)
        # 2. Replace double underscores with single (e.g. raw_fda__products -> raw_fda_products)
        # 3. Add Prefix FDA@DRUGS_

        clean_name = table
        if clean_name.startswith("_"):
            clean_name = clean_name.lstrip("_")

        clean_name = clean_name.replace("__", "_")
        target_name = f"FDA@DRUGS_{clean_name}"

        # Generate SQL
        # Step A: Set Schema
        # Note: We quote names to handle special chars like @ or mixed case if needed.
        # But 'table' from information_schema might be lowercase.
        # We assume standard Postgres identifiers.

        # Move schema
        sqls.append(f'ALTER TABLE "{dataset_name}"."{table}" SET SCHEMA {target_schema};')

        # Rename table (in target schema)
        sqls.append(f'ALTER TABLE {target_schema}."{table}" RENAME TO "{target_name}";')

    # 4. Execute Migrations
    for sql in sqls:
        try:
            client.execute_sql(sql)
            logger.debug(f"Executed: {sql}")
        except Exception as e:
            # We log error but continue, or should we stop?
            # If a move fails, rename might fail.
            logger.error(f"Failed to execute SQL: {sql}. Error: {e}")

    # 5. Optional: Cleanup old schema if empty?
    # User SQL suggested dropping it.
    # "DROP SCHEMA IF EXISTS ... CASCADE?"
    # We will attempt to drop the source schema.
    try:
        client.execute_sql(f'DROP SCHEMA IF EXISTS "{dataset_name}" CASCADE;')
        logger.info(f"Dropped staging schema '{dataset_name}'.")
    except Exception as e:
        logger.warning(f"Could not drop schema '{dataset_name}': {e}")

    logger.info("Medallion Schema Organization Complete.")
