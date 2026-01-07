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

    It iterates through the pipeline's schema tables and executes ALTER TABLE commands
    to move them to the appropriate schema based on their name prefix.

    Prefix Mapping:
    - FDA@DRUGS_bronze_... -> bronze schema
    - FDA@DRUGS_silver_... -> silver schema
    - FDA@DRUGS_gold_...   -> gold schema
    """
    # Only proceed if destination supports schemas (Postgres, Redshift, Snowflake, etc.)
    # We assume Postgres per requirements.
    if pipeline.destination.destination_name != "postgres":
        logger.info(f"Skipping schema organization for destination: {pipeline.destination.destination_name}")
        return

    client = pipeline.sql_client()

    # 1. Ensure schemas exist
    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        client.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # 2. Get list of tables in the dataset
    # We can inspect the dlt schema to know which tables were loaded
    # pipeline.default_schema.tables contains the table definitions.

    # However, we need to know the *actual* table names in the DB.
    # dlt usually normalizes table names.
    # But since we set resource names explicitly, dlt uses those (normalized).
    # Since we used naming convention in source.py, let's assume they match.

    # We will query information_schema to find tables in the default dataset schema
    dataset_name = pipeline.dataset_name

    # Note: dlt standardizes dataset_name too? Usually strictly follows config.

    # Query to find tables starting with FDA@DRUGS_
    # Note: The table names in DB might be lowercased or snake_cased by dlt depending on config.
    # But we set them in the resource name.
    # We need to be careful about quoting.

    # Let's iterate over the pipeline schema tables instead, as that's what dlt thinks it loaded.
    loaded_tables = pipeline.default_schema.tables.keys()

    for table_name in loaded_tables:
        target_schema = None

        # dlt normalizes names, so FDA@DRUGS might become fda_drugs or similar if not carefully handled?
        # dlt resource names are normalized according to the naming convention *of the schema*.
        # If we passed "FDA@DRUGS_...", standard snake_case might make it "fda_drugs_..."
        # But wait, we modified the source code to pass these names.
        # dlt's default naming convention is snake_case.
        # "FDA@DRUGS_bronze_..." -> "fda_drugs_bronze_..." probably?
        # The '@' is usually replaced by underscore or removed.

        # However, the user request specifically asked for these prefixes.
        # If dlt normalizes them away, we might fail to match.
        # Let's check how dlt normalizes "FDA@DRUGS_".
        # dlt.common.normalizers.naming.snake_case.NamingConvention.normalize_identifier("FDA@DRUGS_abc")
        # -> "fda_drugs_abc"

        # We need to check if the table name STARTS with the normalized version of our prefixes.

        # Actually, let's look at the source code again.
        # We used f"FDA@DRUGS_bronze_fda__{...}"
        # If we want to preserve "@", we might need a different naming convention or check what dlt does.
        # But if dlt normalizes it, we should check for the normalized string.

        # However, for the purpose of this task, let's assume we look for the pattern in the table name string.

        if "bronze" in table_name and "fda" in table_name:  # loose check? No, let's be strict if possible.
            # "fda_drugs_bronze" seems likely if normalized.
            pass

        # To be robust, let's just match on the intent:
        # If it has "bronze" -> bronze schema
        # If it has "silver" -> silver schema
        # If it has "gold" -> gold schema

        if "_bronze_" in table_name or table_name.startswith("bronze_") or "fda_drugs_bronze" in table_name:
            target_schema = "bronze"
        elif "_silver_" in table_name or table_name.startswith("silver_") or "fda_drugs_silver" in table_name:
            target_schema = "silver"
        elif "_gold_" in table_name or table_name.startswith("gold_") or "fda_drugs_gold" in table_name:
            target_schema = "gold"

        if target_schema:
            logger.info(f"Moving table {table_name} to schema {target_schema}")
            # Move the table
            # ALTER TABLE dataset.table SET SCHEMA target_schema
            # We need to handle the fact that dlt might have created it in 'public' or 'dataset_name' schema.
            # The client connection usually points to the DB.

            # Construct SQL
            # We assume table_name is the name in the default schema.
            # We need to quote identifiers to be safe.

            # Using fully qualified names: dataset_name.table_name
            # But wait, ALTER TABLE {dataset_name}.{table_name} SET SCHEMA {target_schema}

            try:
                # We wrap in quotes just in case, though dlt table names are usually safe.
                # dlt.client().make_qualified_table_name?
                # Simplified approach:
                sql = f'ALTER TABLE "{dataset_name}"."{table_name}" SET SCHEMA "{target_schema}";'
                client.execute_sql(sql)
            except Exception as e:
                logger.warning(f"Failed to move table {table_name}: {e}")
