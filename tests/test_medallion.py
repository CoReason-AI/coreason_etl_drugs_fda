# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from unittest.mock import MagicMock

from coreason_etl_drugs_fda.utils.medallion import organize_schemas


def test_organize_schemas_postgres() -> None:
    """
    Test that organize_schemas correctly issues ALTER TABLE statements
    for a Postgres destination.
    """
    # Mock Pipeline
    mock_pipeline = MagicMock()
    mock_pipeline.destination.destination_name = "postgres"
    mock_pipeline.dataset_name = "fda_data"

    # Mock Tables in Default Schema (using dlt normalized names based on our check)
    # Using the 'fd_aa_...' structure found in verification
    mock_pipeline.default_schema.tables.keys.return_value = [
        "fd_aa_drugs_bronze_fda_products",
        "fd_aa_drugs_silver_products",
        "fd_aa_drugs_gold_drug_product",
        "other_table",
        "_dlt_loads",
    ]

    # Mock SQL Client
    mock_client = MagicMock()
    mock_pipeline.sql_client.return_value = mock_client

    # Execute
    organize_schemas(mock_pipeline)

    # Verify Schema Creation
    mock_client.execute_sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS bronze;")
    mock_client.execute_sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS silver;")
    mock_client.execute_sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS gold;")

    # Verify Table Moves
    # Bronze
    expected_bronze = 'ALTER TABLE "fda_data"."fd_aa_drugs_bronze_fda_products" SET SCHEMA "bronze";'
    mock_client.execute_sql.assert_any_call(expected_bronze)

    # Silver
    expected_silver = 'ALTER TABLE "fda_data"."fd_aa_drugs_silver_products" SET SCHEMA "silver";'
    mock_client.execute_sql.assert_any_call(expected_silver)

    # Gold
    expected_gold = 'ALTER TABLE "fda_data"."fd_aa_drugs_gold_drug_product" SET SCHEMA "gold";'
    mock_client.execute_sql.assert_any_call(expected_gold)

    # Ensure unrelated tables are not moved
    # calls is a list of call objects.
    # We check that 'other_table' was NOT in any call args.
    for call_args in mock_client.execute_sql.call_args_list:
        sql = call_args[0][0]
        assert "other_table" not in sql


def test_organize_schemas_skip_non_postgres() -> None:
    """Test that function returns early for non-postgres destinations."""
    mock_pipeline = MagicMock()
    mock_pipeline.destination.destination_name = "duckdb"

    # Should not access sql_client
    organize_schemas(mock_pipeline)
    mock_pipeline.sql_client.assert_not_called()


def test_organize_schemas_exception_handling() -> None:
    """Test that exceptions during table moves are caught and logged."""
    mock_pipeline = MagicMock()
    mock_pipeline.destination.destination_name = "postgres"
    mock_pipeline.default_schema.tables.keys.return_value = ["fd_aa_drugs_bronze_table"]

    mock_client = MagicMock()
    mock_pipeline.sql_client.return_value = mock_client

    # Raise exception on execute_sql
    mock_client.execute_sql.side_effect = [None, None, None, Exception("DB Error")]
    # First 3 calls are CREATE SCHEMA, 4th call is ALTER TABLE

    organize_schemas(mock_pipeline)

    # Should complete without raising exception
    assert mock_client.execute_sql.call_count == 4
