# Architectural Deviations

## Schema Evolution Strategy

**Requirement:** The Business Requirements Document (BRD) mandated using `schema_contract={"columns": "warn"}` to strictly warn on new columns during ingestion.

**Deviation:** The project currently uses `schema_contract={"columns": "evolve"}`.

**Reason:** The installed version of `dlt` (1.20.0) does not support the `"warn"` value for `columns`. Attempting to use it results in a `DictValidationException`. The supported values are `['evolve', 'discard_value', 'freeze', 'discard_row']`.

**Resolution:** We have reverted to `"evolve"` to ensure pipeline stability. Future upgrades to `dlt` should be checked for `"warn"` support if strict schema monitoring becomes critical.
