# Welcome to coreason_etl_drugs_fda

**coreason_etl_drugs_fda** is a fast, extensible Python ETL pipeline designed to maintain a local, high-fidelity mirror of the FDA Drugs database.

It utilizes modern data engineering tools to download, clean, and structure the data into a Medallion Architecture (Bronze, Silver, Gold).

## Key Features

*   **Automated Download**: Fetches the latest data directly from the FDA website, bypassing basic bot protection.
*   **Medallion Architecture**:
    *   **Bronze**: Raw data exactly as received from FDA (ZIP/TXT).
    *   **Silver**: Cleaned data with normalized types, standard casing, and generated UUIDs.
    *   **Gold**: Denormalized, analytics-ready "One Big Table" for Products.
*   **Robust Technology Stack**: Built with `dlt` for loading, `Polars` for fast transformation, and `Pydantic` for schema validation.
*   **Production Ready**: Includes comprehensive logging, error handling, and Docker support.

## Quick Links

*   [User Guide](user_guide.md): Learn how to install and run the pipeline.
*   [Architecture](architecture.md): Understand the design and data flow.
*   [API Reference](api.md): Detailed module documentation.
*   [Contributing](contributing.md): Guidelines for developers.

## License

This software is proprietary and dual-licensed under the **Prosperity Public License 3.0**.
See the [LICENSE](https://github.com/CoReason-AI/coreason_etl_drugs_fda/blob/main/LICENSE) file for details.
