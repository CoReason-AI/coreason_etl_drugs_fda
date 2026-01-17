# Contributing Guide

Thank you for your interest in contributing to **coreason_etl_drugs_fda**!

This project adheres to strict coding standards to ensure reliability and maintainability.

## Development Setup

### Prerequisites

*   Python 3.12+
*   Poetry
*   Docker (optional, for containerized testing)

### Installation

1.  **Clone the repo**:
    ```bash
    git clone https://github.com/CoReason-AI/coreason_etl_drugs_fda.git
    cd coreason_etl_drugs_fda
    ```

2.  **Install dependencies**:
    ```bash
    poetry install
    ```

3.  **Install pre-commit hooks**:
    ```bash
    poetry run pre-commit install
    ```

## Testing

We enforce **100% test coverage**.

*   **Run all tests**:
    ```bash
    poetry run pytest
    ```

*   **Run with coverage**:
    ```bash
    poetry run pytest --cov=src --cov-report=term-missing
    ```

Tests are located in the `tests/` directory. We use `pytest` fixtures and mocking extensively. External HTTP calls are mocked using `unittest.mock` or `respx`.

## Code Style

We use **Ruff** for linting and formatting, and **Mypy** for static type checking.

*   **Format code**:
    ```bash
    poetry run ruff format .
    ```

*   **Lint code**:
    ```bash
    poetry run ruff check --fix .
    ```

*   **Type check**:
    ```bash
    poetry run mypy .
    ```

## Development Protocol

1.  **Atomic Changes**: Break down your work into small, atomic units.
2.  **Test-Driven**: Write tests for every new feature or bug fix.
3.  **No Regressions**: Ensure all existing tests pass.
4.  **Logging**: Use `loguru` (imported from `coreason_etl_drugs_fda.utils.logger`) instead of `print` or standard `logging`.

## Documentation

*   Update documentation in `docs/` when modifying features.
*   Build docs locally to verify:
    ```bash
    poetry run mkdocs build
    poetry run mkdocs serve
    ```

## License

By contributing, you agree that your code will be licensed under the project's **Prosperity Public License 3.0**.
