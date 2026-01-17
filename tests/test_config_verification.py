from typing import Any

import dlt


def test_dlt_config_loading() -> None:
    """
    Verifies that dlt correctly loads the runtime configuration from .dlt/config.toml.
    """
    # Verify that the config provider sees the values defined in .dlt/config.toml
    config: Any = dlt.config

    # Check request_timeout
    assert config["runtime.request_timeout"] == 300

    # Check max_retries
    assert config["runtime.max_retries"] == 5


if __name__ == "__main__":
    test_dlt_config_loading()
