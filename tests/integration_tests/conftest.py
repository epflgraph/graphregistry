# graphregistry/tests/integration_tests/conftest.py
"""Safety gate for the integration test suite.

The integration tests read live database data. The whole session refuses to
run unless the registry configuration declares the dev execution mode, so a
checkout pointed at production schemas can never execute them by accident.

The mode is declared in config/environment/config_registry.yml under
database.mode; only in dev mode do the schema names carry the _1_DEV_ prefix
that separates test data from production data.
"""
from __future__ import annotations
import pytest

# Public Function: Build the refusal message for a non-dev execution mode,
# or None when the mode allows the suite to run.
def _refusal_reason(execution_mode: str) -> str | None:
    if execution_mode == "dev":
        return None
    return (
        f"Refusing to run integration tests: registry execution mode is '{execution_mode}', expected 'dev'. "
        "These tests read live database data. Point config/environment/config_registry.yml "
        "at a dev configuration (database.mode: dev) before running them."
    )

# Public Function: Refuse the integration session unless the registry runs in
# dev mode; fires before any test fixture can open a database connection.
@pytest.fixture(scope="session", autouse=True)
def _require_dev_mode() -> None:

    # Load the registry configuration lazily so collection stays fast.
    from graphregistry.common.config import GlobalConfig

    # Resolve the configured execution mode and refuse when it is not dev.
    execution_mode = GlobalConfig().mysql_execution_mode
    reason = _refusal_reason(execution_mode)
    if reason is not None:
        pytest.exit(reason, returncode=1)
