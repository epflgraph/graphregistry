# graphregistry/tests/unit_tests/api/test_exception_handlers.py
"""Unit tests for API exception handlers."""
from __future__ import annotations
import pytest
from fastapi.testclient import TestClient
from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    DuplicateKeyError,
    LockWaitTimeoutError,
    PersistenceError,
)
from graphregistry.entrypoints.api.main import create_app
from graphregistry.entrypoints.api.router import get_node_ops

#================================================================#
# Function Group: Pytest fixtures                                #
#================================================================#

# Public Method: Build a TestClient with a fake node operations that raises on demand.
@pytest.fixture
def api_client() -> TestClient:

    # Internal Function: Build a dependency override factory that injects a fake node
    # operations instance.
    # Internal Function: make node ops
    def _make_node_ops(exc: Exception | None = None):

#==================#
# Class Definition #
#==================#
        class FakeNodeOps:
            # Public Method: save many
            def save_many(self, *args, **kwargs):
                if exc is not None:
                    raise exc
                return args[0]

        # Internal Function: get node ops
        def _get_node_ops():
            return FakeNodeOps()

        # Return the computed result.
        return _get_node_ops

    # Prepare app for the following steps.
    app = create_app()

    # Internal Function: Build a TestClient configured with the fake node operations.
    def _client_for(exc: Exception | None) -> TestClient:
        app.dependency_overrides[get_node_ops] = _make_node_ops(exc)
        return TestClient(app)

    # Continue with the next step.
    yield _client_for
    app.dependency_overrides.clear()

#================================================================#
# Test Group: Persistence exception HTTP responses               #
#================================================================#

# Test: Connection exhaustion is surfaced as 503 with a Retry-After header.
# Public Method: test connection exhausted returns 503
def test_connection_exhausted_returns_503(api_client) -> None:
    client = api_client(ConnectionExhaustedError("too many connections"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 503
    assert response.headers.get("retry-after") is not None
    assert "overloaded" in response.json()["detail"].lower()

# Test: Lock wait timeout is surfaced as 503 with a Retry-After header.
# Public Method: test lock wait timeout returns 503
def test_lock_wait_timeout_returns_503(api_client) -> None:
    client = api_client(LockWaitTimeoutError("lock wait timeout"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 503
    assert response.headers.get("retry-after") is not None

# Test: Duplicate key violation is surfaced as 409.
# Public Method: test duplicate key returns 409
def test_duplicate_key_returns_409(api_client) -> None:
    client = api_client(DuplicateKeyError("duplicate entry"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 409
    assert "duplicate" in response.json()["detail"].lower()

# Test: Generic persistence error is surfaced as 500.
# Public Method: test generic persistence error returns 500
def test_generic_persistence_error_returns_500(api_client) -> None:
    client = api_client(PersistenceError("some db error"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 500
    assert "persistence" in response.json()["detail"].lower()
