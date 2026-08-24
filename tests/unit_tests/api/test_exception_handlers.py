# tests/unit_tests/api/test_exception_handlers.py
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


@pytest.fixture
def api_client() -> TestClient:
    """Build a TestClient with a fake node operations that raises on demand."""

    def _make_node_ops(exc: Exception | None = None):
        class FakeNodeOps:
            def save_many(self, *args, **kwargs):
                if exc is not None:
                    raise exc
                return args[0]

        def _get_node_ops():
            return FakeNodeOps()

        return _get_node_ops

    app = create_app()

    def _client_for(exc: Exception | None) -> TestClient:
        app.dependency_overrides[get_node_ops] = _make_node_ops(exc)
        return TestClient(app)

    yield _client_for
    app.dependency_overrides.clear()


def test_connection_exhausted_returns_503(api_client) -> None:
    client = api_client(ConnectionExhaustedError("too many connections"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 503
    assert response.headers.get("retry-after") is not None
    assert "overloaded" in response.json()["detail"].lower()


def test_lock_wait_timeout_returns_503(api_client) -> None:
    client = api_client(LockWaitTimeoutError("lock wait timeout"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 503
    assert response.headers.get("retry-after") is not None


def test_duplicate_key_returns_409(api_client) -> None:
    client = api_client(DuplicateKeyError("duplicate entry"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 409
    assert "duplicate" in response.json()["detail"].lower()


def test_generic_persistence_error_returns_500(api_client) -> None:
    client = api_client(PersistenceError("some db error"))
    response = client.post("/api/nodes/save_many", json={"node_list": []})
    assert response.status_code == 500
    assert "persistence" in response.json()["detail"].lower()
