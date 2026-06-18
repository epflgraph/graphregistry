# tests/end2end_tests/test_api_endpoints.py
"""End-to-end tests for the REST API against a real database.

These tests start a local uvicorn server and exercise the full HTTP stack. They
are marked with `e2e` and excluded from the default pytest run.
"""
from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import requests
import uvicorn
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.entrypoints.api.main import create_app
from tests.helpers.db_checks import (
    count_edge_rows,
    count_node_rows,
    db_field_map,
    edge_label,
    fetch_edge_custom_fields,
    fetch_node_basic_row,
    fetch_node_custom_fields,
    field_map,
    node_label,
)
from tests.helpers.fixtures import get_test_schema_name, load_subgraph_fixture

ENGINE_NAME = "xaas_coresrv"
SUBGRAPH_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "end2end_tests"
    / "test_subgraph_data.json"
)
SUBGRAPH_KEYS_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "end2end_tests"
    / "test_subgraph_keys.json"
)


@pytest.fixture(scope="module")
def schema_name() -> str:
    return get_test_schema_name()


@pytest.fixture(scope="module")
def db() -> GraphDB:
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def api_base_url() -> Iterator[str]:
    port = get_free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(),
            host="127.0.0.1",
            port=port,
            log_level="warning",
            lifespan="off",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                response = requests.get(f"{base_url}/health", timeout=0.5)
                if response.status_code == 200:
                    break
            except requests.RequestException:
                time.sleep(0.05)
        else:
            raise AssertionError("API server did not start within 10 seconds.")

        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def assert_response_ok(
    response: requests.Response,
    *,
    count_key: str,
    expected_count: int,
    saved_key: str | None = None,
) -> None:
    assert response.status_code == 200, response.text
    response_json = response.json()
    assert response_json["success"] is True
    assert response_json[count_key] == expected_count
    if saved_key is not None:
        assert len(response_json[saved_key]) == expected_count


def delete_edges(base_url: str, edge_list: list[dict[str, Any]]) -> None:
    response = requests.post(
        f"{base_url}/api/edges/delete_many",
        json={"key_list": edge_list},
        timeout=60,
    )
    assert response.status_code == 200, response.text


def delete_nodes(base_url: str, node_list: list[dict[str, Any]]) -> None:
    response = requests.post(
        f"{base_url}/api/nodes/delete_many",
        json={"key_list": node_list},
        timeout=60,
    )
    assert response.status_code == 200, response.text


@pytest.mark.e2e
def test_api_data_insert_subgraph_e2e(db: GraphDB, schema_name: str) -> None:
    sample_subgraph = load_subgraph_fixture(SUBGRAPH_FIXTURE_PATH)
    key_subgraph = load_subgraph_fixture(SUBGRAPH_KEYS_FIXTURE_PATH)
    sample_nodes = sample_subgraph["node_list"]
    sample_edges = sample_subgraph["edge_list"]
    key_nodes = key_subgraph["node_list"]
    key_edges = key_subgraph["edge_list"]

    with api_base_url() as base_url:
        delete_edges(base_url, key_edges)
        delete_nodes(base_url, key_nodes)

        try:
            node_response = requests.post(
                f"{base_url}/api/nodes/save_many",
                json={"node_list": sample_nodes},
                timeout=120,
            )
            assert_response_ok(
                node_response,
                count_key="count",
                expected_count=len(sample_nodes),
                saved_key="saved_keys",
            )

            edge_response = requests.post(
                f"{base_url}/api/edges/save_many",
                json={"edge_list": sample_edges},
                timeout=120,
            )
            assert_response_ok(
                edge_response,
                count_key="count",
                expected_count=len(sample_edges),
                saved_key="saved_keys",
            )

            for node_json in sample_nodes:
                label = node_label(node_json)

                assert count_node_rows(db, schema_name, ENGINE_NAME, node_json) == 1, (
                    f"Missing or duplicate node shell row for {label}"
                )

                basic_row = fetch_node_basic_row(db, schema_name, ENGINE_NAME, node_json)
                assert basic_row is not None, f"Missing basic node row for {label}"
                assert basic_row[0] == node_json.get("title", "")
                assert (basic_row[1] or "") == (node_json.get("text_source") or "")
                assert (basic_row[2] or "") == (node_json.get("raw_text") or "")

                expected_custom_fields = field_map(node_json.get("custom_fields") or [])
                actual_custom_fields = db_field_map(
                    fetch_node_custom_fields(db, schema_name, ENGINE_NAME, node_json)
                )
                assert actual_custom_fields == expected_custom_fields, (
                    f"Custom fields mismatch for node {label}\n"
                    f"Expected:\n{expected_custom_fields}\n"
                    f"Actual:\n{actual_custom_fields}"
                )

            for edge_json in sample_edges:
                label = edge_label(edge_json)
                assert count_edge_rows(db, schema_name, ENGINE_NAME, edge_json) == 1, (
                    f"Missing or duplicate edge shell row for {label}"
                )

                expected_custom_fields = field_map(edge_json.get("custom_fields") or [])
                actual_custom_fields = db_field_map(
                    fetch_edge_custom_fields(db, schema_name, ENGINE_NAME, edge_json)
                )
                assert actual_custom_fields == expected_custom_fields, (
                    f"Custom fields mismatch for edge {label}\n"
                    f"Expected:\n{expected_custom_fields}\n"
                    f"Actual:\n{actual_custom_fields}"
                )

        finally:
            delete_edges(base_url, key_edges)
            delete_nodes(base_url, key_nodes)

        for edge_json in key_edges:
            assert count_edge_rows(db, schema_name, ENGINE_NAME, edge_json) == 0

        for node_json in key_nodes:
            assert count_node_rows(db, schema_name, ENGINE_NAME, node_json) == 0
