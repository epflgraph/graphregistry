from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import pytest
import requests
import uvicorn
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.entrypoints.api.main import create_app


glbcfg = GlobalConfig()

FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_edgelist.json")
DELETE_FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_edgekeylist.json")
ENGINE_NAME = "xaas_coresrv"
SCHEMA_NAME = glbcfg.schema_registry

if not SCHEMA_NAME.startswith("_1_DEV_"):
    raise AssertionError(
        f"Test is configured to use schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
        "Please set execution mode to 'dev' in your config to ensure tests run against a safe schema."
    )


def load_sample_edges() -> list[dict]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected edge_list fixture to be a JSON list.")

    return data


def load_edge_keys() -> list[dict]:
    with DELETE_FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected edge key fixture to be a JSON list.")

    return data


def make_db() -> GraphDB:
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


def edge_key_tuple(edge_json: dict) -> tuple[str, str, str, str, str, str, str]:
    return (
        str(edge_json["from_institution_id"]),
        str(edge_json["from_object_type"]),
        str(edge_json["from_object_id"]),
        str(edge_json["to_institution_id"]),
        str(edge_json["to_object_type"]),
        str(edge_json["to_object_id"]),
        str(edge_json["context"]),
    )


def to_api_edge(edge_json: dict) -> dict:
    return {
        "from_object_type": edge_json["from_object_type"],
        "from_object_id": edge_json["from_object_id"],
        "to_object_type": edge_json["to_object_type"],
        "to_object_id": edge_json["to_object_id"],
        "context": edge_json["context"],
        "custom_fields": edge_json.get("custom_fields", []),
    }


def to_api_edge_key(edge_json: dict) -> dict:
    return {
        "from_object_type": edge_json["from_object_type"],
        "from_object_id": edge_json["from_object_id"],
        "to_object_type": edge_json["to_object_type"],
        "to_object_id": edge_json["to_object_id"],
        "context": edge_json["context"],
    }


def delete_edges(base_url: str) -> None:
    if not SCHEMA_NAME.startswith("_1_DEV_"):
        raise AssertionError(
            f"Attempting to delete edges from schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
            "Aborting to prevent potential data loss. Please set execution mode to 'dev' in your config."
        )

    response = requests.post(
        f"{base_url}/api/edges/delete_many",
        json={"key_list": [to_api_edge_key(edge) for edge in load_edge_keys()]},
        timeout=60,
    )

    print("\nDELETE STATUS:\n", response.status_code)
    print("\nDELETE BODY:\n", response.text)

    assert response.status_code == 200, response.text


def count_edge_rows(db: GraphDB, edge_json: dict) -> int:
    (
        from_institution_id,
        from_object_type,
        from_object_id,
        to_institution_id,
        to_object_type,
        to_object_id,
        context,
    ) = edge_key_tuple(edge_json)
    query = f"""
    SELECT COUNT(*)
    FROM {SCHEMA_NAME}.Edges_N_Object_N_Object_T_ChildToParent
    WHERE from_institution_id = "{from_institution_id}"
      AND from_object_type    = "{from_object_type}"
      AND from_object_id      = "{from_object_id}"
      AND to_institution_id   = "{to_institution_id}"
      AND to_object_type      = "{to_object_type}"
      AND to_object_id        = "{to_object_id}"
      AND context             = "{context}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return int(rows[0][0])


def fetch_custom_fields(db: GraphDB, edge_json: dict) -> list[tuple[str, str, str]]:
    (
        from_institution_id,
        from_object_type,
        from_object_id,
        to_institution_id,
        to_object_type,
        to_object_id,
        context,
    ) = edge_key_tuple(edge_json)
    query = f"""
    SELECT field_language, field_name, field_value
    FROM {SCHEMA_NAME}.Data_N_Object_N_Object_T_CustomFields
    WHERE from_institution_id = "{from_institution_id}"
      AND from_object_type    = "{from_object_type}"
      AND from_object_id      = "{from_object_id}"
      AND to_institution_id   = "{to_institution_id}"
      AND to_object_type      = "{to_object_type}"
      AND to_object_id        = "{to_object_id}"
      AND context             = "{context}";
    """
    return cast(list[tuple[str, str, str]], db.execute_query(engine_name=ENGINE_NAME, query=query))


@pytest.mark.e2e
def test_api_data_insert_edge_list_e2e() -> None:
    sample_edges = load_sample_edges()
    db = make_db()

    with api_base_url() as base_url:
        delete_edges(base_url)

        try:
            response = requests.post(
                f"{base_url}/api/edges/save_many",
                json={"edge_list": [to_api_edge(edge) for edge in sample_edges]},
                timeout=120,
            )

            print("\nSTATUS:\n", response.status_code)
            print("\nBODY:\n", response.text)

            assert response.status_code == 200, response.text

            response_json = response.json()
            assert response_json["success"] is True
            assert response_json["count"] == len(sample_edges)
            assert len(response_json["saved_keys"]) == len(sample_edges)

            for edge_json in sample_edges:
                edge_label = (
                    f"{edge_json['from_institution_id']}:{edge_json['from_object_type']}:{edge_json['from_object_id']} -> "
                    f"{edge_json['to_institution_id']}:{edge_json['to_object_type']}:{edge_json['to_object_id']} "
                    f"({edge_json['context']})"
                )

                assert count_edge_rows(db, edge_json) == 1, (
                    f"Missing or duplicate edge shell row for {edge_label}"
                )

                expected_custom_fields = edge_json.get("custom_fields", [])
                db_custom_fields = fetch_custom_fields(db, edge_json)

                expected_field_map = {
                    (field["field_language"], field["field_name"]): field["field_value"]
                    for field in expected_custom_fields
                }

                actual_field_map = {
                    (field_language, field_name): field_value
                    for field_language, field_name, field_value in db_custom_fields
                }

                if True:
                    from rich import print as rprint

                    rprint(f"\n[bold cyan]CHECKING CUSTOM FIELDS FOR:[/bold cyan] {edge_label}")
                    rprint("[bold]Expected:[/bold]", expected_field_map)
                    rprint("[bold]Actual:[/bold]", actual_field_map)

                assert actual_field_map == expected_field_map, (
                    f"Custom fields mismatch for edge {edge_label}\n"
                    f"Expected:\n{expected_field_map}\n"
                    f"Actual:\n{actual_field_map}"
                )

        finally:
            delete_edges(base_url)

        for edge_json in sample_edges:
            assert count_edge_rows(db, edge_json) == 0
