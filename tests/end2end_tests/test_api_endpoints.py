from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator
from typing import cast

import pytest
import requests
import uvicorn
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.entrypoints.api.main import create_app


glbcfg = GlobalConfig()

FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_nodelist.json")
DELETE_FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_nodekeylist.json")
ENGINE_NAME = "xaas_coresrv"
SCHEMA_NAME = glbcfg.schema_registry


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




if not SCHEMA_NAME.startswith("_1_DEV_"):
    raise AssertionError(
        f"Test is configured to use schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
        "Please set execution mode to 'dev' in your config to ensure tests run against a safe schema."
    )


def load_sample_nodes() -> list[dict]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected node_list fixture to be a JSON list.")

    return data


def load_node_keys() -> list[dict]:
    with DELETE_FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected node key fixture to be a JSON list.")

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


def node_key_tuple(node_json: dict) -> tuple[str, str, str]:
    return (
        str(node_json["institution_id"]),
        str(node_json["object_type"]),
        str(node_json["object_id"]),
    )


def to_api_node(node_json: dict) -> dict:
    return {
        "object_type": node_json["object_type"],
        "object_id": node_json["object_id"],
        "object_title": node_json.get("object_title"),
        "text_source": node_json.get("text_source"),
        "raw_text": node_json.get("raw_text"),
        "custom_fields": node_json.get("custom_fields", []),
        "page_profile": node_json.get("page_profile") or {},
    }


def to_api_node_key(node_json: dict) -> dict:
    return {
        "object_type": node_json["object_type"],
        "object_id": node_json["object_id"],
    }


def delete_nodes(base_url: str) -> None:
    if not SCHEMA_NAME.startswith("_1_DEV_"):
        raise AssertionError(
            f"Attempting to delete nodes from schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
            "Aborting to prevent potential data loss. Please set execution mode to 'dev' in your config."
        )

    response = requests.post(
        f"{base_url}/api/nodes/delete_many",
        json={"key_list": [to_api_node_key(node) for node in load_node_keys()]},
        timeout=60,
    )

    print("\nDELETE STATUS:\n", response.status_code)
    print("\nDELETE BODY:\n", response.text)

    assert response.status_code == 200, response.text


def count_node_rows(db: GraphDB, node_json: dict) -> int:
    institution_id, object_type, object_id = node_key_tuple(node_json)
    query = f"""
    SELECT COUNT(*)
    FROM {SCHEMA_NAME}.Nodes_N_Object
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return int(rows[0][0])


def fetch_node_basic_row(db: GraphDB, node_json: dict) -> tuple | None:
    institution_id, object_type, object_id = node_key_tuple(node_json)
    query = f"""
    SELECT object_title, text_source, raw_text
    FROM {SCHEMA_NAME}.Nodes_N_Object
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return cast(tuple | None, rows[0] if rows else None)


def fetch_custom_fields(db: GraphDB, node_json: dict) -> list[tuple[str, str, str]]:
    institution_id, object_type, object_id = node_key_tuple(node_json)
    query = f"""
    SELECT field_language, field_name, field_value
    FROM {SCHEMA_NAME}.Data_N_Object_T_CustomFields
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    return cast(list[tuple[str, str, str]], db.execute_query(engine_name=ENGINE_NAME, query=query))


def fetch_page_profile_row(db: GraphDB, node_json: dict) -> tuple | None:
    institution_id, object_type, object_id = node_key_tuple(node_json)
    query = f"""
    SELECT *
    FROM {SCHEMA_NAME}.Data_N_Object_T_PageProfile
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return cast(tuple | None, rows[0] if rows else None)


@pytest.mark.e2e
def test_api_data_insert_node_list_e2e() -> None:
    sample_nodes = load_sample_nodes()
    db = make_db()

    with api_base_url() as base_url:
        delete_nodes(base_url)

        try:
            response = requests.post(
                f"{base_url}/api/nodes/save_many",
                json={"node_list": [to_api_node(node) for node in sample_nodes]},
                timeout=120,
            )

            print("\nSTATUS:\n", response.status_code)
            print("\nBODY:\n", response.text)

            assert response.status_code == 200, response.text

            response_json = response.json()
            assert response_json["success"] is True
            assert response_json["count"] == len(sample_nodes)
            assert len(response_json["saved_keys"]) == len(sample_nodes)

            from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import (
                PAGE_PROFILE_COLUMNS,
            )

            for node_json in sample_nodes:
                node_label = (
                    f"{node_json['institution_id']}:"
                    f"{node_json['object_type']}:"
                    f"{node_json['object_id']}"
                )

                assert count_node_rows(db, node_json) == 1, (
                    f"Missing or duplicate node shell row for {node_label}"
                )

                basic_row = fetch_node_basic_row(db, node_json)
                assert basic_row is not None, f"Missing basic node row for {node_label}"

                expected_title = node_json.get("object_title", "")
                expected_text_source = node_json.get("text_source") or ""
                expected_raw_text = node_json.get("raw_text") or ""

                assert basic_row[0] == expected_title
                assert (basic_row[1] or "") == expected_text_source
                assert (basic_row[2] or "") == expected_raw_text

                expected_custom_fields = node_json.get("custom_fields", [])
                db_custom_fields = fetch_custom_fields(db, node_json)

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

                    rprint(f"\n[bold cyan]CHECKING CUSTOM FIELDS FOR:[/bold cyan] {node_label}")
                    rprint("[bold]Expected:[/bold]", expected_field_map)
                    rprint("[bold]Actual:[/bold]", actual_field_map)

                assert actual_field_map == expected_field_map, (
                    f"Custom fields mismatch for node {node_label}\n"
                    f"Expected:\n{expected_field_map}\n"
                    f"Actual:\n{actual_field_map}"
                )

                expected_page_profile = node_json.get("page_profile") or {}
                db_page_profile = fetch_page_profile_row(db, node_json)

                if expected_page_profile:
                    assert db_page_profile is not None, (
                        f"Missing page profile row for node {node_label}"
                    )

                    assert len(db_page_profile) >= 3 + len(PAGE_PROFILE_COLUMNS), (
                        f"Unexpected page profile row length for node {node_label}: "
                        f"got {len(db_page_profile)}, expected at least {3 + len(PAGE_PROFILE_COLUMNS)}"
                    )

                    assert db_page_profile[0] == node_json["institution_id"]
                    assert db_page_profile[1] == node_json["object_type"]
                    assert db_page_profile[2] == node_json["object_id"]

                    actual_page_profile_map = dict(
                        zip(
                            PAGE_PROFILE_COLUMNS,
                            db_page_profile[3:3 + len(PAGE_PROFILE_COLUMNS)],
                        )
                    )

                    if True:
                        from rich import print as rprint

                        rprint(f"\n[bold magenta]CHECKING PAGE PROFILE FOR:[/bold magenta] {node_label}")
                        rprint("[bold]Expected:[/bold]", expected_page_profile)
                        rprint("[bold]Actual:[/bold]", actual_page_profile_map)

                    for field_name, expected_value in expected_page_profile.items():
                        assert field_name in actual_page_profile_map, (
                            f"Missing page profile column '{field_name}' for node {node_label}"
                        )

                        actual_value = actual_page_profile_map[field_name]

                        if isinstance(expected_value, bool):
                            assert int(bool(actual_value)) == int(expected_value), (
                                f"Mismatch in page_profile[{field_name}] for node {node_label}: "
                                f"expected {int(expected_value)!r}, got {actual_value!r}"
                            )
                        else:
                            assert actual_value == expected_value, (
                                f"Mismatch in page_profile[{field_name}] for node {node_label}: "
                                f"expected {expected_value!r}, got {actual_value!r}"
                            )
                else:
                    assert db_page_profile is None, (
                        f"Unexpected page profile row for node {node_label}"
                    )

        finally:
            delete_nodes(base_url)

        for node_json in sample_nodes:
            assert count_node_rows(db, node_json) == 0






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
