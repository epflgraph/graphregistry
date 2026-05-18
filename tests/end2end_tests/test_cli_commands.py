from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.entrypoints.mappers import SpecMapper


glbcfg = GlobalConfig()

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

ENGINE_NAME = "xaas_coresrv"
SCHEMA_NAME = glbcfg.schema_registry

if not SCHEMA_NAME.startswith("_1_DEV_"):
    raise AssertionError(
        f"Test is configured to use schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
        "Please set execution mode to 'dev' in your config to ensure tests run against a safe schema."
    )


def load_fixture(path: Path) -> dict[str, list[dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    subgraph = data.get("subgraph") if isinstance(data, dict) else None
    if not isinstance(subgraph, dict):
        raise AssertionError(f"Expected {path} to contain a JSON object at 'subgraph'.")

    node_list = subgraph.get("node_list")
    edge_list = subgraph.get("edge_list")
    if not isinstance(node_list, list) or not isinstance(edge_list, list):
        raise AssertionError(
            f"Expected {path} subgraph to contain node_list and edge_list arrays."
        )

    return {"node_list": node_list, "edge_list": edge_list}


def load_sample_subgraph() -> dict[str, list[dict[str, Any]]]:
    return load_fixture(SUBGRAPH_FIXTURE_PATH)


def load_key_subgraph() -> dict[str, list[dict[str, Any]]]:
    return load_fixture(SUBGRAPH_KEYS_FIXTURE_PATH)


def make_db() -> GraphDB:
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def cli_base_cmd() -> list[str]:
    cli_executable = Path(".venv.registry/bin/graphregistry").resolve()
    cli_name = str(cli_executable) if cli_executable.exists() else "graphregistry"
    return [cli_name, "data"]


def run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        text=True,
        capture_output=False,
        check=False,
    )
    assert result.returncode == 0, (
        f"CLI command failed: {' '.join(args)}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    return result


def expected_node_key(node_json: dict[str, Any]) -> tuple[str, str, str]:
    node = SpecMapper.from_node_spec(node_json)
    return (
        node.key.institution_id,
        node.key.object_type,
        node.key.object_id,
    )


def expected_edge_key(
    edge_json: dict[str, Any],
) -> tuple[str, str, str, str, str, str, str]:
    edge = SpecMapper.from_edge_spec(edge_json)
    return (
        edge.key.from_institution_id,
        edge.key.from_object_type,
        edge.key.from_object_id,
        edge.key.to_institution_id,
        edge.key.to_object_type,
        edge.key.to_object_id,
        edge.key.context,
    )


def node_label(node_json: dict[str, Any]) -> str:
    institution_id, object_type, object_id = expected_node_key(node_json)
    return f"{institution_id}:{object_type}:{object_id}"


def edge_label(edge_json: dict[str, Any]) -> str:
    (
        from_institution_id,
        from_object_type,
        from_object_id,
        to_institution_id,
        to_object_type,
        to_object_id,
        context,
    ) = expected_edge_key(edge_json)
    return (
        f"{from_institution_id}:{from_object_type}:{from_object_id} -> "
        f"{to_institution_id}:{to_object_type}:{to_object_id} ({context})"
    )


def count_node_rows(db: GraphDB, node_json: dict[str, Any]) -> int:
    institution_id, object_type, object_id = expected_node_key(node_json)
    query = f"""
    SELECT COUNT(*)
    FROM {SCHEMA_NAME}.Nodes_N_Object
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return int(rows[0][0])


def fetch_node_basic_row(db: GraphDB, node_json: dict[str, Any]) -> tuple | None:
    institution_id, object_type, object_id = expected_node_key(node_json)
    query = f"""
    SELECT object_title, text_source, raw_text
    FROM {SCHEMA_NAME}.Nodes_N_Object
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return cast(tuple | None, rows[0] if rows else None)


def fetch_node_custom_fields(
    db: GraphDB, node_json: dict[str, Any]
) -> list[tuple[str, str, str]]:
    institution_id, object_type, object_id = expected_node_key(node_json)
    query = f"""
    SELECT field_language, field_name, field_value
    FROM {SCHEMA_NAME}.Data_N_Object_T_CustomFields
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    return cast(
        list[tuple[str, str, str]],
        db.execute_query(engine_name=ENGINE_NAME, query=query),
    )


def count_edge_rows(db: GraphDB, edge_json: dict[str, Any]) -> int:
    (
        from_institution_id,
        from_object_type,
        from_object_id,
        to_institution_id,
        to_object_type,
        to_object_id,
        context,
    ) = expected_edge_key(edge_json)
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


def fetch_edge_custom_fields(
    db: GraphDB, edge_json: dict[str, Any]
) -> list[tuple[str, str, str]]:
    (
        from_institution_id,
        from_object_type,
        from_object_id,
        to_institution_id,
        to_object_type,
        to_object_id,
        context,
    ) = expected_edge_key(edge_json)
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
    return cast(
        list[tuple[str, str, str]],
        db.execute_query(engine_name=ENGINE_NAME, query=query),
    )


def field_map(fields: list[dict[str, Any]]) -> dict[tuple[str, str], str]:
    return {
        (field["field_language"], field["field_name"]): field["field_value"]
        for field in fields
    }


def db_field_map(rows: list[tuple[str, str, str]]) -> dict[tuple[str, str], str]:
    return {
        (field_language, field_name): field_value
        for field_language, field_name, field_value in rows
    }


def delete_many(
    node_keys_path: Path,
    edge_keys_path: Path,
) -> None:
    run_cli(
        cli_base_cmd()
        + [
            "delete",
            f"--edge_key_list=@{edge_keys_path}",
            "--actions=commit",
        ]
    )
    run_cli(
        cli_base_cmd()
        + [
            "delete",
            f"--node_key_list=@{node_keys_path}",
            "--actions=commit",
        ]
    )


@pytest.mark.e2e
def test_cli_data_commands_subgraph_e2e(tmp_path: Path) -> None:
    sample_subgraph = load_sample_subgraph()
    key_subgraph = load_key_subgraph()
    sample_nodes = sample_subgraph["node_list"]
    sample_edges = sample_subgraph["edge_list"]
    key_nodes = key_subgraph["node_list"]
    key_edges = key_subgraph["edge_list"]
    db = make_db()

    node_list_path = tmp_path / "node_list.json"
    edge_list_path = tmp_path / "edge_list.json"
    node_key_list_path = tmp_path / "node_key_list.json"
    edge_key_list_path = tmp_path / "edge_key_list.json"

    write_json(node_list_path, {"node_list": sample_nodes})
    write_json(edge_list_path, {"edge_list": sample_edges})
    write_json(node_key_list_path, {"key_list": key_nodes})
    write_json(edge_key_list_path, {"key_list": key_edges})

    delete_many(node_key_list_path, edge_key_list_path)

    try:
        run_cli(
            cli_base_cmd()
            + [
                "save",
                f"--node_list=@{node_list_path}",
                "--actions=commit",
            ]
        )

        run_cli(
            cli_base_cmd()
            + [
                "save",
                f"--edge_list=@{edge_list_path}",
                "--actions=commit",
            ]
        )

        for node_json in sample_nodes:
            label = node_label(node_json)
            expected_node = SpecMapper.from_node_spec(node_json)

            assert count_node_rows(db, node_json) == 1, (
                f"Missing or duplicate node shell row for {label}"
            )

            basic_row = fetch_node_basic_row(db, node_json)
            assert basic_row is not None, f"Missing basic node row for {label}"
            assert basic_row[0] == expected_node.title
            assert (basic_row[1] or "") == (expected_node.text_source or "")
            assert (basic_row[2] or "") == (expected_node.raw_text or "")

            expected_custom_fields = field_map(node_json.get("custom_fields") or [])
            actual_custom_fields = db_field_map(fetch_node_custom_fields(db, node_json))
            assert actual_custom_fields == expected_custom_fields, (
                f"Custom fields mismatch for node {label}\n"
                f"Expected:\n{expected_custom_fields}\n"
                f"Actual:\n{actual_custom_fields}"
            )

        for edge_json in sample_edges:
            label = edge_label(edge_json)
            assert count_edge_rows(db, edge_json) == 1, (
                f"Missing or duplicate edge shell row for {label}"
            )

            expected_custom_fields = field_map(edge_json.get("custom_fields") or [])
            actual_custom_fields = db_field_map(fetch_edge_custom_fields(db, edge_json))
            assert actual_custom_fields == expected_custom_fields, (
                f"Custom fields mismatch for edge {label}\n"
                f"Expected:\n{expected_custom_fields}\n"
                f"Actual:\n{actual_custom_fields}"
            )

    finally:
        delete_many(node_key_list_path, edge_key_list_path)

    for edge_json in key_edges:
        assert count_edge_rows(db, edge_json) == 0

    for node_json in key_nodes:
        assert count_node_rows(db, node_json) == 0
