# tests/end2end_tests/test_cli_commands.py
"""End-to-end tests for the CLI against a real database.

These tests spawn the actual `graphregistry` CLI binary and verify DB state.
They are marked with `e2e` and excluded from the default pytest run.
"""
from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.entrypoints.mappers import SpecMapper
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
from tests.helpers.fixtures import load_subgraph_fixture, temp_test_global_config

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
def test_config() -> Iterator[tuple[Path, str]]:
    with temp_test_global_config() as (config_path, glbcfg):
        yield config_path, glbcfg.schema_registry


@pytest.fixture(scope="module")
def schema_name(test_config: tuple[Path, str]) -> str:
    return test_config[1]


@pytest.fixture(scope="module")
def db() -> GraphDB:
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def cli_base_cmd() -> list[str]:
    cli_executable = Path(".venv.registry/bin/graphregistry").resolve()
    cli_name = str(cli_executable) if cli_executable.exists() else "graphregistry"
    return [cli_name, "data"]


def run_cli(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        text=True,
        capture_output=False,
        check=False,
        env=env,
    )
    assert result.returncode == 0, (
        f"CLI command failed: {' '.join(args)}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    return result


def delete_many(node_keys_path: Path, edge_keys_path: Path, env: dict[str, str]) -> None:
    run_cli(
        cli_base_cmd()
        + [
            "delete",
            f"--edge_key_list=@{edge_keys_path}",
            "--actions=commit",
        ],
        env=env,
    )
    run_cli(
        cli_base_cmd()
        + [
            "delete",
            f"--node_key_list=@{node_keys_path}",
            "--actions=commit",
        ],
        env=env,
    )


@pytest.mark.e2e
def test_cli_data_commands_subgraph_e2e(
    tmp_path: Path, db: GraphDB, test_config: tuple[Path, str]
) -> None:
    config_path, schema_name = test_config
    sample_subgraph = load_subgraph_fixture(SUBGRAPH_FIXTURE_PATH)
    key_subgraph = load_subgraph_fixture(SUBGRAPH_KEYS_FIXTURE_PATH)
    sample_nodes = sample_subgraph["node_list"]
    sample_edges = sample_subgraph["edge_list"]
    key_nodes = key_subgraph["node_list"]
    key_edges = key_subgraph["edge_list"]

    node_list_path = tmp_path / "node_list.json"
    edge_list_path = tmp_path / "edge_list.json"
    node_key_list_path = tmp_path / "node_key_list.json"
    edge_key_list_path = tmp_path / "edge_key_list.json"

    write_json(node_list_path, {"node_list": sample_nodes})
    write_json(edge_list_path, {"edge_list": sample_edges})
    write_json(node_key_list_path, {"key_list": key_nodes})
    write_json(edge_key_list_path, {"key_list": key_edges})

    cli_env = os.environ.copy()
    cli_env["GRAPH_REGISTRY_CONFIG_GLOBAL"] = str(config_path)

    delete_many(node_key_list_path, edge_key_list_path, cli_env)

    try:
        run_cli(
            cli_base_cmd()
            + [
                "save",
                f"--node_list=@{node_list_path}",
                "--actions=commit",
            ],
            env=cli_env,
        )

        run_cli(
            cli_base_cmd()
            + [
                "save",
                f"--edge_list=@{edge_list_path}",
                "--actions=commit",
            ],
            env=cli_env,
        )

        for node_json in sample_nodes:
            label = node_label(node_json)
            expected_node = SpecMapper.from_node_spec(node_json)

            assert count_node_rows(db, schema_name, ENGINE_NAME, node_json) == 1, (
                f"Missing or duplicate node shell row for {label}"
            )

            basic_row = fetch_node_basic_row(db, schema_name, ENGINE_NAME, node_json)
            assert basic_row is not None, f"Missing basic node row for {label}"
            assert basic_row[0] == expected_node.title
            assert (basic_row[1] or "") == (expected_node.text_source or "")
            assert (basic_row[2] or "") == (expected_node.raw_text or "")

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
        delete_many(node_key_list_path, edge_key_list_path, cli_env)

    for edge_json in key_edges:
        assert count_edge_rows(db, schema_name, ENGINE_NAME, edge_json) == 0

    for node_json in key_nodes:
        assert count_node_rows(db, schema_name, ENGINE_NAME, node_json) == 0
