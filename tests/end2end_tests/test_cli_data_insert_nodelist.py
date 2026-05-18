# tests/end2end_tests/test_cli_data_insert_nodelist.py
from __future__ import annotations
from typing import cast
from pathlib import Path
from graphregistry.common.config import GlobalConfig
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
import pytest, json, subprocess

# Initialize global config to ensure schema names are available
glbcfg = GlobalConfig()

# ------------------------------------------------------------------ #
# Configuration                                                      #
# ------------------------------------------------------------------ #

# Define CLI command as a constant list for reuse in insert operations
CLI_CMD_INSERT = [
    "graphregistry", "data", "insert",
    "--node_list=@./tests/fixtures/e2e_data_insert_graph_sample_set_nodelist.json",
    "--actions=commit",
]

# Define CLI command for deleting nodes using the node key list fixture
CLI_CMD_DELETE = [
    "graphregistry", "data", "delete",
    "--node_list=@tests/fixtures/e2e_data_insert_graph_sample_set_nodekeylist.json",
    "--actions=commit",
]

# Resolve the fixture path relative to this test file, going up to the project root and then into tests/fixtures
FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_nodelist.json")

# Define the engine name constant used in the schema resolver
ENGINE_NAME = 'xaas_coresrv'

# Use registry schema from global config
# IMPORTANT: make sure execution mode is set to 'dev'
SCHEMA_NAME = glbcfg.schema_registry

# Force test fail if config is not set to dev to prevent accidental data pollution
# Do this by checking the schema name's prefix
if not SCHEMA_NAME.startswith("_1_DEV_"):
    raise AssertionError(
        f"Test is configured to use schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
        "Please set execution mode to 'dev' in your config to ensure tests run against a safe schema."
    )

# ------------------------------------------------------------------ #
# Helpers                                                            #
# ------------------------------------------------------------------ #

def load_sample_nodes() -> list[dict]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected node_list fixture to be a JSON list.")

    return data


def make_db() -> GraphDB:
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


def node_key_tuple(node_json: dict) -> tuple[str, str, str]:
    return (
        str(node_json["institution_id"]),
        str(node_json["object_type"]),
        str(node_json["object_id"]),
    )

def delete_nodes() -> None:
    if not SCHEMA_NAME.startswith("_1_DEV_"):
        raise AssertionError(
            f"Attempting to delete nodes from schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
            "Aborting to prevent potential data loss. Please set execution mode to 'dev' in your config."
        )

    result = subprocess.run(
        CLI_CMD_DELETE,
        text=True,
        capture_output=True,
        check=False,
    )

    print("\nDELETE STDOUT:\n", result.stdout)
    print("\nDELETE STDERR:\n", result.stderr)

    assert result.returncode == 0, (
        f"CLI delete failed with return code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

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


# ------------------------------------------------------------------ #
# Test                                                               #
# ------------------------------------------------------------------ #

@pytest.mark.e2e
def test_cli_data_insert_node_list_e2e() -> None:
    sample_nodes = load_sample_nodes()
    db = make_db()

    delete_nodes()

    try:
        result = subprocess.run(
            CLI_CMD_INSERT,
            text=True,
            capture_output=True,
            check=False,
        )

        print("\nSTDOUT:\n", result.stdout)
        print("\nSTDERR:\n", result.stderr)

        assert result.returncode == 0, (
            f"CLI failed with return code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import (
            PAGE_PROFILE_COLUMNS,
        )

        for node_json in sample_nodes:
            node_label = (
                f"{node_json['institution_id']}:"
                f"{node_json['object_type']}:"
                f"{node_json['object_id']}"
            )

            # 1) Node shell row exists
            assert count_node_rows(db, node_json) == 1, (
                f"Missing or duplicate node shell row for {node_label}"
            )

            # 2) Basic node content
            basic_row = fetch_node_basic_row(db, node_json)
            assert basic_row is not None, f"Missing basic node row for {node_label}"

            expected_title = node_json.get("object_title", "")
            expected_text_source = node_json.get("text_source") or ""
            expected_raw_text = node_json.get("raw_text") or ""

            assert basic_row[0] == expected_title
            assert (basic_row[1] or "") == expected_text_source
            assert (basic_row[2] or "") == expected_raw_text

            # 3) Custom fields
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

            if True:  # Set to False to disable rich printing of field maps
                from rich import print as rprint

                rprint(f"\n[bold cyan]CHECKING CUSTOM FIELDS FOR:[/bold cyan] {node_label}")
                rprint("[bold]Expected:[/bold]", expected_field_map)
                rprint("[bold]Actual:[/bold]", actual_field_map)

            assert actual_field_map == expected_field_map, (
                f"Custom fields mismatch for node {node_label}\n"
                f"Expected:\n{expected_field_map}\n"
                f"Actual:\n{actual_field_map}"
            )

            # 4) Page profile
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

                if True:  # Set to False to disable rich printing of page profile maps
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
        delete_nodes()