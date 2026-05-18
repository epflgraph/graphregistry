# tests/end2end_tests/test_cli_data_insert_edgelist.py
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
    "--edge_list=@./tests/fixtures/e2e_data_insert_graph_sample_set_edgelist.json",
    "--actions=commit",
]

# Define CLI command for deleting edges using the edge key list fixture
CLI_CMD_DELETE = [
    "graphregistry", "data", "delete",
    "--edge_list=@tests/fixtures/e2e_data_insert_graph_sample_set_edgekeylist.json",
    "--actions=commit",
]

# Resolve the fixture path relative to this test file, going up to the project root and then into tests/fixtures
FIXTURE_PATH = Path("./tests/fixtures/e2e_data_insert_graph_sample_set_edgelist.json")

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

def load_sample_edges() -> list[dict]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise AssertionError("Expected edge_list fixture to be a JSON list.")

    return data


def make_db() -> GraphDB:
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


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



def delete_edges() -> None:
    if not SCHEMA_NAME.startswith("_1_DEV_"):
        raise AssertionError(
            f"Attempting to delete edges from schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
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


def count_edge_rows(db: GraphDB, edge_json: dict) -> int:
    from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context = edge_key_tuple(edge_json)
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


def fetch_edge_basic_row(db: GraphDB, edge_json: dict) -> tuple | None:
    from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context = edge_key_tuple(edge_json)
    query = f"""
    SELECT object_title, text_source, raw_text
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
    return cast(tuple | None, rows[0] if rows else None)


def fetch_custom_fields(db: GraphDB, edge_json: dict) -> list[tuple[str, str, str]]:
    from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context = edge_key_tuple(edge_json)
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


def fetch_page_profile_row(db: GraphDB, edge_json: dict) -> tuple | None:
    from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context = edge_key_tuple(edge_json)
    query = f"""
    SELECT *
    FROM {SCHEMA_NAME}.Data_N_Object_T_PageProfile
    WHERE from_institution_id = "{from_institution_id}"
      AND from_object_type    = "{from_object_type}"
      AND from_object_id      = "{from_object_id}"
      AND to_institution_id   = "{to_institution_id}"
      AND to_object_type      = "{to_object_type}"
      AND to_object_id        = "{to_object_id}"
      AND context             = "{context}";
    """
    rows = db.execute_query(engine_name=ENGINE_NAME, query=query)
    return cast(tuple | None, rows[0] if rows else None)


# ------------------------------------------------------------------ #
# Test                                                               #
# ------------------------------------------------------------------ #

@pytest.mark.e2e
def test_cli_data_insert_edge_list_e2e() -> None:
    sample_edges = load_sample_edges()
    db = make_db()

    delete_edges()

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

        for edge_json in sample_edges:
            edge_label = (
                f"{edge_json['from_institution_id']}:{edge_json['from_object_type']}:{edge_json['from_object_id']} -> "
                f"{edge_json['to_institution_id']}:{edge_json['to_object_type']}:{edge_json['to_object_id']} "
                f"({edge_json['context']})"
            )

            # 1) Edge shell row exists
            assert count_edge_rows(db, edge_json) == 1

            # 2) Custom fields
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

            if True:  # Set to False to disable rich printing of field maps
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
        delete_edges()
