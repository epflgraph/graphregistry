# tests/end2end_tests/test_cli_data_insert_nodelist.py
from __future__ import annotations
from typing import Any, cast
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

CLI_CMD = [
    "graphregistry",
    "data",
    "insert",
    "--node_list=@./tests/fixtures/e2e_data_insert_graph_sample_set_nodelist.json",
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

def delete_node(db: GraphDB, node_json: dict) -> None:
    institution_id, object_type, object_id = node_key_tuple(node_json)

    # Double-check schema name
    if not SCHEMA_NAME.startswith("_1_DEV_"):
        raise AssertionError(
            f"Attempting to delete node from schema '{SCHEMA_NAME}' which does not start with '_1_DEV_'. "
            "Aborting to prevent potential data loss. Please set execution mode to 'dev' in your config."
        )

    query = f"""
    DELETE FROM {SCHEMA_NAME}.Nodes_N_Object
    WHERE institution_id = "{institution_id}"
      AND object_type    = "{object_type}"
      AND object_id      = "{object_id}";
    """
    db.execute_query(
        engine_name=ENGINE_NAME,
        query=query,
        commit=True,
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

    # -------------------------------------------------------------- #
    # Defensive cleanup before test                                  #
    # -------------------------------------------------------------- #
    for node_json in sample_nodes:
        delete_node(db, node_json)

    try:
        # ---------------------------------------------------------- #
        # Execute real CLI command                                   #
        # ---------------------------------------------------------- #
        result = subprocess.run(
            CLI_CMD,
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

        # ---------------------------------------------------------- #
        # Explicit DB checks                                         #
        # ---------------------------------------------------------- #
        for node_json in sample_nodes:
            # 1) Node shell row exists
            assert count_node_rows(db, node_json) == 1

            # 2) Basic node content
            basic_row = fetch_node_basic_row(db, node_json)
            assert basic_row is not None

            expected_title = node_json.get("object_title", "")
            expected_text_source = node_json.get("text_source", "")
            expected_raw_text = node_json.get("raw_text", "")

            assert basic_row[0] == expected_title
            assert basic_row[1] == expected_text_source
            assert basic_row[2] == expected_raw_text

            # 3) Custom fields
            expected_custom_fields = node_json.get("custom_fields", [])
            db_custom_fields = fetch_custom_fields(db, node_json)

            db_field_map = {
                (row[0], row[1]): row[2]
                for row in db_custom_fields
            }

            for field in expected_custom_fields:
                key = (field["field_language"], field["field_name"])
                assert key in db_field_map
                assert db_field_map[key] == field["field_value"]

            # 4) Page profile row exists if expected
            expected_page_profile = node_json.get("page_profile")
            db_page_profile = fetch_page_profile_row(db, node_json)

            # Print all data extracted with SQL for debugging
            # Use rich library for better formatting
            # import rich
            # rich.print(f"\nNode: {node_json['object_id']}")
            # rich.print("Basic Row:")
            # rich.print(basic_row)
            # rich.print("Custom Fields:")
            # rich.print(db_custom_fields)
            # rich.print("Page Profile Row:")
            # rich.print(db_page_profile)

            # ==============================
            # Assertion block
            # ==============================

            # 4) Page profile row exists and matches expected values
            expected_page_profile = node_json.get("page_profile", {})
            assert db_page_profile is not None, f"Missing page profile row for node {node_json['object_id']}"

            from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import PAGE_PROFILE_COLUMNS

            # `SELECT *` returns:
            #   institution_id, object_type, object_id, <page profile columns...>, <extra trailing DB columns...>
            assert len(db_page_profile) >= 3 + len(PAGE_PROFILE_COLUMNS), (
                f"Unexpected page profile row length for node {node_json['object_id']}: "
                f"got {len(db_page_profile)}, expected at least {3 + len(PAGE_PROFILE_COLUMNS)}"
            )

            assert db_page_profile[0] == node_json["institution_id"]
            assert db_page_profile[1] == node_json["object_type"]
            assert db_page_profile[2] == node_json["object_id"]

            db_page_profile_map = dict(zip(PAGE_PROFILE_COLUMNS, db_page_profile[3:3 + len(PAGE_PROFILE_COLUMNS)]))

            for field_name, expected_value in expected_page_profile.items():
                assert field_name in db_page_profile_map, (
                    f"Missing page profile column '{field_name}' for node {node_json['object_id']}"
                )

                actual_value = db_page_profile_map[field_name]

                # Normalize bools because MySQL stores them as 0/1
                if isinstance(expected_value, bool):
                    assert int(bool(actual_value)) == int(expected_value), (
                        f"Mismatch in page_profile[{field_name}] for node {node_json['object_id']}: "
                        f"expected {int(expected_value)!r}, got {actual_value!r}"
                    )
                else:
                    assert actual_value == expected_value, (
                        f"Mismatch in page_profile[{field_name}] for node {node_json['object_id']}: "
                        f"expected {expected_value!r}, got {actual_value!r}"
                    )

            # 5) Make sure the exact number of custom fields was written
            assert len(db_custom_fields) == len(expected_custom_fields), (
                f"Unexpected number of custom fields for node {node_json['object_id']}: "
                f"expected {len(expected_custom_fields)}, got {len(db_custom_fields)}"
            )

            expected_field_keys = {
                (field["field_language"], field["field_name"])
                for field in expected_custom_fields
            }
            actual_field_keys = {
                (row[0], row[1])
                for row in db_custom_fields
            }

            assert actual_field_keys == expected_field_keys, (
                f"Custom field key mismatch for node {node_json['object_id']}: "
                f"expected {expected_field_keys}, got {actual_field_keys}"
            )

            # 6) Extra safety: verify page-profile identity and a few invariant columns
            assert db_page_profile_map["short_code"] == node_json["object_id"]
            assert int(bool(db_page_profile_map["is_visible"])) == int(bool(expected_page_profile.get("is_visible", True)))

            # 7) Extra safety: if EN/FR URLs or keys are present in fixture, verify them explicitly
            for invariant_col in (
                "external_key_en",
                "external_key_fr",
                "external_url_en",
                "external_url_fr",
                "name_en_value",
                "name_fr_value",
                "description_short_en_value",
                "description_short_fr_value",
                "description_medium_en_value",
                "description_medium_fr_value",
                "description_long_en_value",
                "description_long_fr_value",
            ):
                if invariant_col in expected_page_profile:
                    assert db_page_profile_map[invariant_col] == expected_page_profile[invariant_col], (
                        f"Mismatch in invariant page_profile[{invariant_col}] for node {node_json['object_id']}: "
                        f"expected {expected_page_profile[invariant_col]!r}, got {db_page_profile_map[invariant_col]!r}"
                    )

    finally:
        # ---------------------------------------------------------- #
        # Cleanup                                                    #
        # ---------------------------------------------------------- #
        for node_json in sample_nodes:
            delete_node(db, node_json)
