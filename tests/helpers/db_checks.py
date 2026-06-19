# tests/helpers/db_checks.py
"""Shared database assertion helpers for integration and end-to-end tests."""
from __future__ import annotations

from typing import Any, cast

from graphdb.core.graphdb import GraphDB

from graphregistry.entrypoints.mappers import SpecMapper


def expected_node_key(node_json: dict[str, Any]) -> tuple[str, str, str]:
    node = SpecMapper.from_node_spec(node_json)
    return node.key.to_tuple()


def expected_edge_key(edge_json: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    edge = SpecMapper.from_edge_spec(edge_json)
    return edge.key.to_tuple()


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


def _node_where_clause(node_json: dict[str, Any]) -> tuple[str, str, str]:
    institution_id, object_type, object_id = expected_node_key(node_json)
    return (
        f'institution_id = "{institution_id}"',
        f'object_type    = "{object_type}"',
        f'object_id      = "{object_id}"',
    )


def _edge_where_clause(edge_json: dict[str, Any]) -> tuple[str, ...]:
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
        f'from_institution_id = "{from_institution_id}"',
        f'from_object_type    = "{from_object_type}"',
        f'from_object_id      = "{from_object_id}"',
        f'to_institution_id   = "{to_institution_id}"',
        f'to_object_type      = "{to_object_type}"',
        f'to_object_id        = "{to_object_id}"',
        f'context             = "{context}"',
    )


def count_node_rows(db: GraphDB, schema_name: str, engine_name: str, node_json: dict[str, Any]) -> int:
    wheres = _node_where_clause(node_json)
    query = f"""
    SELECT COUNT(*)
    FROM {schema_name}.Nodes_N_Object
    WHERE {" AND ".join(wheres)};
    """
    rows = db.execute_query(engine_name=engine_name, query=query)
    return int(rows[0][0])


def fetch_node_basic_row(
    db: GraphDB, schema_name: str, engine_name: str, node_json: dict[str, Any]
) -> tuple | None:
    wheres = _node_where_clause(node_json)
    query = f"""
    SELECT object_title, text_source, raw_text
    FROM {schema_name}.Nodes_N_Object
    WHERE {" AND ".join(wheres)};
    """
    rows = db.execute_query(engine_name=engine_name, query=query)
    return cast(tuple | None, rows[0] if rows else None)


def fetch_node_custom_fields(
    db: GraphDB, schema_name: str, engine_name: str, node_json: dict[str, Any]
) -> list[tuple[str, str, str]]:
    wheres = _node_where_clause(node_json)
    query = f"""
    SELECT field_language, field_name, field_value
    FROM {schema_name}.Data_N_Object_T_CustomFields
    WHERE {" AND ".join(wheres)};
    """
    return cast(list[tuple[str, str, str]], db.execute_query(engine_name=engine_name, query=query))


def count_edge_rows(db: GraphDB, schema_name: str, engine_name: str, edge_json: dict[str, Any]) -> int:
    wheres = _edge_where_clause(edge_json)
    query = f"""
    SELECT COUNT(*)
    FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent
    WHERE {" AND ".join(wheres)};
    """
    rows = db.execute_query(engine_name=engine_name, query=query)
    return int(rows[0][0])


def fetch_edge_custom_fields(
    db: GraphDB, schema_name: str, engine_name: str, edge_json: dict[str, Any]
) -> list[tuple[str, str, str]]:
    wheres = _edge_where_clause(edge_json)
    query = f"""
    SELECT field_language, field_name, field_value
    FROM {schema_name}.Data_N_Object_N_Object_T_CustomFields
    WHERE {" AND ".join(wheres)};
    """
    return cast(list[tuple[str, str, str]], db.execute_query(engine_name=engine_name, query=query))


def field_map(fields: list[dict[str, Any]]) -> dict[tuple[str, str], str]:
    result: dict[tuple[str, str], str] = {}
    for field in fields:
        if "field_language" in field:
            # Flat fixture format
            language = field["field_language"]
            name = field["field_name"]
            value = field["field_value"]
        elif "key" in field:
            # NodeField.to_json() nested format
            language = field["key"]["field_language"]
            name = field["key"]["field_name"]
            value = field["field_value"]
        else:
            raise ValueError(f"Unrecognized field shape: {field}")
        result[(language, name)] = value
    return result


def db_field_map(rows: list[tuple[str, str, str]]) -> dict[tuple[str, str], str]:
    return {
        (field_language, field_name): field_value
        for field_language, field_name, field_value in rows
    }
