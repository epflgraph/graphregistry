# tests/helpers/fixtures.py
"""Shared fixture loading helpers for integration and end-to-end tests."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graphregistry.common.config import GlobalConfig


def load_json_fixture(path: Path) -> Any:
    """Load a JSON fixture from disk."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_subgraph_fixture(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Load a subgraph fixture containing node_list and edge_list."""
    data = load_json_fixture(path)
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


class FixedTestSchemaResolver:
    """Fixed schema resolver used by integration tests against a dedicated test schema."""

    def __init__(self, engine_name: str, schema_name: str) -> None:
        self.engine_name = engine_name
        self.schema_name = schema_name

    def for_node(self, key: Any) -> tuple[str, str]:
        return (self.engine_name, self.schema_name)

    def for_edge(self, key: Any) -> tuple[str, str]:
        return (self.engine_name, self.schema_name)

    def for_object_type(self, object_type: str) -> tuple[str, str]:
        return (self.engine_name, self.schema_name)


def get_test_schema_name(glbcfg: GlobalConfig | None = None) -> str:
    """Return a dedicated test schema name derived from the configured registry schema."""
    if glbcfg is None:
        glbcfg = GlobalConfig()
    registry_schema = glbcfg.schema_registry
    if not registry_schema.startswith("_1_DEV_"):
        raise AssertionError(
            f"Test is configured to use schema '{registry_schema}' which does not start with '_1_DEV_'. "
            "Please set execution mode to 'dev' in your config to ensure tests run against a safe schema."
        )
    return "_0_PYTESTS_" + registry_schema.replace("_1_DEV_", "")
