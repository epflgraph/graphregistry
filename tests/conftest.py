# tests/conftest.py
"""Shared pytest fixtures and test helpers for the graphregistry test suite.

This module provides:
- Domain object factories for fast, deterministic test setup.
- In-memory fake repository adapters that implement the repository ports.
- Dependency-overridable API test client fixtures.
- Common helpers for integration/e2e tests.

The fake adapters make it possible to test the application and entrypoint layers
without a running MySQL database, which keeps the default `pytest` run fast and
independent of external services.
"""
from __future__ import annotations

from typing import Any, Iterator

import pytest
from fastapi.testclient import TestClient

from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.entrypoints.api.main import create_app
from graphregistry.entrypoints.api.router import get_edge_ops, get_node_ops
from graphregistry.domain.models.entities.mdl_base import (
    EdgeKey,
    EdgeKeyList,
    NodeFieldKey,
    NodeKey,
    NodeKeyList,
)
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeField, NodeFieldList, NodeList
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile
from graphregistry.domain.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.repositories.rpo_node import NodeRepository
from graphregistry.domain.types import ActionSet


# --------------------------------------------------------------------------- #
# Domain object factories                                                     #
# --------------------------------------------------------------------------- #

def make_node_key(
    object_type: str = "Course",
    object_id: str = "CS-433",
) -> NodeKey:
    return NodeKey(
        object_type=object_type,  # type: ignore[arg-type]
        object_id=object_id,
    )


def make_node(
    object_type: str = "Course",
    object_id: str = "CS-433",
    title: str = "Machine Learning",
    raw_text: str | None = "Learn machine learning.",
    custom_fields: list[dict[str, Any]] | None = None,
) -> Node:
    key = make_node_key(object_type, object_id)
    field_list = NodeFieldList(
        item_list=[
            NodeField(
                key=NodeFieldKey(key=key, field_language=row.get("field_language", "n/a"), field_name=row["field_name"]),
                field_value=row.get("field_value", ""),
            )
            for row in (custom_fields or [])
        ]
    )
    return Node(
        key=key,
        title=title,
        raw_text=raw_text,
        field_list=field_list,
        page_profile=PageProfile(key=key),
    )


def make_edge_key(
    from_object_type: str = "Course",
    from_object_id: str = "CS-433",
    to_object_type: str = "Person",
    to_object_id: str = "person-12345",
    context: str = "taught_by",
) -> EdgeKey:
    return EdgeKey(
        from_object_type=from_object_type,  # type: ignore[arg-type]
        from_object_id=from_object_id,
        to_object_type=to_object_type,  # type: ignore[arg-type]
        to_object_id=to_object_id,
        context=context,
    )


def make_edge(
    from_object_type: str = "Course",
    from_object_id: str = "CS-433",
    to_object_type: str = "Person",
    to_object_id: str = "person-12345",
    context: str = "taught_by",
    custom_fields: list[dict[str, Any]] | None = None,
) -> Edge:
    key = make_edge_key(
        from_object_type=from_object_type,
        from_object_id=from_object_id,
        to_object_type=to_object_type,
        to_object_id=to_object_id,
        context=context,
    )
    field_list = EdgeFieldList(
        item_list=[
            EdgeField(
                key=EdgeFieldKey(key=key, field_language=row.get("field_language", "n/a"), field_name=row["field_name"]),
                field_value=row.get("field_value", ""),
            )
            for row in (custom_fields or [])
        ]
    )
    return Edge(key=key, field_list=field_list)


# --------------------------------------------------------------------------- #
# Fake repository adapters                                                    #
# --------------------------------------------------------------------------- #

class FakeNodeRepository:
    """In-memory implementation of NodeRepository for fast unit tests."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], Node] = {}

    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str]]:
        results = [
            key.to_tuple()
            for key in (NodeKey.from_tuple(k) for k in self._store)
            if key.object_type == object_type
        ]
        if id_pattern and id_pattern != "*":
            results = [row for row in results if id_pattern.replace("*", "") in row[1]]
        return results

    def exists(self, key: NodeKey) -> bool:
        return key.to_tuple() in self._store

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        if isinstance(key_list, NodeKeyList):
            key_list = key_list.item_list
        return [self.exists(key) for key in key_list]

    def get(self, key: NodeKey) -> Node | None:
        return self._store.get(key.to_tuple())

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        if isinstance(key_list, NodeKeyList):
            key_list = key_list.item_list
        return NodeList(item_list=[node for node in (self.get(key) for key in key_list) if node is not None])

    def save(self, node: Node, actions: ActionSet = ("commit",)) -> Node:
        if "commit" not in actions:
            return node
        self._store[node.key.to_tuple()] = node.model_copy(deep=True)
        return self._store[node.key.to_tuple()]

    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("commit",)) -> NodeList:
        if isinstance(node_list, NodeList):
            node_list = node_list.item_list
        saved = [self.save(node, actions=actions) for node in node_list]
        return NodeList(item_list=saved)

    def delete(self, key: NodeKey, actions: ActionSet = ("commit",)) -> bool | None:
        if "commit" not in actions:
            return None
        return self._store.pop(key.to_tuple(), None) is not None

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        if isinstance(key_list, NodeKeyList):
            key_list = key_list.item_list
        return [self.delete(key, actions=actions) for key in key_list]

    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        results = [
            node for node in self._store.values()
            if not (node.concepts.detected.item_list or node.concepts.ai_validated.item_list or node.concepts.manually_mapped.item_list)
        ]
        if object_type:
            results = [node for node in results if node.key.object_type == object_type]
        if id_pattern and id_pattern != "*":
            results = [node for node in results if id_pattern.replace("*", "") in node.key.object_id]
        return NodeList(item_list=results)


class FakeEdgeRepository:
    """In-memory implementation of EdgeRepository for fast unit tests."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str, str, str, str], Edge] = {}

    def _tuple(self, key: EdgeKey) -> tuple[str, ...]:
        return key.to_tuple()

    def list(self, object_type: tuple[str, str], id_pattern: str | None = None) -> list[tuple[str, str, str, str, str]]:
        results = [
            key.to_tuple()
            for key in (EdgeKey.from_tuple(k) for k in self._store)
            if (key.from_object_type, key.to_object_type) == object_type
        ]
        if id_pattern and id_pattern != "*":
            results = [row for row in results if id_pattern.replace("*", "") in row[1] or id_pattern.replace("*", "") in row[3]]
        return results

    def exists(self, key: EdgeKey) -> bool:
        return self._tuple(key) in self._store

    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        return [self.exists(key) for key in key_list]

    def get(self, key: EdgeKey) -> Edge | None:
        return self._store.get(self._tuple(key))

    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        return EdgeList(item_list=[edge for edge in (self.get(key) for key in key_list) if edge is not None])

    def save(self, edge: Edge, actions: ActionSet = ("commit",)) -> Edge:
        if "commit" not in actions:
            return edge
        self._store[self._tuple(edge.key)] = edge.model_copy(deep=True)
        return self._store[self._tuple(edge.key)]

    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ("commit",)) -> EdgeList:
        if isinstance(edge_list, EdgeList):
            edge_list = edge_list.item_list
        saved = [self.save(edge, actions=actions) for edge in edge_list]
        return EdgeList(item_list=saved)

    def delete(self, key: EdgeKey, actions: ActionSet = ("commit",)) -> bool | None:
        if "commit" not in actions:
            return None
        return self._store.pop(self._tuple(key), None) is not None

    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        return [self.delete(key, actions=actions) for key in key_list]


# --------------------------------------------------------------------------- #
# Pytest fixtures                                                             #
# --------------------------------------------------------------------------- #

@pytest.fixture
def fake_node_repo() -> Iterator[FakeNodeRepository]:
    """Provide a fresh empty fake node repository."""
    yield FakeNodeRepository()


@pytest.fixture
def fake_edge_repo() -> Iterator[FakeEdgeRepository]:
    """Provide a fresh empty fake edge repository."""
    yield FakeEdgeRepository()


@pytest.fixture
def node_ops(fake_node_repo: FakeNodeRepository) -> NodeOperations:
    """Provide NodeOperations backed by a fake repository."""
    return NodeOperations(repo=fake_node_repo)


@pytest.fixture
def edge_ops(fake_edge_repo: FakeEdgeRepository) -> EdgeOperations:
    """Provide EdgeOperations backed by a fake repository."""
    return EdgeOperations(repo=fake_edge_repo)


@pytest.fixture
def api_client() -> Iterator[TestClient]:
    """Build a TestClient with fake repositories injected into the router."""
    node_repo = FakeNodeRepository()
    edge_repo = FakeEdgeRepository()

    def _node_ops() -> NodeOperations:
        return NodeOperations(repo=node_repo)

    def _edge_ops() -> EdgeOperations:
        return EdgeOperations(repo=edge_repo)

    app = create_app()
    app.dependency_overrides[get_node_ops] = _node_ops
    app.dependency_overrides[get_edge_ops] = _edge_ops

    yield TestClient(app)
    app.dependency_overrides.clear()
