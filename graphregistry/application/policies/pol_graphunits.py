# graphregistry/application/policies/pol_graphunits.py
"""Graph-units validation policy for API node/edge saves.

This module implements an application-level policy that enforces the
``allowed-types`` section of ``config/config_api.json``. It is intentionally
kept free of framework and persistence details: it receives plain sets of
allowed values and domain model objects, and raises a domain exception when a
node or edge type is not permitted.
"""
from __future__ import annotations

from graphregistry.domain.exceptions import DisallowedTypeError
from graphregistry.domain.models.entities.mdl_base import EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList


class GraphUnitsValidator:
    """Validate node object types and edge (from, to, context) tuples.

    The validator is configured with the allow-lists coming from the external
    API configuration. It can be injected into API entrypoints or application
    operations without coupling them to the configuration source.
    """

    def __init__(
        self,
        *,
        allowed_node_types: set[str],
        allowed_edge_tuples: set[tuple[str, str, str]],
    ) -> None:
        self.allowed_node_types = allowed_node_types
        self.allowed_edge_tuples = allowed_edge_tuples

    # --------------------------------------------------------------------- #
    # Node validation                                                       #
    # --------------------------------------------------------------------- #

    def validate_node(self, node: Node | str) -> None:
        """Validate a single node's object type."""
        node_type = node.key.object_type if isinstance(node, Node) else node
        if node_type not in self.allowed_node_types:
            raise DisallowedTypeError(
                f"Node type '{node_type}' is not an allowed type."
            )

    def validate_nodes(self, nodes: NodeList | list[Node]) -> None:
        """Validate every node object type in a list."""
        if isinstance(nodes, NodeList):
            nodes = nodes.item_list
        for node in nodes:
            self.validate_node(node)

    # --------------------------------------------------------------------- #
    # Edge validation                                                       #
    # --------------------------------------------------------------------- #

    def validate_edge(self, edge: Edge | EdgeKey | tuple[str, str, str]) -> None:
        """Validate a single edge's (from_type, to_type, context) tuple."""
        if isinstance(edge, Edge):
            key = edge.key
            edge_tuple = (key.from_object_type, key.to_object_type, key.context)
        elif isinstance(edge, EdgeKey):
            edge_tuple = (edge.from_object_type, edge.to_object_type, edge.context)
        else:
            edge_tuple = edge

        if edge_tuple not in self.allowed_edge_tuples:
            raise DisallowedTypeError(
                f"Edge type {edge_tuple} is not an allowed type."
            )

    def validate_edges(self, edges: EdgeList | list[Edge]) -> None:
        """Validate every edge tuple in a list."""
        if isinstance(edges, EdgeList):
            edges = edges.item_list
        for edge in edges:
            self.validate_edge(edge)
