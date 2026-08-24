# graphregistry/adapters/persistence/mysql/mappers/amp_subgraph.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_edge import MySQLEdgeMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.domain.models.entities.mdl_subgraph import SubGraph

# Class definition
class MySQLSubGraphMapper:
    """
    Maps between portable subgraph payloads and the domain SubGraph model.

    Node payloads are delegated to MySQLNodeMapper.
    Edge payloads are delegated to MySQLEdgeMapper.
    """

    @staticmethod
    def from_dict(data: dict[str, Any]) -> SubGraph:
        assert isinstance(data, dict), "Input data must be a dictionary"
        assert "nodes" in data and isinstance(data["nodes"], list), "Input data must contain a 'nodes' list"
        assert "edges" in data and isinstance(data["edges"], list), "Input data must contain an 'edges' list"

        return SubGraph(
            nodes=NodeList(
                item_list=MySQLNodeMapper.from_simplified_dict_list(data["nodes"])
            ),
            edges=EdgeList(
                item_list=MySQLEdgeMapper.from_simplified_dict_list(data["edges"])
            ),
        )

    @staticmethod
    def to_dict(subgraph: SubGraph) -> dict[str, Any]:
        return {
            "nodes": MySQLNodeMapper.to_simplified_dict_list(subgraph.nodes.item_list),
            "edges": MySQLEdgeMapper.to_simplified_dict_list(subgraph.edges.item_list),
        }

    @staticmethod
    def from_parts(
        nodes: list[Node] | NodeList | None = None,
        edges: list[Edge] | EdgeList | None = None,
    ) -> SubGraph:
        if isinstance(nodes, NodeList):
            node_list = nodes
        else:
            node_list = NodeList(item_list=list(nodes or []))

        if isinstance(edges, EdgeList):
            edge_list = edges
        else:
            edge_list = EdgeList(item_list=list(edges or []))

        return SubGraph(nodes=node_list, edges=edge_list)
