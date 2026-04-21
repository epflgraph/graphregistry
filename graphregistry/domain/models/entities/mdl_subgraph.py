# graphregistry/domain/models/mdl_subgraph.py
from __future__ import annotations
from typing import Iterator
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import NodeKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList

# Model definition
class SubGraph(BaseModel):
    nodes: NodeList = Field(default_factory=NodeList)
    edges: EdgeList = Field(default_factory=EdgeList)

    @classmethod
    def from_json(cls, json_data: dict) -> "SubGraph":
        return cls(
            nodes=NodeList.from_json(json_data.get("nodes", [])),
            edges=EdgeList.from_json(json_data.get("edges", [])),
        )

    def to_json(self) -> dict:
        return {
            "nodes": self.nodes.to_json(),
            "edges": self.edges.to_json(),
        }

    def iter_nodes(self) -> Iterator[Node]:
        return self.nodes.iter_nodes()

    def iter_edges(self) -> Iterator[Edge]:
        return self.edges.iter_edges()

    def __bool__(self) -> bool:
        return bool(self.nodes) or bool(self.edges)

    def node_count(self) -> int:
        return len(self.nodes)

    def edge_count(self) -> int:
        return len(self.edges)

    def append_node(self, node: Node) -> None:
        self.nodes.append(node)

    def append_edge(self, edge: Edge) -> None:
        self.edges.append(edge)

    def extend_nodes(self, nodes: list[Node]) -> None:
        self.nodes.extend(nodes)

    def extend_edges(self, edges: list[Edge]) -> None:
        self.edges.extend(edges)

    def get_node(self, key: NodeKey) -> Node | None:
        return self.nodes.get(key)

    def has_node(self, key: NodeKey) -> bool:
        return self.get_node(key) is not None

    def get_edge(self, key: EdgeKey) -> Edge | None:
        for edge in self.edges.edge_list:
            if edge.key == key:
                return edge
        return None

    def has_edge(self, key: EdgeKey) -> bool:
        return self.get_edge(key) is not None

    def node_keys(self) -> list[NodeKey]:
        return self.nodes.keys()

    def edge_keys(self) -> list[EdgeKey]:
        return [edge.key for edge in self.edges.edge_list]
