# graphregistry/domain/models/entities/mdl_subgraph.py
from __future__ import annotations
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_edge import EdgeList
from graphregistry.domain.models.entities.mdl_node import NodeList

# Model definition
class SubGraph(BaseModel):
    """Model representing a subgraph with a list of nodes and edges.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    nodes: NodeList = Field(default_factory=NodeList)
    edges: EdgeList = Field(default_factory=EdgeList)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "SubGraph":
        return cls(
            nodes=NodeList.from_list(input_json.get("nodes", [])),
            edges=EdgeList.from_list(input_json.get("edges", [])),
        )

    def to_json(self) -> dict:
        return {
            "nodes": self.nodes.to_list(),
            "edges": self.edges.to_list(),
        }
