from __future__ import annotations
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_edge import EdgeList
from graphregistry.domain.models.mdl_node import NodeList

# Model definition
class SubGraph(BaseModel):
    nodes: NodeList = Field(default_factory=NodeList)
    edges: EdgeList = Field(default_factory=EdgeList)
