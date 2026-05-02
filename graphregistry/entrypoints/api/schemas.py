# graphregistry/entrypoints/api/schemas.py
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.domain.models.entities.mdl_subgraph import SubGraph


#================#
# Shared schemas #
#================#

ActionName = Literal["print", "eval", "commit"]


def _default_actions() -> list[ActionName]:
    return ["eval"]


class APIRequestBase(BaseModel):
    env: str = Field(
        default="test",
        description="GraphDB engine/environment name to use for this request.",
    )


class ActionRequestBase(APIRequestBase):
    actions: list[ActionName] = Field(
        default_factory=_default_actions,
        description="Actions to perform. Use `eval` for dry-run behavior and add `commit` to persist changes.",
    )


class StatusResponse(BaseModel):
    success: bool = True
    message: str


#==============#
# Node schemas #
#==============#

class NodeListRequest(APIRequestBase):
    object_type: str = Field(..., description="Node object type to list.")
    id_pattern: str | None = Field(
        default=None,
        description="Optional wildcard pattern for object_id values. `*` is supported.",
    )


class NodeListResponse(BaseModel):
    nodes: list[NodeKey] = Field(default_factory=list)
    count: int = 0


class NodeExistsAPIRequest(APIRequestBase):
    key: NodeKey


class NodeExistsResponse(BaseModel):
    exists: bool


class NodeFetchRequest(APIRequestBase):
    key: NodeKey


class NodeFetchResponse(BaseModel):
    found: bool
    node: Node | None = None


class NodeSaveAPIRequest(ActionRequestBase):
    node: Node


class NodeSaveAPIResponse(BaseModel):
    success: bool
    node: Node


class NodeListSaveRequest(ActionRequestBase):
    node_list: NodeList


class NodeListSaveResponse(BaseModel):
    success: bool
    node_list: NodeList
    count: int


class NodeDeleteAPIRequest(ActionRequestBase):
    key: NodeKey


class NodeDeleteResponse(BaseModel):
    success: bool


class NodeDeleteManyRequest(ActionRequestBase):
    keys: list[NodeKey] = Field(default_factory=list)


class NodeDeleteManyResponse(BaseModel):
    success: bool
    results: list[bool]
    count: int


#==============#
# Edge schemas #
#==============#

class EdgeListRequest(APIRequestBase):
    from_object_type: str = Field(..., description="Source node object type.")
    to_object_type: str = Field(..., description="Target node object type.")
    id_pattern: str | None = Field(
        default=None,
        description="Optional wildcard pattern applied to edge identifiers. `*` is supported.",
    )


class EdgeListResponse(BaseModel):
    edges: list[EdgeKey] = Field(default_factory=list)
    count: int = 0


class EdgeExistsAPIRequest(APIRequestBase):
    key: EdgeKey


class EdgeExistsResponse(BaseModel):
    exists: bool


class EdgeFetchRequest(APIRequestBase):
    key: EdgeKey


class EdgeFetchResponse(BaseModel):
    found: bool
    edge: Edge | None = None


class EdgeSaveAPIRequest(ActionRequestBase):
    edge: Edge


class EdgeSaveAPIResponse(BaseModel):
    success: bool
    edge: Edge


class EdgeListSaveRequest(ActionRequestBase):
    edge_list: EdgeList


class EdgeListSaveResponse(BaseModel):
    success: bool
    edge_list: EdgeList
    count: int


class EdgeDeleteAPIRequest(ActionRequestBase):
    key: EdgeKey


class EdgeDeleteResponse(BaseModel):
    success: bool


class EdgeDeleteManyRequest(ActionRequestBase):
    keys: list[EdgeKey] = Field(default_factory=list)


class EdgeDeleteManyResponse(BaseModel):
    success: bool
    results: list[bool]
    count: int


#==================#
# Subgraph schemas #
#==================#

class SubGraphSaveRequest(ActionRequestBase):
    subgraph: SubGraph


class SubGraphSaveResponse(BaseModel):
    success: bool
    nodes_saved: int
    edges_saved: int
    subgraph: SubGraph
