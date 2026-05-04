# graphregistry/entrypoints/api/schemas.py
from __future__ import annotations
from typing import Literal, Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.domain.models.entities.mdl_subgraph import SubGraph

#================#
# Shared schemas #
#================#

# Generic response schema for API endpoints, indicating success and providing a message
class StatusResponse(BaseModel):
    success: bool = True
    message: str

class NodeSimplifiedKey(BaseModel):
    object_type    : str
    object_id      : str

class NodeCustomFieldInput(BaseModel):
    field_language : str = "n/a"
    field_name     : str
    field_value    : str

class NodeSimplifiedInput(BaseModel):
    institution_id : str = "EPFL"
    object_type    : str
    object_id      : str
    object_title   : str | None = ""
    text_source    : str | None = ""
    raw_text       : str | None = ""
    custom_fields  : list[NodeCustomFieldInput] = Field(default_factory=list)
    page_profile   : dict[str, Any] | None = None

#==============#
# Node schemas #
#==============#

# api/nodes/list
class NodeListRequest(BaseModel):
    object_type : str = Field(..., description="Node object type to list.")
    id_pattern  : str | None = Field(
        default=None,
        description="Optional wildcard pattern for object_id values. `*` is supported.",
    )

class NodeListResponse(BaseModel):
    nodes : list[NodeKey] = Field(default_factory=list)
    count : int = 0

# api/nodes/exists
class NodeExistsAPIRequest(BaseModel):
    object_type : str
    object_id   : str

class NodeExistsResponse(BaseModel):
    exists : bool

# api/nodes/fetch
class NodeFetchRequest(BaseModel):
    object_type : str
    object_id   : str

class NodeFetchResponse(BaseModel):
    found : bool
    node  : dict[str, Any] | None = None

# api/nodes/save
class NodeSaveAPIRequest(BaseModel):
    node : NodeSimplifiedInput
    detect_concepts : bool = False

class NodeSaveAPIResponse(BaseModel):
    success   : bool
    saved_key : NodeKey
    n_concepts_detected : int = 0

# api/nodes/save-many
class NodeListSaveRequest(BaseModel):
    node_list : list[NodeSimplifiedInput] = Field(default_factory=list)
    detect_concepts : bool = False

class NodeListSaveResponse(BaseModel):
    success    : bool
    saved_keys : list[NodeKey] = Field(default_factory=list)
    count      : int
    total_detected_concepts : int = 0

# api/nodes/delete
class NodeDeleteAPIRequest(BaseModel):
    object_type : str
    object_id   : str

class NodeDeleteResponse(BaseModel):
    success: bool

# api/nodes/delete-many
class NodeDeleteManyRequest(BaseModel):
    key_list : list[NodeSimplifiedKey] = Field(default_factory=list)

class NodeDeleteManyResponse(BaseModel):
    success   : bool
    results   : list[bool]
    n_deleted : int

#==============#
# Edge schemas #
#==============#

class EdgeListRequest(BaseModel):
    from_object_type: str = Field(..., description="Source node object type.")
    to_object_type: str = Field(..., description="Target node object type.")
    id_pattern: str | None = Field(
        default=None,
        description="Optional wildcard pattern applied to edge identifiers. `*` is supported.",
    )

class EdgeListResponse(BaseModel):
    edges: list[EdgeKey] = Field(default_factory=list)
    count: int = 0

class EdgeExistsAPIRequest(BaseModel):
    key: EdgeKey

class EdgeExistsResponse(BaseModel):
    exists: bool

class EdgeFetchRequest(BaseModel):
    key: EdgeKey

class EdgeFetchResponse(BaseModel):
    found: bool
    edge: Edge | None = None

class EdgeSaveAPIRequest(BaseModel):
    edge: Edge

class EdgeSaveAPIResponse(BaseModel):
    success: bool
    edge: Edge

class EdgeListSaveRequest(BaseModel):
    edge_list: EdgeList

class EdgeListSaveResponse(BaseModel):
    success: bool
    edge_list: EdgeList
    count: int

class EdgeDeleteAPIRequest(BaseModel):
    key: EdgeKey

class EdgeDeleteResponse(BaseModel):
    success: bool

class EdgeDeleteManyRequest(BaseModel):
    keys: list[EdgeKey] = Field(default_factory=list)

class EdgeDeleteManyResponse(BaseModel):
    success: bool
    results: list[bool]
    count: int

#==================#
# Subgraph schemas #
#==================#

class SubGraphSaveRequest(BaseModel):
    subgraph: SubGraph

class SubGraphSaveResponse(BaseModel):
    success: bool
    nodes_saved: int
    edges_saved: int
    subgraph: SubGraph
