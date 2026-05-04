# graphregistry/entrypoints/api/schemas.py
from __future__ import annotations
from typing import Literal, Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.domain.models.entities.mdl_subgraph import SubGraph

# Define a type for supported field languages, which can be used in custom fields of nodes
FieldLanguage = Literal['n/a', 'en', 'fr', 'de', 'it']

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

class CustomFieldInput(BaseModel):
    field_language : FieldLanguage
    field_name     : str
    field_value    : str

class NodeSimplifiedInput(BaseModel):
    object_type    : str
    object_id      : str
    object_title   : str | None = ""
    text_source    : str | None = ""
    raw_text       : str | None = ""
    custom_fields  : list[CustomFieldInput] = Field(default_factory=list)
    page_profile   : dict[str, Any] | None = None

class EdgeSimplifiedKey(BaseModel):
    from_object_type : str
    from_object_id   : str
    to_object_type   : str
    to_object_id     : str
    context          : str

#==============#
# Node schemas #
#==============#

# api/nodes/list
class NodeListRequest(BaseModel):
    object_type : str = Field(..., description="Node object type to list.")
    id_pattern  : str | None = Field(
        default     = None,
        description = "Optional wildcard pattern for object_id values. `*` is supported.",
    )

class NodeListResponse(BaseModel):
    nodes : list[NodeKey] = Field(default_factory=list)
    count : int = 0

# api/nodes/exists
class NodeExistsAPIRequest(BaseModel):
    key: NodeSimplifiedKey

class NodeExistsResponse(BaseModel):
    exists: bool

# api/nodes/get
class NodeGetRequest(BaseModel):
    key: NodeSimplifiedKey

class NodeGetResponse(BaseModel):
    found : bool
    node  : dict[str, Any] | None = None

# api/nodes/save
class NodeSaveAPIRequest(BaseModel):
    node: NodeSimplifiedInput

class NodeSaveAPIResponse(BaseModel):
    success   : bool
    saved_key : dict[str, str] | None = None

# api/nodes/save_many
class NodeListSaveRequest(BaseModel):
    node_list : list[NodeSimplifiedInput] = Field(default_factory=list)

class NodeListSaveResponse(BaseModel):
    success    : bool
    saved_keys : list[dict[str, str]] = Field(default_factory=list)
    count      : int

# api/nodes/delete
class NodeDeleteAPIRequest(BaseModel):
    key: NodeSimplifiedKey

class NodeDeleteResponse(BaseModel):
    success: bool

# api/nodes/delete_many
class NodeDeleteManyRequest(BaseModel):
    key_list: list[NodeSimplifiedKey] = Field(default_factory=list)

class NodeDeleteManyResponse(BaseModel):
    success   : bool
    results   : list[bool] = Field(default_factory=list)
    n_deleted : int

#==============#
# Edge schemas #
#==============#

# api/edges/list
class EdgeListRequest(BaseModel):
    from_object_type : str = Field(..., description="Source node object type.")
    to_object_type   : str = Field(..., description="Target node object type.")
    id_pattern       : str | None = Field(
        default     = None,
        description = "Optional wildcard pattern applied to edge identifiers. `*` is supported.",
    )

class EdgeListResponse(BaseModel):
    edges : list[EdgeKey] = Field(default_factory=list)
    count : int = 0

# api/edges/exists
class EdgeExistsAPIRequest(BaseModel):
    key: EdgeSimplifiedKey

class EdgeExistsResponse(BaseModel):
    exists: bool

# api/edges/get
class EdgeGetRequest(BaseModel):
    key: EdgeSimplifiedKey

class EdgeGetResponse(BaseModel):
    found : bool
    edge  : dict[str, Any] | None = None

# api/edges/save
class EdgeSaveAPIRequest(BaseModel):
    edge: EdgeSimplifiedKey

class EdgeSaveAPIResponse(BaseModel):
    success   : bool
    saved_key : dict[str, str] | None = None

# api/edges/save_many
class EdgeListSaveRequest(BaseModel):
    edge_list: list[EdgeSimplifiedKey] = Field(default_factory=list)

class EdgeListSaveResponse(BaseModel):
    success    : bool
    saved_keys : list[dict[str, str]] = Field(default_factory=list)
    count      : int

# api/edges/delete
class EdgeDeleteAPIRequest(BaseModel):
    key: EdgeSimplifiedKey

class EdgeDeleteResponse(BaseModel):
    success: bool

# api/edges/delete_many
class EdgeDeleteManyRequest(BaseModel):
    key_list: list[EdgeSimplifiedKey] = Field(default_factory=list)

class EdgeDeleteManyResponse(BaseModel):
    success   : bool
    results   : list[bool] = Field(default_factory=list)
    n_deleted : int

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
