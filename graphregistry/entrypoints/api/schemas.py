# graphregistry/entrypoints/api/schemas.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
# Import spec versions for response consistency
from graphregistry.entrypoints.schemas import NodeKeySpec, EdgeKeySpec
from graphregistry.entrypoints.schemas import NodeSpec, EdgeSpec, NodeKeySpec, EdgeKeySpec

# Generic response schema for API endpoints, indicating success and providing a message
class StatusResponse(BaseModel):
    success: bool = True
    message: str

#==============#
# Node schemas #
#==============#

# api/nodes/list
class APINodesListRequest(BaseModel):
    type : str = Field(..., description="Node object type to list.")
    id_pattern  : str | None = Field(
        default     = None,
        description = "Optional wildcard pattern for object id values. `*` is supported.",
    )

class APINodesListResponse(BaseModel):
    # Use NodeKeySpec for consistency with request schema (type/id)
    nodes : list[NodeKeySpec] = Field(default_factory=list)
    count : int = 0

# api/nodes/exists
class APINodesExistsRequest(BaseModel):
    key: NodeKeySpec

class APINodesExistsResponse(BaseModel):
    exists: bool

# api/nodes/exists_many
class APINodesExistsManyRequest(BaseModel):
    key_list: list[NodeKeySpec] = Field(default_factory=list)

class APINodesExistsManyResponse(BaseModel):
    exist_keys : list[bool] = Field(default_factory=list)
    count      : int

# api/nodes/get
class APINodesGetRequest(BaseModel):
    key: NodeKeySpec

class APINodesGetResponse(BaseModel):
    found : bool
    node  : dict[str, Any] | None = None

# api/nodes/get_many
class APINodesGetManyRequest(BaseModel):
    key_list: list[NodeKeySpec] = Field(default_factory=list)

class APINodesGetManyResponse(BaseModel):
    found_keys : list[bool] = Field(default_factory=list)
    nodes      : list[dict[str, Any] | None] = Field(default_factory=list)
    count      : int

# api/nodes/save
class APINodesSaveRequest(BaseModel):
    node: NodeSpec

class APINodesSaveResponse(BaseModel):
    success   : bool
    saved_key : dict[str, str] | None = None

# api/nodes/save_many
class APINodesSaveManyRequest(BaseModel):
    node_list : list[NodeSpec] = Field(default_factory=list)

class APINodesSaveManyResponse(BaseModel):
    success    : bool
    saved_keys : list[dict[str, str]] = Field(default_factory=list)
    count      : int

# api/nodes/delete
class APINodesDeleteRequest(BaseModel):
    key: NodeKeySpec

class APINodesDeleteResponse(BaseModel):
    success: bool

# api/nodes/delete_many
class APINodesDeleteManyRequest(BaseModel):
    key_list: list[NodeKeySpec] = Field(default_factory=list)

class APINodesDeleteManyResponse(BaseModel):
    success   : bool
    results   : list[bool] = Field(default_factory=list)
    n_deleted : int

#==============#
# Edge schemas #
#==============#

# api/edges/list
class APIEdgesListRequest(BaseModel):
    from_type : str = Field(..., description="Source node object type.")
    to_type   : str = Field(..., description="Target node object type.")
    id_pattern       : str | None = Field(
        default     = None,
        description = "Optional wildcard pattern applied to edge identifiers. `*` is supported.",
    )

class APIEdgesListResponse(BaseModel):
    # Use EdgeKeySpec for consistency (from_type/from_id/to_type/to_id/context)
    edges : list[EdgeKeySpec] = Field(default_factory=list)
    count : int = 0

# api/edges/exists
class APIEdgesExistsRequest(BaseModel):
    key: EdgeKeySpec

class APIEdgesExistsResponse(BaseModel):
    exists: bool

# api/edges/exists_many
class APIEdgesExistsManyRequest(BaseModel):
    key_list: list[EdgeKeySpec] = Field(default_factory=list)

class APIEdgesExistsManyResponse(BaseModel):
    exist_keys : list[bool] = Field(default_factory=list)
    count      : int

# api/edges/get
class APIEdgesGetRequest(BaseModel):
    key: EdgeKeySpec

class APIEdgesGetResponse(BaseModel):
    found : bool
    edge  : dict[str, Any] | None = None

# api/edges/get_many
class APIEdgesGetManyRequest(BaseModel):
    key_list: list[EdgeKeySpec] = Field(default_factory=list)

class APIEdgesGetManyResponse(BaseModel):
    found_keys : list[bool] = Field(default_factory=list)
    edges      : list[dict[str, Any] | None] = Field(default_factory=list)
    count      : int

# api/edges/save
class APIEdgesSaveRequest(BaseModel):
    edge: EdgeSpec

class APIEdgesSaveResponse(BaseModel):
    success   : bool
    saved_key : dict[str, str] | None = None

# api/edges/save_many
class APIEdgesSaveManyRequest(BaseModel):
    edge_list: list[EdgeSpec] = Field(default_factory=list)

class APIEdgesSaveManyResponse(BaseModel):
    success    : bool
    saved_keys : list[dict[str, str]] = Field(default_factory=list)
    count      : int

# api/edges/delete
class APIEdgesDeleteRequest(BaseModel):
    key: EdgeKeySpec

class APIEdgesDeleteResponse(BaseModel):
    success: bool

# api/edges/delete_many
class APIEdgesDeleteManyRequest(BaseModel):
    key_list: list[EdgeKeySpec] = Field(default_factory=list)

class APIEdgesDeleteManyResponse(BaseModel):
    success   : bool
    results   : list[bool] = Field(default_factory=list)
    n_deleted : int
