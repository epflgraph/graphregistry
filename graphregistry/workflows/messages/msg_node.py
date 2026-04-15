# graphregistry/workflows/messages/msg_node.py
from __future__ import annotations
from pydantic import BaseModel
from graphregistry.domain.models.mdl_node import Node, NodeKey

# Class definition
class NodeExistsRequest(BaseModel):
    key: NodeKey

# Class definition
class NodeExistsResponse(BaseModel):
    exists: bool

# Class definition
class NodeInsertRequest(BaseModel):
    node: Node

# Class definition
class NodeInsertResponse(BaseModel):
    success: bool

# Class definition
class NodeUpdateRequest(BaseModel):
    node: Node

# Class definition
class NodeUpdateResponse(BaseModel):
    success: bool

# Class definition
class NodeUpsertRequest(BaseModel):
    node: Node

# Class definition
class NodeUpsertResponse(BaseModel):
    success: bool
    created: bool

# Class definition
class NodeDeleteRequest(BaseModel):
    key: NodeKey

# Class definition
class NodeDeleteResponse(BaseModel):
    success: bool

# Class definition
class NodeSaveRequest(BaseModel):
    node: Node

# Class definition
class NodeSaveResponse(BaseModel):
    success: bool