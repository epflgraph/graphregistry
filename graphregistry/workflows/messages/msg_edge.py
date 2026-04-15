# graphregistry/workflows/messages/msg_edge.py
from __future__ import annotations
from pydantic import BaseModel
from graphregistry.domain.models.mdl_edge import Edge, EdgeKey

# Class definition
class EdgeExistsRequest(BaseModel):
    key: EdgeKey

# Class definition
class EdgeExistsResponse(BaseModel):
    exists: bool

# Class definition
class EdgeInsertRequest(BaseModel):
    edge: Edge

# Class definition
class EdgeInsertResponse(BaseModel):
    success: bool

# Class definition
class EdgeUpdateRequest(BaseModel):
    edge: Edge

# Class definition
class EdgeUpdateResponse(BaseModel):
    success: bool

# Class definition
class EdgeUpsertRequest(BaseModel):
    edge: Edge

# Class definition
class EdgeUpsertResponse(BaseModel):
    success: bool
    created: bool

# Class definition
class EdgeDeleteRequest(BaseModel):
    key: EdgeKey

# Class definition
class EdgeDeleteResponse(BaseModel):
    success: bool

# Class definition
class EdgeSaveRequest(BaseModel):
    edge: Edge

# Class definition
class EdgeSaveResponse(BaseModel):
    success: bool