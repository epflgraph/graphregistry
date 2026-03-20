from __future__ import annotations

from pydantic import BaseModel

from graphregistry.domain.models.mdl_node import NodeKey


# ------------------ #
# EXISTS
# ------------------ #

class NodeExistsRequest(BaseModel):
    key: NodeKey


class NodeExistsResponse(BaseModel):
    exists: bool


# ------------------ #
# INSERT
# ------------------ #

class NodeInsertRequest(BaseModel):
    key: NodeKey


class NodeInsertResponse(BaseModel):
    success: bool


# ------------------ #
# UPDATE
# ------------------ #

class NodeUpdateRequest(BaseModel):
    key: NodeKey


class NodeUpdateResponse(BaseModel):
    success: bool


# ------------------ #
# UPSERT
# ------------------ #

class NodeUpsertRequest(BaseModel):
    key: NodeKey


class NodeUpsertResponse(BaseModel):
    success: bool
    created: bool  # True = inserted, False = updated


# ------------------ #
# DELETE
# ------------------ #

class NodeDeleteRequest(BaseModel):
    key: NodeKey


class NodeDeleteResponse(BaseModel):
    success: bool
