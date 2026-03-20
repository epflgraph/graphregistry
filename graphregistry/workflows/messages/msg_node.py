from __future__ import annotations

from pydantic import BaseModel

from graphregistry.domain.models.mdl_node import Node, NodeKey


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
    node: Node


class NodeInsertResponse(BaseModel):
    success: bool


# ------------------ #
# UPDATE
# ------------------ #

class NodeUpdateRequest(BaseModel):
    node: Node


class NodeUpdateResponse(BaseModel):
    success: bool


# ------------------ #
# UPSERT
# ------------------ #

class NodeUpsertRequest(BaseModel):
    node: Node


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
