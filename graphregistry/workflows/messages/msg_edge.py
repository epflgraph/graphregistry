from __future__ import annotations

from pydantic import BaseModel

from graphregistry.domain.models.mdl_edge import Edge, EdgeKey


# ------------------ #
# EXISTS
# ------------------ #

class EdgeExistsRequest(BaseModel):
    key: EdgeKey


class EdgeExistsResponse(BaseModel):
    exists: bool


# ------------------ #
# INSERT
# ------------------ #

class EdgeInsertRequest(BaseModel):
    edge: Edge


class EdgeInsertResponse(BaseModel):
    success: bool


# ------------------ #
# UPDATE
# ------------------ #

class EdgeUpdateRequest(BaseModel):
    edge: Edge


class EdgeUpdateResponse(BaseModel):
    success: bool


# ------------------ #
# UPSERT
# ------------------ #

class EdgeUpsertRequest(BaseModel):
    edge: Edge


class EdgeUpsertResponse(BaseModel):
    success: bool
    created: bool  # True = inserted, False = updated


# ------------------ #
# DELETE
# ------------------ #

class EdgeDeleteRequest(BaseModel):
    key: EdgeKey


class EdgeDeleteResponse(BaseModel):
    success: bool
