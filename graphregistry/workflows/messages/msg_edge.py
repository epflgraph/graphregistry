from __future__ import annotations

from pydantic import BaseModel

from graphregistry.domain.models.edge import EdgeKey


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
    key: EdgeKey


class EdgeInsertResponse(BaseModel):
    success: bool


# ------------------ #
# UPDATE
# ------------------ #

class EdgeUpdateRequest(BaseModel):
    key: EdgeKey


class EdgeUpdateResponse(BaseModel):
    success: bool


# ------------------ #
# UPSERT
# ------------------ #

class EdgeUpsertRequest(BaseModel):
    key: EdgeKey


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
