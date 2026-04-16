from __future__ import annotations

from graphregistry.domain.models.mdl_base import EdgeKey
from graphregistry.domain.models.mdl_edge import Edge
from graphregistry.workflows.messages.msg_edge import (
    EdgeExistsRequest,
    EdgeInsertRequest,
    EdgeUpsertResponse,
)


def _edge() -> Edge:
    return Edge(
        key=EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS-101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="123",
            context="teacher",
        )
    )


def test_edge_message_models_accept_expected_payloads() -> None:
    req = EdgeExistsRequest(
        key=("EPFL", "Course", "CS-101", "EPFL", "Person", "123", "teacher")
    )
    insert = EdgeInsertRequest(edge=_edge())
    upsert = EdgeUpsertResponse(success=True, created=True)

    assert req.key.context == "teacher"
    assert insert.edge.key.to_object_type == "Person"
    assert upsert.created is True
