from __future__ import annotations

from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_node import Node
from graphregistry.workflows.messages.msg_node import (
    NodeDeleteRequest,
    NodeExistsRequest,
    NodeSaveRequest,
    NodeUpsertResponse,
)


def _node() -> Node:
    return Node(key=NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"), title="Course")


def test_node_message_models_accept_expected_payloads() -> None:
    req = NodeExistsRequest(key=("EPFL", "Course", "CS-101"))
    save = NodeSaveRequest(node=_node())
    delete = NodeDeleteRequest(key=req.key)
    upsert = NodeUpsertResponse(success=True, created=False)

    assert req.key.object_id == "CS-101"
    assert save.node.title == "Course"
    assert delete.key.object_type == "Course"
    assert upsert.created is False
