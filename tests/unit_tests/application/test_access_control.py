# tests/unit_tests/application/test_access_control.py
"""Unit tests for AccessControl modes and the authorized operations wrappers."""
from types import SimpleNamespace

import pytest

from graphregistry.application.operations.ops_access import (
    AuthorizedEdgeOperations,
    AuthorizedNodeOperations,
)
from graphregistry.application.policies.pol_access import AccessControl, AccessDeniedError
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.values.mdl_access import AccessAction, AccessDecision, EnforcementMode


#----------------------------------------------------------#
# Test doubles                                             #
#----------------------------------------------------------#
class FakeGateway:
    """Gateway double: allows keys whose type is in allowed_types."""

    def __init__(self, allowed_types: set[str] | None = None, allow_all: bool | None = None):
        self.calls: list[dict] = []
        self.allowed_types = allowed_types or set()
        # Default to allow-all only when no type restriction was given.
        self.allow_all = allow_all if allow_all is not None else (allowed_types is None)

    def verify(self, access_token, action, node_key=None, edge_key=None):
        self.calls.append({"token": access_token, "action": action,
                           "node_key": node_key, "edge_key": edge_key})
        key = node_key if node_key is not None else edge_key
        allowed = self.allow_all or (key is not None and key[0] in self.allowed_types)
        return AccessDecision(allowed=allowed,
                              principal="svc-test",
                              reason=None if allowed else "denied by grants")


class StubNodeOps:
    """Minimal inner operations double (only what the wrapper touches)."""

    def __init__(self):
        self.saved: list = []
        self.deleted: list = []
        self.got_many: list = []

    def list(self, object_type, id_pattern=None):
        return [("Course", "A"), ("Course", "B"), ("Person", "C")]

    def get(self, key):
        return f"node-{key.object_id}"

    def get_many(self, key_list):
        keys = key_list.item_list if hasattr(key_list, "item_list") else list(key_list)
        self.got_many = [k.to_tuple() for k in keys]
        return self.got_many

    def save(self, node, actions=("commit",)):
        self.saved.append(node)
        return node

    def save_many(self, node_list, actions=("commit",)):
        nodes = node_list.item_list if hasattr(node_list, "item_list") else list(node_list)
        self.saved.extend(nodes)
        return nodes

    def delete(self, key, actions=("commit",)):
        self.deleted.append(key)
        return True

    def delete_many(self, key_list, actions=("commit",)):
        keys = key_list.item_list if hasattr(key_list, "item_list") else list(key_list)
        self.deleted.extend(keys)
        return [True] * len(keys)


class StubEdgeOps:
    """Minimal inner edge operations double."""

    def __init__(self):
        self.saved: list = []

    def save(self, edge, actions=("commit",)):
        self.saved.append(edge)
        return edge

    def save_many(self, edge_list, actions=("commit",)):
        edges = edge_list.item_list if hasattr(edge_list, "item_list") else list(edge_list)
        self.saved.extend(edges)
        return edges


NODE_A = NodeKey.from_tuple(("Course", "A"))
NODE_C = NodeKey.from_tuple(("Person", "C"))
EDGE_1 = EdgeKey.from_tuple(("Course", "A", "Person", "1", "teacher"))

TOKEN = "gk-test-token"


#----------------------------------------------------------#
# AccessControl modes                                      #
#----------------------------------------------------------#
def test_off_mode_never_calls_gateway():
    gateway = FakeGateway()
    access = AccessControl(gateway=gateway, mode=EnforcementMode.OFF)
    access.require_node(TOKEN, AccessAction.WRITE, NODE_A)
    assert gateway.calls == []


def test_log_mode_logs_but_never_blocks():
    access = AccessControl(gateway=FakeGateway(allow_all=False), mode=EnforcementMode.LOG)
    # Denying gateway, but log mode must not raise.
    access.require_node(TOKEN, AccessAction.WRITE, NODE_A)
    access.require_node_all(TOKEN, AccessAction.WRITE, [NODE_A, NODE_C])


def test_enforce_mode_raises_on_denied_node():
    access = AccessControl(gateway=FakeGateway(allow_all=False), mode=EnforcementMode.ENFORCE)
    with pytest.raises(AccessDeniedError) as excinfo:
        access.require_node(TOKEN, AccessAction.WRITE, NODE_A)
    assert excinfo.value.denied_keys == [("Course", "A")]
    assert excinfo.value.action == "write"


def test_enforce_mode_batch_reports_all_denied_keys():
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    with pytest.raises(AccessDeniedError) as excinfo:
        access.require_node_all(TOKEN, AccessAction.WRITE, [NODE_A, NODE_C])
    assert excinfo.value.denied_keys == [("Person", "C")]


def test_enforce_mode_without_gateway_fails_closed():
    access = AccessControl(gateway=None, mode=EnforcementMode.ENFORCE)
    with pytest.raises(AccessDeniedError):
        access.require_node(TOKEN, AccessAction.READ, NODE_A)


def test_filter_node_keys_drops_unreadable():
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    visible = access.filter_node_keys(TOKEN, [NODE_A, NODE_C])
    assert [k.to_tuple() for k in visible] == [("Course", "A")]


def test_off_mode_filter_returns_everything():
    access = AccessControl(gateway=None, mode=EnforcementMode.OFF)
    keys = [NODE_A, NODE_C]
    assert access.filter_node_keys(TOKEN, keys) == keys


def test_edge_grants_independent_of_node_grants():
    """An edge-allowing gateway must not be affected by node-type checks."""
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    # Edge from Course to Person: allowed because the EDGE type check passes.
    access.require_edge(TOKEN, AccessAction.WRITE, EDGE_1)
    with pytest.raises(AccessDeniedError):
        access.require_edge(TOKEN, AccessAction.WRITE,
                            EdgeKey.from_tuple(("Publication", "P1", "Person", "1", "author")))


#----------------------------------------------------------#
# Authorized wrappers                                      #
#----------------------------------------------------------#
def test_node_wrapper_enforce_blocks_save():
    inner = StubNodeOps()
    access = AccessControl(gateway=FakeGateway(allow_all=False), mode=EnforcementMode.ENFORCE)
    wrapper = AuthorizedNodeOperations(inner, access, TOKEN)

    node = SimpleNamespace(key=NODE_A)
    with pytest.raises(AccessDeniedError):
        wrapper.save(node)
    assert inner.saved == []


def test_node_wrapper_enforce_allows_authorized_save():
    inner = StubNodeOps()
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    wrapper = AuthorizedNodeOperations(inner, access, TOKEN)

    node = SimpleNamespace(key=NODE_A)
    assert wrapper.save(node) is node
    assert len(inner.saved) == 1


def test_node_wrapper_save_many_all_or_nothing():
    inner = StubNodeOps()
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    wrapper = AuthorizedNodeOperations(inner, access, TOKEN)

    nodes = [SimpleNamespace(key=NODE_A), SimpleNamespace(key=NODE_C)]
    with pytest.raises(AccessDeniedError) as excinfo:
        wrapper.save_many(nodes)
    assert excinfo.value.denied_keys == [("Person", "C")]
    assert inner.saved == []          # nothing written


def test_node_wrapper_list_filters_readable():
    inner = StubNodeOps()
    access = AccessControl(gateway=FakeGateway(allowed_types={"Course"}), mode=EnforcementMode.ENFORCE)
    wrapper = AuthorizedNodeOperations(inner, access, TOKEN)

    assert wrapper.list("Course") == [("Course", "A"), ("Course", "B")]


def test_node_wrapper_off_mode_passes_through():
    inner = StubNodeOps()
    access = AccessControl(gateway=None, mode=EnforcementMode.OFF)
    wrapper = AuthorizedNodeOperations(inner, access, None)

    keys = [NODE_A, NODE_C]
    assert wrapper.get_many(keys) == [NODE_A.to_tuple(), NODE_C.to_tuple()]
    assert inner.got_many == [NODE_A.to_tuple(), NODE_C.to_tuple()]


def test_node_wrapper_passthrough_non_overridden_methods():
    inner = StubNodeOps()
    access = AccessControl(gateway=None, mode=EnforcementMode.OFF)
    wrapper = AuthorizedNodeOperations(inner, access, TOKEN)
    assert wrapper._inner is inner       # __getattr__ delegation works


def test_edge_wrapper_enforce_requires_edge_grant_only():
    inner = StubEdgeOps()
    # Gateway denies edges entirely.
    class DenyEdges(FakeGateway):
        def verify(self, access_token, action, node_key=None, edge_key=None):
            self.calls.append({})
            allowed = edge_key is None          # allow node checks, deny edges
            return AccessDecision(allowed=allowed, principal="svc-test")
    access = AccessControl(gateway=DenyEdges(), mode=EnforcementMode.ENFORCE)
    wrapper = AuthorizedEdgeOperations(inner, access, TOKEN)

    edge = SimpleNamespace(key=EDGE_1)
    with pytest.raises(AccessDeniedError):
        wrapper.save(edge)
    assert inner.saved == []
