# tests/unit_tests/adapters/test_graphauth_gateway.py
"""Unit tests for the GraphAuth access gateway adapter (HTTP mocked)."""
import pytest
import requests

from graphregistry.adapters.gateways.graphauth.agt_auth import GraphAuthAccessGateway
from graphregistry.domain.models.values.mdl_access import AccessAction


#----------------------------------------------------------#
# Helpers                                                  #
#----------------------------------------------------------#
class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _Recorder:
    """Captures requests.post kwargs and returns a canned response."""

    def __init__(self, response: _FakeResponse | Exception):
        self.response = response
        self.calls: list[dict] = []

    def __call__(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _patch_post(monkeypatch, recorder: _Recorder) -> None:
    monkeypatch.setattr(
        "graphregistry.adapters.gateways.graphauth.agt_auth.requests.post",
        recorder,
    )


#----------------------------------------------------------#
# Tests                                                    #
#----------------------------------------------------------#
def test_allow_decision(monkeypatch):
    recorder = _Recorder(_FakeResponse(200, {"decision": "allow", "principal": "graphsearch"}))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011/")

    decision = gateway.verify("gk-tok", AccessAction.WRITE,
                              node_key=("Course", "CS-1"))

    assert decision.allowed is True
    assert decision.principal == "graphsearch"
    # Request shape: correct URL, bearer header, exactly one of node/edge.
    assert recorder.calls[0]["url"] == "http://graphauth:3011/v1/verify"
    assert recorder.calls[0]["headers"]["Authorization"] == "Bearer gk-tok"
    assert recorder.calls[0]["json"] == {"action": "write", "node": ["Course", "CS-1"]}


def test_deny_decision(monkeypatch):
    recorder = _Recorder(_FakeResponse(200, {"decision": "deny", "principal": "graphsearch"}))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011")

    decision = gateway.verify("gk-tok", AccessAction.READ,
                              edge_key=("Course", "CS-1", "Person", "12345", "teacher"))

    assert decision.allowed is False
    assert recorder.calls[0]["json"] == {
        "action": "read",
        "edge": ["Course", "CS-1", "Person", "12345", "teacher"],
    }


def test_invalid_key_maps_to_deny(monkeypatch):
    recorder = _Recorder(_FakeResponse(401, {"detail": "Invalid or inactive API key"}))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011")

    decision = gateway.verify("gk-revoked", AccessAction.READ, node_key=("Course", "CS-1"))

    assert decision.allowed is False
    assert "invalid" in (decision.reason or "")


def test_unexpected_status_fails_closed(monkeypatch):
    recorder = _Recorder(_FakeResponse(500, {"detail": "boom"}))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011")

    decision = gateway.verify("gk-tok", AccessAction.READ, node_key=("Course", "CS-1"))

    assert decision.allowed is False
    assert "unexpected graphauth status 500" in (decision.reason or "")


def test_unreachable_service_fails_closed(monkeypatch):
    recorder = _Recorder(requests.ConnectionError("connection refused"))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011")

    decision = gateway.verify("gk-tok", AccessAction.WRITE, node_key=("Course", "CS-1"))

    assert decision.allowed is False
    assert "unreachable" in (decision.reason or "")


def test_timeout_is_bounded(monkeypatch):
    recorder = _Recorder(_FakeResponse(200, {"decision": "allow"}))
    _patch_post(monkeypatch, recorder)
    gateway = GraphAuthAccessGateway(url="http://graphauth:3011", timeout=1.5)

    gateway.verify("gk-tok", AccessAction.READ, node_key=("Course", "CS-1"))

    assert recorder.calls[0]["timeout"] == 1.5
