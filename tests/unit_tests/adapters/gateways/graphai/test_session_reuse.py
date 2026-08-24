# tests/unit_tests/adapters/gateways/graphai/test_session_reuse.py
"""Unit tests for GraphAI gateway HTTP session reuse.

The base gateway keeps a persistent ``requests.Session`` so that TLS
handshakes and TCP connections are reused across the submit/status polling
pattern. These tests verify that the session is created lazily, reused, and
used by the low-level request helper.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from requests import Session

from graphregistry.adapters.gateways.graphai.gtw_base import GraphAIBaseGateway
from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway


@pytest.fixture
def gateway() -> GraphAIBaseGateway:
    gtw = GraphAIBaseGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


def test_session_created_lazily(gateway: GraphAIBaseGateway) -> None:
    assert gateway._session is None
    session = gateway._http_session
    assert isinstance(session, Session)
    assert gateway._session is session


def test_session_reused_across_accesses(gateway: GraphAIBaseGateway) -> None:
    first = gateway._http_session
    second = gateway._http_session
    assert first is second


def test_request_uses_session(gateway: GraphAIBaseGateway) -> None:
    fake_response = MagicMock()
    fake_response.ok = True
    fake_response.status_code = 200

    with patch.object(gateway._http_session, "request", return_value=fake_response) as mock_request:
        response = gateway._request(
            url="/foo",
            login_info=gateway._login_info,
            method="POST",
            headers={"Content-Type": "application/json"},
            json={"x": 1},
        )

    assert response is fake_response
    mock_request.assert_called_once_with(
        "POST",
        "http://localhost:28800/foo",
        headers={"Authorization": "Bearer fake-token", "Content-Type": "application/json"},
        json={"x": 1},
        data=None,
        timeout=600,
        stream=False,
    )


def test_close_clears_session(gateway: GraphAIBaseGateway) -> None:
    session = gateway._http_session
    with patch.object(session, "close") as mock_close:
        gateway.close()
    mock_close.assert_called_once()
    assert gateway._session is None


def test_context_manager_closes_session() -> None:
    gateway = GraphAIBaseGateway()
    gateway._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    session = gateway._http_session
    with patch.object(session, "close") as mock_close:
        with gateway:
            pass
    mock_close.assert_called_once()


def test_translation_gateway_inherits_session() -> None:
    gtw = GraphAITextTranslationGateway()
    assert gtw._session is None
    session = gtw._http_session
    assert isinstance(session, Session)
    assert gtw._session is session
