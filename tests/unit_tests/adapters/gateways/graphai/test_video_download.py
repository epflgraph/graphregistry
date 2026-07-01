# tests/unit_tests/adapters/gateways/graphai/test_video_download.py
"""Unit tests for GraphAI video file download.

These tests verify that ``GraphAIVideoGateway.download_file`` streams the
response body to disk, handles missing files cleanly, and uses a fresh
``requests.Session`` per download to avoid persistent connection state.
"""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


@pytest.fixture
def gateway() -> GraphAIVideoGateway:
    gtw = GraphAIVideoGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


class _FakeResponse:
    """Minimal stand-in for a streamed ``requests.Response``."""

    def __init__(self, body: bytes, status_code: int = 200) -> None:
        self.body = body
        self.status_code = status_code
        self.ok = status_code < 400
        self.closed = False

    def iter_content(self, chunk_size: int = 1024) -> list[bytes]:
        return [self.body[i : i + chunk_size] for i in range(0, len(self.body), chunk_size)]

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        self.closed = True


def _mock_session_with_response(response: _FakeResponse | MagicMock) -> MagicMock:
    """Return a MagicMock session whose .request() returns the given response."""
    session = MagicMock()
    session.request.return_value = response
    return session


def test_download_file_uses_fresh_session(gateway: GraphAIVideoGateway) -> None:
    video_bytes = b"video data"
    fake_response = _FakeResponse(video_bytes)
    mock_session = _mock_session_with_response(fake_response)

    with (
        patch("graphregistry.adapters.gateways.graphai.agt_video.Session", return_value=mock_session),
        TemporaryDirectory() as tmp_dir,
    ):
        output_path = Path(tmp_dir) / "downloaded.mp4"
        result = gateway.download_file(token="abc123.mp4", file_path=output_path)

        assert result == output_path
        assert output_path.read_bytes() == video_bytes
        mock_session.request.assert_called_once()
        mock_session.close.assert_called_once()


def test_download_file_streams_to_disk(gateway: GraphAIVideoGateway) -> None:
    video_bytes = b"\x00\x01\x02" * 1000  # 3 KB of dummy video data
    fake_response = _FakeResponse(video_bytes)
    mock_session = _mock_session_with_response(fake_response)

    with (
        patch("graphregistry.adapters.gateways.graphai.agt_video.Session", return_value=mock_session),
        TemporaryDirectory() as tmp_dir,
    ):
        output_path = Path(tmp_dir) / "downloaded.mp4"
        gateway.download_file(token="abc123.mp4", file_path=output_path)

        _call_kwargs = mock_session.request.call_args.kwargs
        assert _call_kwargs.get("stream") is True
        assert _call_kwargs.get("json") == {"token": "abc123.mp4"}
        assert _call_kwargs.get("timeout") == (5.0, 30.0)
        assert _call_kwargs.get("headers") == {
            "Authorization": "Bearer fake-token",
            "Content-Type": "application/json",
            "Connection": "close",
        }
        assert mock_session.request.call_args.args == (
            "POST",
            "http://localhost:28800/video/get_file",
        )


def test_download_file_returns_none_on_404(gateway: GraphAIVideoGateway) -> None:
    fake_response = MagicMock()
    fake_response.status_code = 404
    fake_response.ok = False
    mock_session = _mock_session_with_response(fake_response)

    with (
        patch("graphregistry.adapters.gateways.graphai.agt_video.Session", return_value=mock_session),
        TemporaryDirectory() as tmp_dir,
    ):
        output_path = Path(tmp_dir) / "missing.mp4"
        result = gateway.download_file(token="missing.mp4", file_path=output_path)

        assert result is None
        assert not output_path.exists()
        mock_session.request.assert_called_once()
        mock_session.close.assert_called_once()


def test_download_file_creates_parent_directories(gateway: GraphAIVideoGateway) -> None:
    video_bytes = b"video data"
    fake_response = _FakeResponse(video_bytes)
    mock_session = _mock_session_with_response(fake_response)

    with (
        patch("graphregistry.adapters.gateways.graphai.agt_video.Session", return_value=mock_session),
        TemporaryDirectory() as tmp_dir,
    ):
        output_path = Path(tmp_dir) / "nested" / "dir" / "file.mp4"
        result = gateway.download_file(token="abc.mp4", file_path=output_path)

        assert result == output_path
        assert output_path.read_bytes() == video_bytes


def test_download_file_closes_session_on_error(gateway: GraphAIVideoGateway) -> None:
    mock_session = _mock_session_with_response(MagicMock())
    mock_session.request.side_effect = RuntimeError("network down")

    with (
        patch("graphregistry.adapters.gateways.graphai.agt_video.Session", return_value=mock_session),
        TemporaryDirectory() as tmp_dir,
    ):
        output_path = Path(tmp_dir) / "fail.mp4"
        with pytest.raises(RuntimeError, match="network down"):
            gateway.download_file(token="abc.mp4", file_path=output_path, max_tries=1)

    mock_session.close.assert_called_once()
