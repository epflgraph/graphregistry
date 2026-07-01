# tests/unit_tests/adapters/gateways/graphai/test_token_status_recovery.py
"""Unit tests for GraphAI media token-status recovery.

These tests verify that the video/audio/slide adapters detect missing or
inactive cached files and retry with force/recalculate flags, matching the
legacy graphai_client behavior.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


@pytest.fixture
def gateway() -> GraphAIVideoGateway:
    """A video gateway with a fake login_info and voice gateway."""
    voice_gtw = MagicMock()
    voice_gtw.fingerprint = MagicMock(return_value="audio-fp-1")
    gtw = GraphAIVideoGateway(voice_gateway=voice_gtw)
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


_MISSING: Any = object()


def _make_task_result(
    *,
    token: str = "video-token-1",
    active: bool = True,
    fingerprinted: bool | None = None,
    fresh: bool = True,
    token_status: dict[str, Any] | None | Any = _MISSING,
    streams: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a task_result dict as GraphAI would return for /video/retrieve_url."""
    if fingerprinted is None:
        fingerprinted = active
    if token_status is _MISSING:
        token_status = {
            "active": active,
            "fingerprinted": fingerprinted,
            "streams": streams or [{"codec_name": "h264", "duration": 120.0}],
        }
    result: dict[str, Any] = {
        "successful": True,
        "token": token,
        "fresh": fresh,
    }
    if token_status is not None:
        result["token_status"] = token_status
    return result


class TestVideoDownloadRecovery:
    def test_returns_video_when_token_active(self, gateway: GraphAIVideoGateway) -> None:
        task_result = _make_task_result(active=True)

        with patch.object(
            gateway, "_call_async_endpoint", return_value=task_result
        ) as mock_call, patch.object(
            gateway, "fingerprint", return_value="fp-1"
        ) as mock_fp:
            result = gateway.get_video("http://example.com/video.mp4")

        assert result is not None
        assert result.token == "video-token-1"
        assert result.fingerprint == "fp-1"
        mock_call.assert_called_once()
        mock_fp.assert_called_once()

    def test_retries_with_force_when_token_inactive_and_cached(self, gateway: GraphAIVideoGateway) -> None:
        """Inactive token on a non-fresh result should force re-download."""
        inactive_result = _make_task_result(active=False, fresh=False)
        active_result = _make_task_result(active=True, fresh=True)

        with patch.object(
            gateway, "_call_async_endpoint", side_effect=[inactive_result, active_result]
        ) as mock_call, patch.object(gateway, "fingerprint", return_value="fp-2"):
            result = gateway.get_video("http://example.com/video.mp4")

        assert result is not None
        assert result.token == "video-token-1"
        assert mock_call.call_count == 2
        # Second call should have force=True
        assert mock_call.call_args_list[1].kwargs["payload"]["force"] is True

    def test_raises_when_token_inactive_and_fresh(self, gateway: GraphAIVideoGateway) -> None:
        """If the task was fresh but the file is missing, something is wrong."""
        bad_result = _make_task_result(active=False, fresh=True)

        with patch.object(gateway, "_call_async_endpoint", return_value=bad_result):
            with pytest.raises(RuntimeError, match="Missing downloaded file"):
                gateway.get_video("http://example.com/video.mp4")

    def test_retries_with_force_when_token_status_missing(self, gateway: GraphAIVideoGateway) -> None:
        result_no_status = _make_task_result(token_status=None, fresh=False)
        result_active = _make_task_result(active=True, fresh=True)

        with patch.object(
            gateway, "_call_async_endpoint", side_effect=[result_no_status, result_active]
        ) as mock_call, patch.object(gateway, "fingerprint", return_value="fp-3"):
            result = gateway.get_video("http://example.com/video.mp4")

        assert result is not None
        # Calls: retrieve_url, retrieve_url retry (fingerprint is mocked)
        assert mock_call.call_count == 2
        assert mock_call.call_args_list[1].kwargs["payload"]["force"] is True


class TestAudioExtractionRecovery:
    def test_returns_voice_when_token_active(self, gateway: GraphAIVideoGateway) -> None:
        task_result = {
            "successful": True,
            "token": "audio-token-1",
            "duration": 120.0,
            "token_status": {"active": True, "fingerprinted": True},
        }

        with patch.object(
            gateway, "_call_async_endpoint", return_value=task_result
        ) as mock_call, patch.object(
            gateway, "fingerprint", return_value="audio-fp-1"
        ):
            result = gateway.extract_audio(video_token="video-token-1")

        assert result is not None
        assert result.token == "audio-token-1"
        mock_call.assert_called_once()

    def test_retries_with_recalculate_cached_when_audio_inactive_and_cached(
        self, gateway: GraphAIVideoGateway
    ) -> None:
        inactive_result = {
            "successful": True,
            "token": "audio-token-1",
            "duration": 120.0,
            "token_status": {"active": False, "fingerprinted": False},
            "fresh": False,
        }
        active_result = {
            "successful": True,
            "token": "audio-token-1",
            "duration": 120.0,
            "token_status": {"active": True, "fingerprinted": True},
            "fresh": True,
        }

        with patch.object(
            gateway, "_call_async_endpoint", side_effect=[inactive_result, active_result]
        ) as mock_call:
            result = gateway.extract_audio(video_token="video-token-1")

        assert result is not None
        assert mock_call.call_count == 2
        assert mock_call.call_args_list[1].kwargs["payload"]["recalculate_cached"] is True
        assert mock_call.call_args_list[1].kwargs["payload"]["force"] is False


class TestSlideDetectionRecovery:
    def test_returns_slides_when_all_active(self, gateway: GraphAIVideoGateway) -> None:
        task_result = {
            "successful": True,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": True}},
                "1": {"token": "slide-1", "timestamp": 10, "token_status": {"active": True}},
            },
        }

        with patch.object(gateway, "_call_async_endpoint", return_value=task_result):
            result = gateway.extract_slides(video_token="video-token-1")

        assert result is not None
        assert len(result.item_list) == 2
        assert result.item_list[0].token == "slide-0"

    def test_returns_slides_when_fingerprinted(self, gateway: GraphAIVideoGateway) -> None:
        task_result = {
            "successful": True,
            "slide_tokens": {
                "0": {
                    "token": "slide-0",
                    "timestamp": 0,
                    "token_status": {"active": False, "fingerprinted": True},
                },
            },
        }

        with patch.object(gateway, "_call_async_endpoint", return_value=task_result):
            result = gateway.extract_slides(video_token="video-token-1")

        assert result is not None
        assert len(result.item_list) == 1

    def test_retries_with_force_when_slides_missing_and_fresh(self, gateway: GraphAIVideoGateway) -> None:
        fresh_missing_result = {
            "successful": True,
            "fresh": True,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": False}},
            },
        }
        active_result = {
            "successful": True,
            "fresh": True,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": True}},
            },
        }

        with patch.object(
            gateway, "_call_async_endpoint", side_effect=[fresh_missing_result, active_result]
        ) as mock_call:
            result = gateway.extract_slides(video_token="video-token-1")

        assert result is not None
        assert mock_call.call_count == 2
        assert mock_call.call_args_list[1].kwargs["payload"]["force"] is True
        assert mock_call.call_args_list[1].kwargs["payload"]["recalculate_cached"] is False

    def test_retries_with_recalculate_cached_when_some_slides_inactive(self, gateway: GraphAIVideoGateway) -> None:
        inactive_result = {
            "successful": True,
            "fresh": False,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": False}},
            },
        }
        active_result = {
            "successful": True,
            "fresh": True,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": True}},
            },
        }

        with patch.object(
            gateway, "_call_async_endpoint", side_effect=[inactive_result, active_result]
        ) as mock_call:
            result = gateway.extract_slides(video_token="video-token-1")

        assert result is not None
        assert mock_call.call_count == 2
        assert mock_call.call_args_list[1].kwargs["payload"]["recalculate_cached"] is True
        assert mock_call.call_args_list[1].kwargs["payload"]["force"] is False

    def test_raises_when_slides_missing_while_forced(self, gateway: GraphAIVideoGateway) -> None:
        bad_result = {
            "successful": True,
            "fresh": False,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": False}},
            },
        }

        with patch.object(gateway, "_call_async_endpoint", return_value=bad_result):
            with pytest.raises(RuntimeError, match="slide files missing"):
                gateway.extract_slides(video_token="video-token-1", force=True)

    def test_raises_when_slides_missing_while_recalculate_cached(self, gateway: GraphAIVideoGateway) -> None:
        bad_result = {
            "successful": True,
            "fresh": False,
            "slide_tokens": {
                "0": {"token": "slide-0", "timestamp": 0, "token_status": {"active": False}},
            },
        }

        with patch.object(gateway, "_call_async_endpoint", return_value=bad_result):
            with pytest.raises(RuntimeError, match="slide files missing"):
                gateway.extract_slides(video_token="video-token-1", recalculate_cached=True)
