# tests/unit_tests/adapters/gateways/graphai/test_transcription_fallback.py
"""Unit tests for GraphAI voice transcription fallback and subtitle translation."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway


@pytest.fixture
def gateway() -> GraphAIVoiceGateway:
    gtw = GraphAIVoiceGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


def _transcription_result(language: str, segments: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "successful": True,
        "language": language,
        "transcript_results": " ".join(seg["text"] for seg in segments),
        "subtitle_results": segments,
    }


class TestTranscriptionFallback:
    def test_detects_language_then_transcribes(self, gateway: GraphAIVoiceGateway) -> None:
        """When no force_lang is given, language detection is used first."""
        responses = {
            "/voice/detect_language": {"successful": True, "language": "fr"},
            "/voice/transcribe": _transcription_result("fr", [{"start": 0, "end": 1, "text": "bonjour"}]),
        }

        def side_effect(*, endpoint, payload, **kwargs):
            return responses[endpoint]

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.transcribe_audio(audio_token="audio-123")

        assert result is not None
        assert result.language == "fr"
        assert result.full_text == "bonjour"
        assert len(result.item_list) == 1
        assert result.item_list[0].text == "bonjour"
        detect_call = [c for c in mock_call.call_args_list if c.kwargs["endpoint"] == "/voice/detect_language"]
        assert len(detect_call) == 1

    def test_unsupported_language_falls_back_to_english(self, gateway: GraphAIVoiceGateway) -> None:
        """A detected language outside en/fr/de/it is discarded and English is forced."""
        responses = {
            "/voice/detect_language": {"successful": True, "language": "zh"},
            "/voice/transcribe": _transcription_result("en", [{"start": 0, "end": 1, "text": "hello"}]),
        }
        call_count = {"value": 0}

        def side_effect(*, endpoint, payload, **kwargs):
            call_count["value"] += 1
            # First transcription attempt uses zh, returns nothing.
            if endpoint == "/voice/transcribe" and payload.get("force_lang") == "zh":
                return {"successful": True, "language": "zh", "transcript_results": "", "subtitle_results": []}
            return responses[endpoint]

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect):
            result = gateway.transcribe_audio(audio_token="audio-123")

        assert result is not None
        assert result.language == "en"
        assert result.item_list[0].text == "hello"

    def test_empty_segments_retry_english_then_french(self, gateway: GraphAIVoiceGateway) -> None:
        """If transcription returns no segments, the gateway tries English, then French."""
        call_count = {"transcribe": 0}

        def side_effect(*, endpoint, payload, **kwargs):
            if endpoint == "/voice/detect_language":
                return {"successful": True, "language": "de"}
            call_count["transcribe"] += 1
            forced = payload.get("force_lang")
            if forced == "de":
                return {"successful": True, "language": "de", "transcript_results": "", "subtitle_results": []}
            if forced == "en":
                return {"successful": True, "language": "en", "transcript_results": "", "subtitle_results": []}
            # forced == "fr"
            return _transcription_result("fr", [{"start": 0, "end": 1, "text": "bonjour"}])

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect):
            result = gateway.transcribe_audio(audio_token="audio-123")

        assert result is not None
        assert result.language == "fr"
        assert call_count["transcribe"] == 3

    def test_force_lang_skips_detection(self, gateway: GraphAIVoiceGateway) -> None:
        def side_effect(*, endpoint, payload, **kwargs):
            assert endpoint == "/voice/transcribe"
            assert payload.get("force_lang") == "en"
            return _transcription_result("en", [{"start": 0, "end": 1, "text": "hello"}])

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.transcribe_audio(audio_token="audio-123", force_lang="en")

        assert result is not None
        assert result.language == "en"
        assert all(c.kwargs["endpoint"] != "/voice/detect_language" for c in mock_call.call_args_list)


class TestSubtitleTranslation:
    def test_translates_segments_when_gateway_provided(self, gateway: GraphAIVoiceGateway) -> None:
        """If a translation gateway is supplied, segments are translated to destination languages."""
        translation_gateway = MagicMock()
        translation_gateway.translate_text_list.side_effect = [
            ["hello", "hi"],
            ["hallo", "grüezi"],
        ]

        def side_effect(*, endpoint, payload, **kwargs):
            if endpoint == "/voice/detect_language":
                return {"successful": True, "language": "fr"}
            return _transcription_result(
                "fr",
                [
                    {"start": 0, "end": 1, "text": "bonjour"},
                    {"start": 1, "end": 2, "text": "salut"},
                ],
            )

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect):
            result = gateway.transcribe_audio(
                audio_token="audio-123",
                destination_languages=("en", "de"),
                translation_gateway=translation_gateway,
            )

        assert result is not None
        assert translation_gateway.translate_text_list.call_count == 2
        calls = translation_gateway.translate_text_list.call_args_list
        assert calls[0].kwargs["target_language"] == "en"
        assert calls[1].kwargs["target_language"] == "de"
        assert result.item_list[0].translations == {"fr": "bonjour", "en": "hello", "de": "hallo"}
        assert result.item_list[1].translations == {"fr": "salut", "en": "hi", "de": "grüezi"}
