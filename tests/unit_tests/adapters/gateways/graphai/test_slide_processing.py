# tests/unit_tests/adapters/gateways/graphai/test_slide_processing.py
"""Unit tests for GraphAI slide processing orchestration."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.domain.models.entities.mdl_lecture import SlideList


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


def _slide_result(video_token: str) -> dict[str, Any]:
    return {
        "successful": True,
        "slide_tokens": {
            "0": {
                "token": "slide-0",
                "timestamp": 10,
                "token_status": {"active": True},
            },
            "1": {
                "token": "slide-1",
                "timestamp": 30,
                "token_status": {"active": True},
            },
        },
    }


class TestSlideProcessing:
    def test_process_slides_fingerprints_and_ocr(self, gateway: GraphAIVideoGateway) -> None:
        image_gateway = MagicMock()
        image_gateway.calculate_fingerprint.side_effect = ["fp0", "fp1"]
        image_gateway.extract_text_from_slide.side_effect = [
            {"text": "hello", "language": "en"},
            {"text": "world", "language": "en"},
        ]
        gateway._image_gateway = image_gateway

        with patch.object(gateway, "_call_async_endpoint", return_value=_slide_result("vid-1")):
            language, slides = gateway.process_slides(video_token="vid-1")

        assert language == "en"
        assert isinstance(slides, SlideList)
        assert len(slides.item_list) == 2
        assert slides.item_list[0].fingerprint == "fp0"
        assert slides.item_list[0].text == "hello"
        assert slides.item_list[0].language == "en"
        assert slides.item_list[1].text == "world"
        assert image_gateway.calculate_fingerprint.call_count == 2
        assert image_gateway.extract_text_from_slide.call_count == 2

    def test_unsupported_language_falls_back_to_english(self, gateway: GraphAIVideoGateway) -> None:
        image_gateway = MagicMock()
        image_gateway.calculate_fingerprint.side_effect = ["fp0", "fp1"]
        # First OCR detects Chinese; fallback to English is triggered.
        image_gateway.extract_text_from_slide.side_effect = [
            {"text": "你好", "language": "zh"},
            {"text": "世界", "language": "zh"},
            {"text": "hello", "language": "en"},
            {"text": "world", "language": "en"},
        ]
        gateway._image_gateway = image_gateway

        with patch.object(gateway, "_call_async_endpoint", return_value=_slide_result("vid-1")):
            language, slides = gateway.process_slides(video_token="vid-1")

        assert language == "en"
        assert slides.item_list[0].text == "hello"
        assert slides.item_list[0].language == "en"

    def test_translates_slide_text(self, gateway: GraphAIVideoGateway) -> None:
        image_gateway = MagicMock()
        image_gateway.calculate_fingerprint.return_value = "fp"
        image_gateway.extract_text_from_slide.return_value = {"text": "bonjour", "language": "fr"}

        translation_gateway = MagicMock()
        translation_gateway.translate_text_list.return_value = ["hello", "hi"]

        gateway._image_gateway = image_gateway

        with patch.object(gateway, "_call_async_endpoint", return_value=_slide_result("vid-1")):
            language, slides = gateway.process_slides(
                video_token="vid-1",
                destination_languages=("en",),
                translation_gateway=translation_gateway,
            )

        assert language == "fr"
        translation_gateway.translate_text_list.assert_called_once_with(
            ["bonjour", "bonjour"],
            source_language="fr",
            target_language="en",
        )
        assert slides.item_list[0].translations == {"fr": "bonjour", "en": "hello"}
        assert slides.item_list[1].translations == {"fr": "bonjour", "en": "hi"}
