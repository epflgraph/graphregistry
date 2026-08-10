# tests/unit_tests/adapters/gateways/graphai/test_translation_batching.py
"""Unit tests for GraphAI translation list batching and text_too_large handling."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway


@pytest.fixture
def gateway() -> GraphAITextTranslationGateway:
    """A translation gateway with fake login info."""
    gtw = GraphAITextTranslationGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


class TestListBatchingByTotalLength:
    def test_splits_long_list_into_batches(self, gateway: GraphAITextTranslationGateway) -> None:
        """A list whose total length exceeds max_text_list_length is translated in batches."""
        texts = ["a" * 500, "b" * 500, "c" * 500]

        def side_effect(*, payload, **kwargs):
            # Return translations with a prefix so we can see which batch was which.
            return {"successful": True, "result": [f"BATCH:{text}" for text in payload["text"]]}

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.translate_text_list(
                texts,
                source_language="en",
                target_language="fr",
                max_text_list_length=1000,
            )

        assert isinstance(result, list)
        assert len(result) == 3
        # The exact grouping depends on batching logic; verify all items returned.
        assert all(item is not None and item.startswith("BATCH:") for item in result)
        # More than one batch should have been submitted.
        assert mock_call.call_count > 1

    def test_preserves_none_and_empty_items(self, gateway: GraphAITextTranslationGateway) -> None:
        texts = ["hello", None, "", "world"]

        def side_effect(*, payload, **kwargs):
            return {"successful": True, "result": [text.upper() for text in payload["text"]]}

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect):
            result = gateway.translate_text_list(
                texts,
                source_language="en",
                target_language="fr",
            )

        # Empty strings are treated like None by the legacy client and are not sent
        # to GraphAI; they come back as None placeholders.
        assert result == ["HELLO", None, None, "WORLD"]


class TestTextTooLarge:
    def test_reduces_max_text_length_for_string(self, gateway: GraphAITextTranslationGateway) -> None:
        """When a single string is too large, retry with smaller chunks."""
        text = "a" * 5000

        def side_effect(*, payload, **kwargs):
            if isinstance(payload["text"], str) and len(payload["text"]) == 5000:
                return {"successful": False, "text_too_large": True, "result": "text too large"}
            # When split into chunks, succeed.
            texts = payload["text"] if isinstance(payload["text"], list) else [payload["text"]]
            return {"successful": True, "result": [f"chunk-{len(t)}" for t in texts]}

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.translate_text(
                text,
                source_language="en",
                target_language="fr",
                max_text_length=5000,
            )

        assert result is not None
        assert "chunk-" in result
        assert mock_call.call_count >= 2

    def test_reduces_max_text_length_for_list(self, gateway: GraphAITextTranslationGateway) -> None:
        """When a list batch is too large, retry the whole batch with smaller chunks."""
        texts = ["x" * 3000, "short", "y" * 3000]

        def side_effect(*, payload, **kwargs):
            texts = payload["text"] if isinstance(payload["text"], list) else [payload["text"]]
            if any(len(t) == 3000 for t in texts):
                return {"successful": False, "text_too_large": True, "result": "text too large"}
            return {"successful": True, "result": [f"ok-{len(t)}" for t in texts]}

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.translate_text_list(
                texts,
                source_language="en",
                target_language="fr",
                max_text_length=3000,
            )

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[1] == "ok-5"
        assert result[0] is not None and result[0].startswith("ok-")
        assert result[2] is not None and result[2].startswith("ok-")
        assert mock_call.call_count >= 2
