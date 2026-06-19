# tests/unit_tests/adapters/gateways/graphai/test_embedding_batching.py
"""Unit tests for GraphAI embedding list batching and recombination."""
from __future__ import annotations

from math import isclose, sqrt
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.agt_embedding import GraphAIEmbeddingGateway


@pytest.fixture
def gateway() -> GraphAIEmbeddingGateway:
    """An embedding gateway with fake login info."""
    gtw = GraphAIEmbeddingGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


def _l2_norm(vector: list[float]) -> float:
    return sqrt(sum(v * v for v in vector))


class TestEmbedTextListBatching:
    def test_batches_long_lists_by_total_length(self, gateway: GraphAIEmbeddingGateway) -> None:
        """A list whose total character count exceeds max_text_list_length is batched."""
        texts = ["a" * 300, "b" * 300, "c" * 300]

        def side_effect(*, payload, **kwargs):
            return {
                "successful": True,
                "result": [[1.0, 0.0] for _ in payload["text"]],
            }

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.embed_text_list(
                texts,
                max_text_list_length=500,
            )

        assert len(result) == 3
        assert all(isinstance(r, list) for r in result)
        # With total length 900 and limit 500, we expect more than one batch.
        assert mock_call.call_count > 1

    def test_preserves_none_and_empty_items(self, gateway: GraphAIEmbeddingGateway) -> None:
        texts = ["hello", None, "", "world"]

        def side_effect(*, payload, **kwargs):
            return {
                "successful": True,
                "result": [[1.0, 0.0] for _ in payload["text"]],
            }

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect):
            result = gateway.embed_text_list(texts)

        assert result[0] is not None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is not None

    def test_splits_long_items_and_recombines(self, gateway: GraphAIEmbeddingGateway) -> None:
        """A single item longer than max_text_length is split and recombined."""
        text = "word " * 200  # ~1000 chars

        def side_effect(*, payload, **kwargs):
            # Return embeddings whose first component equals chunk length.
            return {
                "successful": True,
                "result": [[float(len(t)), 0.0] for t in payload["text"]],
            }

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.embed_text_list(
                [text],
                max_text_length=300,
            )

        assert isinstance(result[0], list)
        # All returned embeddings point along the positive x-axis and are then
        # L2-normalized, so the combined vector is [1.0, 0.0].
        assert isclose(result[0][0], 1.0, rel_tol=1e-6)
        assert isclose(result[0][1], 0.0, rel_tol=1e-6)
        # The endpoint was called with at least one batch.
        assert mock_call.call_count >= 1


class TestEmbeddingRecombination:
    def test_recombined_embeddings_are_l2_normalized(self, gateway: GraphAIEmbeddingGateway) -> None:
        """Weighted recombination of split embeddings should be L2-normalized."""
        chunks = [[3.0, 4.0], [0.0, 1.0]]
        weights = [5, 5]

        result = gateway._weighted_average_embeddings(chunks, weights)

        norm = _l2_norm(result)
        assert isclose(norm, 1.0, rel_tol=1e-6)

    def test_recombine_embeddings_respects_none(self, gateway: GraphAIEmbeddingGateway) -> None:
        embeddings = [[1.0, 0.0], None, [0.0, 1.0]]
        mapping = {0: 0, 1: 0, 2: 1}

        result = gateway._recombine_embeddings(
            embeddings,
            mapping,
            output_length=2,
            weights=[1, 1, 1],
        )

        assert len(result) == 2
        assert result[0] is not None
        assert result[1] is not None
        assert isclose(_l2_norm(result[0]), 1.0, rel_tol=1e-6)
        assert isclose(_l2_norm(result[1]), 1.0, rel_tol=1e-6)


class TestEmbedTextListTextTooLarge:
    def test_reduces_chunk_size_on_text_too_large(self, gateway: GraphAIEmbeddingGateway) -> None:
        texts = ["a" * 1000]

        def side_effect(*, payload, **kwargs):
            texts = payload["text"] if isinstance(payload["text"], list) else [payload["text"]]
            if any(len(t) == 1000 for t in texts):
                return {"successful": False, "text_too_large": True, "result": "too large"}
            return {"successful": True, "result": [[1.0, 0.0] for _ in texts]}

        with patch.object(gateway, "_call_async_endpoint", side_effect=side_effect) as mock_call:
            result = gateway.embed_text_list(
                texts,
                max_text_length=1000,
            )

        assert isinstance(result[0], list)
        assert mock_call.call_count >= 2
