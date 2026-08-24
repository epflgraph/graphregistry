# tests/unit_tests/adapters/gateways/graphai/test_concept_detection.py
"""Unit tests for GraphAI concept detection adapter."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway


@pytest.fixture
def gateway() -> GraphAIConceptDetectionGateway:
    """A concept detection gateway with fake login info."""
    gtw = GraphAIConceptDetectionGateway()
    gtw._login_info = {
        "host": "http://localhost:28800",
        "token": "fake-token",
        "graph_api_json": "/fake/config.json",
        "user": "test",
    }
    return gtw


def _make_response(*items: dict[str, Any]) -> list[dict[str, Any]]:
    return list(items)


class TestDetectConceptsParameters:
    def test_text_path_sends_all_params(self, gateway: GraphAIConceptDetectionGateway) -> None:
        """When given a string, the adapter should send all concept-detection parameters."""
        response = MagicMock()
        response.json.return_value = _make_response(
            {"concept_id": "1", "concept_name": "Machine learning", "mixed_score": 0.95}
        )

        with patch.object(gateway, "_request", return_value=response) as mock_request:
            result = gateway.detect_concepts("Machine learning is great")

        assert len(result.item_list) == 1
        assert result.item_list[0].concept.name == "Machine learning"

        call = mock_request.call_args
        url = call.kwargs["url"]
        payload = call.kwargs["json"]

        assert "/text/wikify?" in url
        assert "restrict_to_ontology=false" in url
        assert "graph_score_smoothing=true" in url
        assert "ontology_score_smoothing=true" in url
        assert "keywords_score_smoothing=true" in url
        assert "normalisation_coef=0.5" in url
        assert "aggregation_coef=0.5" in url
        assert "filtering_threshold=0.15" in url
        assert "filtering_min_votes=5" in url
        assert "refresh_scores=true" in url
        assert payload == {"raw_text": "Machine learning is great"}

    def test_keywords_path_sends_same_params(self, gateway: GraphAIConceptDetectionGateway) -> None:
        """When given a list of keywords, the adapter should send the same parameters as for text."""
        response = MagicMock()
        response.json.return_value = _make_response(
            {"concept_id": "2", "concept_name": "Deep learning", "score": 0.88}
        )

        with patch.object(gateway, "_request", return_value=response) as mock_request:
            result = gateway.detect_concepts(["machine learning", "neural networks"])

        assert len(result.item_list) == 1

        call = mock_request.call_args
        url = call.kwargs["url"]
        payload = call.kwargs["json"]

        assert "/text/wikify?" in url
        assert "restrict_to_ontology=false" in url
        assert "graph_score_smoothing=true" in url
        assert "filtering_threshold=0.15" in url
        assert payload == {"keywords": ["machine learning", "neural networks"]}

    def test_custom_task_params_are_propagated(self, gateway: GraphAIConceptDetectionGateway) -> None:
        """Custom task parameters should appear in the URL query string."""
        from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionTask

        response = MagicMock()
        response.json.return_value = _make_response()

        task = ConceptDetectionTask(
            text="test",
            restrict_to_ontology=True,
            filtering_threshold=0.5,
            filtering_min_votes=10,
        )

        with patch.object(gateway, "_request", return_value=response) as mock_request:
            gateway.detect_concepts_with_task(task)

        url = mock_request.call_args.kwargs["url"]
        assert "restrict_to_ontology=true" in url
        assert "filtering_threshold=0.5" in url
        assert "filtering_min_votes=10" in url


class TestExtractKeywords:
    def test_extract_keywords_default(self, gateway: GraphAIConceptDetectionGateway) -> None:
        response = MagicMock()
        response.json.return_value = ["machine learning", "neural networks"]

        with patch.object(gateway, "_request", return_value=response) as mock_request:
            result = gateway.extract_keywords("Machine learning and neural networks")

        assert result == ["machine learning", "neural networks"]
        call = mock_request.call_args
        assert "/text/keywords?use_nltk=false" in call.kwargs["url"]
        assert call.kwargs["json"] == {"raw_text": "Machine learning and neural networks"}

    def test_extract_keywords_with_nltk(self, gateway: GraphAIConceptDetectionGateway) -> None:
        response = MagicMock()
        response.json.return_value = ["ai"]

        with patch.object(gateway, "_request", return_value=response) as mock_request:
            gateway.extract_keywords("AI", use_nltk=True)

        assert "/text/keywords?use_nltk=true" in mock_request.call_args.kwargs["url"]

    def test_extract_keywords_rejects_non_list_response(
        self, gateway: GraphAIConceptDetectionGateway
    ) -> None:
        response = MagicMock()
        response.json.return_value = {"unexpected": "dict"}

        with patch.object(gateway, "_request", return_value=response):
            with pytest.raises(ValueError, match="Unexpected /text/keywords response shape"):
                gateway.extract_keywords("text")


class TestDetectConceptsResponseMapping:
    def test_maps_mixed_score(self, gateway: GraphAIConceptDetectionGateway) -> None:
        response = MagicMock()
        response.json.return_value = _make_response(
            {"concept_id": "3", "concept_name": "AI", "mixed_score": 0.75}
        )

        with patch.object(gateway, "_request", return_value=response):
            result = gateway.detect_concepts("AI")

        assert result.item_list[0].score == pytest.approx(0.75)

    def test_falls_back_to_score(self, gateway: GraphAIConceptDetectionGateway) -> None:
        response = MagicMock()
        response.json.return_value = _make_response(
            {"concept_id": "4", "concept_name": "Robotics", "score": 0.66}
        )

        with patch.object(gateway, "_request", return_value=response):
            result = gateway.detect_concepts("Robotics")

        assert result.item_list[0].score == pytest.approx(0.66)

    def test_defaults_to_zero_when_no_score(self, gateway: GraphAIConceptDetectionGateway) -> None:
        response = MagicMock()
        response.json.return_value = _make_response(
            {"concept_id": "5", "concept_name": "Ethics"}
        )

        with patch.object(gateway, "_request", return_value=response):
            result = gateway.detect_concepts("Ethics")

        assert result.item_list[0].score == pytest.approx(0.0)
