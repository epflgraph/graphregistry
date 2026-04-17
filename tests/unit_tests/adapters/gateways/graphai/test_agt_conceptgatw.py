from __future__ import annotations

from pathlib import Path

import pytest

from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.common.config import REPO_ROOT


def test_detect_concepts_returns_empty_for_blank_input() -> None:
    gateway = GraphAIConceptGateway(
        graph_api_json="config/graph_api.json",
        login_info={"token": "ok"},
        extract_fn=lambda text, login_info: [{"concept_id": "x", "score": 1.0}],
    )

    out = gateway.detect_concepts("   ")

    assert out.concept_list == []


def test_detect_concepts_filters_invalid_items_and_normalizes_score() -> None:
    gateway = GraphAIConceptGateway(
        graph_api_json="config/graph_api.json",
        login_info={"token": "ok"},
        extract_fn=lambda text, login_info: [
            {"id": "concept-a", "confidence": "0.7", "text_source": 123},
            {"concept": "concept-b", "score": "bad"},
            {"foo": "bar"},
            "not-a-dict",
        ],
    )

    out = gateway.detect_concepts("example")

    assert [c.concept_id for c in out.concept_list] == ["concept-a", "concept-b"]
    assert out.concept_list[0].score == 0.7
    assert out.concept_list[0].text_source == "123"
    assert out.concept_list[1].score == 0.0


def test_detect_concepts_uses_login_fn_once_when_not_preseeded() -> None:
    login_calls: list[str] = []

    def login_fn(path: str) -> dict[str, str]:
        login_calls.append(path)
        return {"token": "ok"}

    gateway = GraphAIConceptGateway(
        graph_api_json="config/graph_api.json",
        login_fn=login_fn,
        extract_fn=lambda text, login_info: [{"concept_id": "x", "score": 1.0}],
    )

    out1 = gateway.detect_concepts("one")
    out2 = gateway.detect_concepts("two")

    assert len(out1.concept_list) == 1
    assert len(out2.concept_list) == 1
    assert len(login_calls) == 1
    assert login_calls[0].endswith("config/graph_api.json")


def test_ensure_login_info_raises_for_invalid_login_data() -> None:
    gateway = GraphAIConceptGateway(
        graph_api_json="config/graph_api.json",
        login_fn=lambda _path: {},
        extract_fn=lambda text, login_info: [],
    )

    with pytest.raises(ValueError, match="Failed to obtain valid GraphAI login info"):
        gateway.detect_concepts("text")


def test_resolve_graph_api_json_relative_and_absolute() -> None:
    rel = GraphAIConceptGateway._resolve_graph_api_json("config/graph_api.json")
    abs_path = GraphAIConceptGateway._resolve_graph_api_json("/tmp/graph_api.json")

    assert rel == REPO_ROOT / Path("config/graph_api.json")
    assert abs_path == Path("/tmp/graph_api.json")
