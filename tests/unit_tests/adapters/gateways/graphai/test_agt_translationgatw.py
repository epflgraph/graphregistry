from __future__ import annotations

from pathlib import Path

import pytest

from graphregistry.adapters.gateways.graphai.agt_translationgatw import GraphAITextTranslationGateway
from graphregistry.common.config import REPO_ROOT
from graphregistry.domain.models.mdl_text import MultilingualText


def test_translate_text_short_circuits_empty_or_same_language() -> None:
    gateway = GraphAITextTranslationGateway(
        graph_api_json="config/graph_api.json",
        login_info={"token": "x"},
        translate_fn=lambda text, source_language, target_language, login_info: "SHOULD_NOT_BE_USED",
    )

    assert gateway.translate_text("", source_language="en", target_language="fr") == ""
    assert gateway.translate_text("unchanged", source_language="en", target_language="en") == "unchanged"


def test_translate_text_uses_login_and_translate_functions_once() -> None:
    login_calls: list[str] = []
    translate_calls: list[tuple[str, str, str, dict[str, str]]] = []

    def login_fn(path: str) -> dict[str, str]:
        login_calls.append(path)
        return {"token": "ok"}

    def translate_fn(text: str, source_language: str, target_language: str, login_info: dict[str, str]) -> str:
        translate_calls.append((text, source_language, target_language, login_info))
        return f"{text}-{target_language}"

    gateway = GraphAITextTranslationGateway(
        graph_api_json="config/graph_api.json",
        login_fn=login_fn,
        translate_fn=translate_fn,
    )

    out1 = gateway.translate_text("hello", source_language="en", target_language="fr")
    out2 = gateway.translate_text("hello", source_language="en", target_language="de")

    assert out1 == "hello-fr"
    assert out2 == "hello-de"
    assert len(login_calls) == 1
    assert login_calls[0].endswith("config/graph_api.json")
    assert translate_calls[0][3] == {"token": "ok"}
    assert translate_calls[1][3] == {"token": "ok"}


def test_translate_multilingual_only_fills_missing_targets() -> None:
    calls: list[tuple[str, str, str, dict[str, str]]] = []

    def translate_fn(text: str, source_language: str, target_language: str, login_info: dict[str, str]) -> str:
        calls.append((text, source_language, target_language, login_info))
        return f"{text}-{target_language}"

    gateway = GraphAITextTranslationGateway(
        graph_api_json="config/graph_api.json",
        login_info={"token": "ok"},
        translate_fn=translate_fn,
    )

    text = MultilingualText(en="source", fr="already")
    out = gateway.translate_multilingual(text, source_language="en", target_languages=("en", "fr", "de"))

    assert out.en == "source"
    assert out.fr == "already"
    assert out.de == "source-de"
    assert len(calls) == 1


def test_resolve_graph_api_json_relative_and_absolute() -> None:
    rel = GraphAITextTranslationGateway._resolve_graph_api_json("config/graph_api.json")
    abs_path = GraphAITextTranslationGateway._resolve_graph_api_json("/tmp/graph_api.json")

    assert rel == REPO_ROOT / Path("config/graph_api.json")
    assert abs_path == Path("/tmp/graph_api.json")


def test_ensure_login_info_raises_for_invalid_login_data() -> None:
    gateway = GraphAITextTranslationGateway(
        graph_api_json="config/graph_api.json",
        login_fn=lambda _path: {},
        translate_fn=lambda text, source_language, target_language, login_info: text,
    )

    with pytest.raises(ValueError, match="Failed to obtain valid GraphAI login info"):
        gateway.translate_text("hello", source_language="en", target_language="fr")
