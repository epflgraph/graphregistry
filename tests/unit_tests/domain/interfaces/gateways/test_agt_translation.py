# tests/unit_tests/domain/interfaces/gateways/test_agt_translation.py
import pytest

from graphregistry.adapters.gateways.graphai.agt_translation import (
    GraphAITextTranslationGateway,
)
from graphregistry.domain.models.mdl_text import MultilingualText


def test_translate_multilingual_routes_via_english_for_missing_pairs(monkeypatch):
    """
    Available direct pairs are:
        fr<->en, de<->en, it<->en

    So:
        fr -> en  : direct
        fr -> de  : fr -> en -> de
        fr -> it  : fr -> en -> it
    """

    gateway = GraphAITextTranslationGateway(login_info={"token": "fake", "host": "http://fake"})

    calls: list[tuple[str, str, str | list[str]]] = []

    def fake_call_async_endpoint(self, endpoint, payload, login_info, **kwargs):
        assert endpoint == "/translation/translate"

        source = payload["source"]
        target = payload["target"]
        text = payload["text"]

        calls.append((source, target, text))

        translations = {
            ("fr", "en"): "Hello",
            ("en", "de"): "Hallo",
            ("en", "it"): "Ciao",
        }

        result = translations.get((source, target))
        if result is None:
            raise AssertionError(f"Unexpected translation pair requested: {(source, target)}")

        return {
            "result": result,
            "text_too_large": False,
            "successful": True,
            "fresh": True,
            "device": "cpu",
        }

    monkeypatch.setattr(
        GraphAITextTranslationGateway,
        "_call_async_endpoint",
        fake_call_async_endpoint,
    )

    text = MultilingualText(fr="Bonjour")

    out = gateway.translate_multilingual(
        text=text,
        source_language="fr",
        target_languages=("en", "fr", "de", "it"),
    )

    assert out.fr == "Bonjour"
    assert out.en == "Hello"
    assert out.de == "Hallo"
    assert out.it == "Ciao"

    # Expected routing:
    #   fr->en
    #   fr->en, en->de
    #   fr->en, en->it
    assert calls == [
        ("fr", "en", "Bonjour"),
        ("fr", "en", "Bonjour"),
        ("en", "de", "Hello"),
        ("fr", "en", "Bonjour"),
        ("en", "it", "Hello"),
    ]


def test_translate_text_chunks_long_text_after_text_too_large(monkeypatch):
    """
    First request simulates GraphAI saying the text is too large.
    The gateway should then split the text into chunks and retry as a list.
    """

    gateway = GraphAITextTranslationGateway(login_info={"token": "fake", "host": "http://fake"})

    payloads: list[dict] = []

    def fake_call_async_endpoint(self, endpoint, payload, login_info, **kwargs):
        assert endpoint == "/translation/translate"
        payloads.append(payload)

        text = payload["text"]

        # First call: single long string -> pretend GraphAI rejects it as too large
        if isinstance(text, str):
            return {
                "result": "text too large",
                "text_too_large": True,
                "successful": False,
                "fresh": False,
                "device": "cpu",
            }

        # Second call: list of chunks -> succeed and echo chunks unchanged
        if isinstance(text, list):
            return {
                "result": text,
                "text_too_large": False,
                "successful": True,
                "fresh": True,
                "device": "cpu",
            }

        raise AssertionError("Unexpected payload type")

    monkeypatch.setattr(
        GraphAITextTranslationGateway,
        "_call_async_endpoint",
        fake_call_async_endpoint,
    )

    long_text = "x" * 1000

    out = gateway.translate_text(
        text=long_text,
        source_language="fr",
        target_language="en",
    )

    assert out == long_text

    # First call: original long string
    assert isinstance(payloads[0]["text"], str)
    assert payloads[0]["text"] == long_text

    # Second call: split list of chunks
    assert isinstance(payloads[1]["text"], list)
    assert len(payloads[1]["text"]) == 3
    assert payloads[1]["text"][0] == "x" * 400
    assert payloads[1]["text"][1] == "x" * 400
    assert payloads[1]["text"][2] == "x" * 200