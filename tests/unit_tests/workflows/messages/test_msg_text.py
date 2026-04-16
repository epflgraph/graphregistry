from __future__ import annotations

from graphregistry.workflows.messages.msg_text import (
    GenerateAndTranslateRequest,
    GenerateTextRequest,
    TranslateMultilingualTextRequest,
)


def test_text_message_models_apply_defaults() -> None:
    translate = TranslateMultilingualTextRequest(
        text={"en": "hello"},
        source_language="en",
    )
    generate = GenerateTextRequest(prompt="Write a summary")
    both = GenerateAndTranslateRequest(prompt="Describe this", source_language="fr")

    assert translate.target_languages == ("en", "fr", "de", "it")
    assert generate.language == "en"
    assert both.target_languages == ("en", "fr", "de", "it")
