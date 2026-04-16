from __future__ import annotations

from graphregistry.domain.models.mdl_text import MultilingualText


class DummyTextTranslationGateway:
    def translate_text(self, text: str, source_language: str, target_language: str) -> str:
        return f"{text}-{target_language}"

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: str,
        target_languages: tuple[str, ...] = ("en", "fr", "de", "it"),
    ) -> MultilingualText:
        out = text.model_copy(deep=True)
        for lang in target_languages:
            if lang != source_language:
                setattr(out, lang, f"{getattr(text, source_language)}-{lang}")
        return out


def test_text_translation_gateway_shape_is_usable() -> None:
    gateway = DummyTextTranslationGateway()
    assert gateway.translate_text("hello", "en", "de") == "hello-de"

    out = gateway.translate_multilingual(MultilingualText(en="hello"), "en", ("en", "fr"))
    assert out.fr == "hello-fr"
