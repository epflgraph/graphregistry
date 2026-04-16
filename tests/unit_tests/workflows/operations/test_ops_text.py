from __future__ import annotations

from graphregistry.domain.models.mdl_text import GeneratedText, MultilingualText
from graphregistry.workflows.operations.ops_text import GeneratedTextOperations


class FakeTranslationGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[str, ...]]] = []

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: str,
        target_languages: tuple[str, ...] = ("en", "fr", "de", "it"),
    ) -> MultilingualText:
        self.calls.append((getattr(text, source_language), source_language, target_languages))
        out = text.model_copy(deep=True)
        for lang in target_languages:
            if lang != source_language:
                setattr(out, lang, f"{getattr(text, source_language)}-{lang}")
        return out


class FakeGenerationGateway:
    def __init__(self, value: str = "generated") -> None:
        self.value = value

    def generate_text(self, prompt: str, language: str = "en") -> GeneratedText:
        return GeneratedText(is_auto_generated=True, value=f"{prompt}-{language}-{self.value}")


def test_generate_and_translate_marks_source_and_translated_languages() -> None:
    translation = FakeTranslationGateway()
    generation = FakeGenerationGateway(value="ok")
    ops = GeneratedTextOperations(translation_gateway=translation, generation_gateway=generation)

    out = ops.generate_and_translate(
        prompt="hello",
        source_language="en",
        target_languages=("en", "fr", "de"),
    )

    assert out.en.value == "hello-en-ok"
    assert out.en.is_auto_generated is True
    assert out.en.is_auto_translated is False

    assert out.fr.value == "hello-en-ok-fr"
    assert out.fr.is_auto_generated is False
    assert out.fr.is_auto_translated is True
    assert out.fr.translated_from == "en"

    assert translation.calls == [("hello-en-ok", "en", ("en", "fr", "de"))]


def test_translate_multilingual_delegates_gateway_call() -> None:
    translation = FakeTranslationGateway()
    ops = GeneratedTextOperations(translation_gateway=translation, generation_gateway=FakeGenerationGateway())

    input_text = MultilingualText(en="source")
    out = ops.translate_multilingual(input_text, source_language="en", target_languages=("en", "it"))

    assert out.it == "source-it"
