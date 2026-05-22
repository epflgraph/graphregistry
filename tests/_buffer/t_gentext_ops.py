import unittest

from graphregistry.domain.models.entities.mdl_text import GeneratedText, MultilingualText
from graphregistry.application.operations.ops_text import GeneratedTextOperations


class FakeTranslationGateway:
    def translate_text(self, text: str, source_language: str, target_language: str) -> str:
        return f"{text}:{source_language}->{target_language}"

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: str,
        target_languages: tuple[str, ...] = ("en", "fr", "de", "it"),
    ) -> MultilingualText:
        out = text.model_copy(deep=True)
        src = getattr(text, source_language, None)
        if src is None:
            return out
        for lang in target_languages:
            if getattr(out, lang, None) is None:
                setattr(out, lang, f"{src}:{source_language}->{lang}")
        return out


class FakeGenerationGateway:
    def generate_text(self, prompt: str, language: str = "en") -> GeneratedText:
        return GeneratedText(is_auto_generated=True, value=f"{language}:{prompt}")


class GeneratedTextOperationsTests(unittest.TestCase):
    def test_generate_and_translate(self) -> None:
        ops = GeneratedTextOperations(
            translation_gateway=FakeTranslationGateway(),
            generation_gateway=FakeGenerationGateway(),
        )

        out = ops.generate_and_translate(prompt="hello", source_language="en")

        self.assertEqual(out.en.value, "en:hello")
        self.assertEqual(out.fr.value, "en:hello:en->fr")
        self.assertTrue(out.en.is_auto_generated)
        self.assertTrue(out.fr.is_auto_translated)


if __name__ == "__main__":
    unittest.main()
