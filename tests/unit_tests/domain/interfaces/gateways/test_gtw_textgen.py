from __future__ import annotations

from graphregistry.domain.models.mdl_text import GeneratedText


class DummyTextGenerationGateway:
    def generate_text(self, prompt: str, language: str = "en") -> GeneratedText:
        return GeneratedText(value=f"{prompt}-{language}")


def test_text_generation_gateway_shape_is_usable() -> None:
    out = DummyTextGenerationGateway().generate_text("hello", "fr")
    assert out.value == "hello-fr"
