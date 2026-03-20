from __future__ import annotations

from typing import Protocol

from graphregistry.domain.models.mdl_gentext import GeneratedText


class TextGenerationGateway(Protocol):
    def generate_text(self, prompt: str, language: str = "en") -> GeneratedText:
        ...
