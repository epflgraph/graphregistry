# graphregistry/domain/interfaces/gateways/gtw_textgen.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.entities.mdl_text import GeneratedText, LanguageCode

# Model definition
class TextGenerationGateway(Protocol):
    def generate_text(self, prompt: str, language: LanguageCode = "en") -> GeneratedText:
        ...
