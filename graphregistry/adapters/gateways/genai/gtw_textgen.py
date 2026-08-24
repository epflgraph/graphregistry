# graphregistry/adapters/gateways/genai/gtw_textgen.py
from __future__ import annotations
from graphregistry.application.ports.gateways.prt_textgen import TextGenerationGateway
from graphregistry.domain.models.entities.mdl_text import GeneratedText, LanguageCode

# Class definitio
class GenAITextGenerationGateway(TextGenerationGateway):
    """
    Placeholder adapter for future GenAI API integration.
    """

    def generate_text(self, prompt: str, language: LanguageCode = "en") -> GeneratedText:
        raise NotImplementedError(
            "GenAITextGenerationGateway.generate_text is a placeholder. "
            "Integrate your GenAI provider here."
        )
