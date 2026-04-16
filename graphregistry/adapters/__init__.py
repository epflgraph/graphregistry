# graphregistry/adapters/__init__.py
from graphregistry.adapters.gateways.genai import GenAITextGenerationGateway
from graphregistry.adapters.gateways.graphai import GraphAIConceptGateway, GraphAITextTranslationGateway

__all__ = [
    "GraphAIConceptGateway",
    "GraphAITextTranslationGateway",
    "GenAITextGenerationGateway",
]
