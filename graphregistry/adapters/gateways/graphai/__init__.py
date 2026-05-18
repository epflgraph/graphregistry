# graphregistry/adapters/gateways/graphai/__init__.py
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway

__all__ = ["GraphAIConceptGateway", "GraphAITextTranslationGateway"]
