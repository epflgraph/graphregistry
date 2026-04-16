# graphregistry/adapters/gateways/graphai/__init__.py
from graphregistry.adapters.gateways.graphai.agt_conceptgatw import GraphAIConceptGateway
from graphregistry.adapters.gateways.graphai.agt_translationgatw import GraphAITextTranslationGateway

__all__ = ["GraphAIConceptGateway", "GraphAITextTranslationGateway"]
