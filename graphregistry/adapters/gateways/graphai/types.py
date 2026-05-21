# graphregistry/adapters/gateways/graphai/types.py
from __future__ import annotations
from typing import TypeAlias
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway

# Define a type for supported field languages, which can be used in custom fields of nodes
GraphAIGateway: TypeAlias = type[GraphAIConceptDetectionGateway] | type[GraphAITextTranslationGateway]
GraphAIGatewayDict: TypeAlias = dict[str, GraphAIGateway]
