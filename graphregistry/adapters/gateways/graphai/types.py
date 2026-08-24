# graphregistry/adapters/gateways/graphai/types.py
from __future__ import annotations

from typing import TypeAlias

from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.gtw_embedding import GraphAIEmbeddingGateway
from graphregistry.adapters.gateways.graphai.gtw_image import GraphAIImageGateway
from graphregistry.adapters.gateways.graphai.gtw_translation import GraphAITextTranslationGateway
from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.gtw_voice import GraphAIVoiceGateway

GraphAIGateway: TypeAlias = (
    type[GraphAIConceptDetectionGateway]
    | type[GraphAITextTranslationGateway]
    | type[GraphAIEmbeddingGateway]
    | type[GraphAIImageGateway]
    | type[GraphAIVideoGateway]
    | type[GraphAIVoiceGateway]
)
GraphAIGatewayDict: TypeAlias = dict[str, GraphAIGateway]
