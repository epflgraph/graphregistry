# graphregistry/adapters/gateways/graphai/types.py
from __future__ import annotations

from typing import TypeAlias

from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.agt_embedding import GraphAIEmbeddingGateway
from graphregistry.adapters.gateways.graphai.agt_image import GraphAIImageGateway
from graphregistry.adapters.gateways.graphai.agt_translation import GraphAITextTranslationGateway
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_audio import GraphAIAudioGateway

GraphAIGateway: TypeAlias = (
    type[GraphAIConceptDetectionGateway]
    | type[GraphAITextTranslationGateway]
    | type[GraphAIEmbeddingGateway]
    | type[GraphAIImageGateway]
    | type[GraphAIVideoGateway]
    | type[GraphAIAudioGateway]
)
GraphAIGatewayDict: TypeAlias = dict[str, GraphAIGateway]
