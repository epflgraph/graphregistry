# graphregistry/domain/interfaces/gateways/types.py
from __future__ import annotations
from typing import TypeAlias, TypedDict
from graphregistry.application.gateways.gtw_conceptdet    import ConceptDetectionGateway
from graphregistry.application.gateways.gtw_embedding     import TextEmbeddingGateway
from graphregistry.application.gateways.gtw_image         import ImageProcessingGateway
from graphregistry.application.gateways.gtw_translation   import TextTranslationGateway
from graphregistry.application.gateways.gtw_lectureenrich import LectureEnrichmentGateway
from graphregistry.application.gateways.gtw_video         import VideoProcessingGateway
from graphregistry.application.gateways.gtw_voice         import VoiceProcessingGateway

Gateway: TypeAlias = (
    ConceptDetectionGateway
    | TextEmbeddingGateway
    | ImageProcessingGateway
    | TextTranslationGateway
    | LectureEnrichmentGateway
    | VideoProcessingGateway
    | VoiceProcessingGateway
)

class GatewayDict(TypedDict, total=False):
    video_processing   : VideoProcessingGateway
    voice_processing   : VoiceProcessingGateway
    concept_detection  : ConceptDetectionGateway
    text_translation   : TextTranslationGateway
    text_embedding     : TextEmbeddingGateway
    image_processing   : ImageProcessingGateway
    lecture_enrichment : LectureEnrichmentGateway
