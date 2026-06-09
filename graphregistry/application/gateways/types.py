# graphregistry/domain/interfaces/gateways/types.py
from __future__ import annotations
from typing import TypeAlias, TypedDict
from graphregistry.application.gateways.gtw_conceptdet    import ConceptDetectionGateway
from graphregistry.application.gateways.gtw_translation   import TextTranslationGateway
from graphregistry.application.gateways.gtw_lectureenrich import LectureEnrichmentGateway
from graphregistry.application.gateways.gtw_video         import VideoProcessingGateway

Gateway: TypeAlias = ConceptDetectionGateway | TextTranslationGateway | VideoProcessingGateway

class GatewayDict(TypedDict, total=False):
    video_processing   : VideoProcessingGateway
    concept_detection  : ConceptDetectionGateway
    text_translation   : TextTranslationGateway
    lecture_enrichment : LectureEnrichmentGateway