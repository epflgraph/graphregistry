# graphregistry/domain/interfaces/gateways/types.py
from __future__ import annotations
from typing import TypeAlias, TypedDict
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.application.gateways.gtw_translation import TextTranslationGateway
from graphregistry.application.gateways.gtw_lectureenrich import LectureEnrichmentGateway

Gateway: TypeAlias = ConceptDetectionGateway | TextTranslationGateway

class GatewayDict(TypedDict, total=False):
    concept_detection  : ConceptDetectionGateway
    text_translation   : TextTranslationGateway
    lecture_enrichment : LectureEnrichmentGateway