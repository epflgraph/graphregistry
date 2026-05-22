# graphregistry/domain/interfaces/gateways/types.py
from __future__ import annotations
from typing import TypeAlias, TypedDict
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.domain.interfaces.gateways.gtw_translation import TextTranslationGateway

Gateway: TypeAlias = ConceptDetectionGateway | TextTranslationGateway

class GatewayDict(TypedDict, total=False):
    concept_detection : ConceptDetectionGateway
    text_translation  : TextTranslationGateway