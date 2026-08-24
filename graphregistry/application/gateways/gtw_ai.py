# graphregistry/domain/interfaces/gateways/gtw_ai.py
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.application.gateways.gtw_translation import TextTranslationGateway

AIGateways = {
    "concept_detection": ConceptDetectionGateway,
    "translation": TextTranslationGateway
}