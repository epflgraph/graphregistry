# graphregistry/domain/interfaces/gateways/__init__.py
from graphregistry.application.gateways.gtw_conceptdet import (
    ConceptDetectionGateway,
)
from graphregistry.application.gateways.gtw_textgen import (
    TextGenerationGateway,
)
from graphregistry.application.gateways.gtw_translation import (
    TextTranslationGateway,
)
__all__ = [
    "ConceptDetectionGateway",
    "TextTranslationGateway",
    "TextGenerationGateway",
]
