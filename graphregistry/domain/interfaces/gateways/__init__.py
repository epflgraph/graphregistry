# graphregistry/domain/interfaces/gateways/__init__.py
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import (
    ConceptDetectionGateway,
)
from graphregistry.domain.interfaces.gateways.gtw_textgen import (
    TextGenerationGateway,
)
from graphregistry.domain.interfaces.gateways.gtw_translation import (
    TextTranslationGateway,
)
__all__ = [
    "ConceptDetectionGateway",
    "TextTranslationGateway",
    "TextGenerationGateway",
]
