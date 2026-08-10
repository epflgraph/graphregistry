# graphregistry/domain/interfaces/gateways/__init__.py
from graphregistry.application.ports.gateways.prt_conceptdet import (
    ConceptDetectionGateway,
)
from graphregistry.application.ports.gateways.prt_textgen import (
    TextGenerationGateway,
)
from graphregistry.application.ports.gateways.prt_translation import (
    TextTranslationGateway,
)
__all__ = [
    "ConceptDetectionGateway",
    "TextTranslationGateway",
    "TextGenerationGateway",
]
