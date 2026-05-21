# graphregistry/domain/interfaces/__init__.py
from graphregistry.domain.interfaces.gateways import (
    ConceptDetectionGateway,
    TextGenerationGateway,
    TextTranslationGateway,
)
from graphregistry.domain.interfaces.repositories import (
    EdgeRepository,
    NodeRepository,
)
from graphregistry.domain.interfaces.services import (
    SchemaResolver,
)
__all__ = [
    "ConceptDetectionGateway",
    "TextTranslationGateway",
    "TextGenerationGateway",
    "NodeRepository",
    "EdgeRepository",
    "SchemaResolver",
]
