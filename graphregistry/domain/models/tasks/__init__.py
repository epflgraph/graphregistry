# graphregistry/domain/models/tasks/__init__.py
from graphregistry.domain.models.tasks.mdl_conceptdet import (
    ConceptDetectionTask,
    ConceptDetectionResult,
    ConceptDetectionResultList,
)
from graphregistry.domain.models.tasks.mdl_translation import (
    TranslationTask,
)
__all__ = [
    "ConceptDetectionTask",
    "ConceptDetectionResult",
    "ConceptDetectionResultList",
    "TranslationTask",
]
