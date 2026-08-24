# graphregistry/domain/interfaces/repositories/__init__.py
from graphregistry.domain.repositories.rpo_edge import (
    EdgeRepository,
)
from graphregistry.domain.repositories.rpo_node import (
    NodeRepository,
)
from graphregistry.domain.repositories.rpo_lecture import (
    LectureRepository,
)
from graphregistry.domain.repositories.rpo_lecture_processing import (
    LectureProcessingStatePort,
)

__all__ = [
    "NodeRepository",
    "EdgeRepository",
    "LectureRepository",
    "LectureProcessingStatePort",
]
