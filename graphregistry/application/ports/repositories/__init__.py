# graphregistry/application/ports/repositories/__init__.py
from graphregistry.application.ports.repositories.prt_cacheprojection import (
    CacheProjectionRepository,
)
from graphregistry.application.ports.repositories.prt_changetracking import (
    ChangeTrackingRepository,
)
from graphregistry.application.ports.repositories.prt_edge import (
    EdgeRepository,
)
from graphregistry.application.ports.repositories.prt_formula import (
    FormulaRepository,
)
from graphregistry.application.ports.repositories.prt_indexbuildup import (
    IndexBuildupRepository,
)
from graphregistry.application.ports.repositories.prt_indexdoclink import (
    IndexDocLinkRepository,
)
from graphregistry.application.ports.repositories.prt_indexintegrity import (
    IndexIntegrityRepository,
)
from graphregistry.application.ports.repositories.prt_indexdocs import (
    IndexDocRepository,
)
from graphregistry.application.ports.repositories.prt_lecture import (
    LectureRepository,
)
from graphregistry.application.ports.repositories.prt_lecture_processing import (
    LectureProcessingStatePort,
)
from graphregistry.application.ports.repositories.prt_node import (
    NodeRepository,
)
from graphregistry.application.ports.repositories.prt_pageprofile import (
    PageProfileRepository,
)
from graphregistry.application.ports.repositories.prt_scoresmatrix import (
    ScoresMatrixRepository,
)
from graphregistry.application.ports.repositories.prt_searchindexexport import (
    SearchIndexExportRepository,
)
from graphregistry.application.ports.repositories.prt_typeflags import (
    TypeFlagsRepository,
)

# Re-export the repository ports as a single stable import surface for operations
# and adapters, so the module a port lives in can change without breaking consumers.
__all__ = [
    "CacheProjectionRepository",
    "ChangeTrackingRepository",
    "EdgeRepository",
    "FormulaRepository",
    "IndexBuildupRepository",
    "IndexDocLinkRepository",
    "IndexIntegrityRepository",
    "IndexDocRepository",
    "LectureProcessingStatePort",
    "LectureRepository",
    "NodeRepository",
    "PageProfileRepository",
    "ScoresMatrixRepository",
    "SearchIndexExportRepository",
    "TypeFlagsRepository",
]
