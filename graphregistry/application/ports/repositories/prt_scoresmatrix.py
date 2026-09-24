# graphregistry/application/ports/repositories/prt_scoresmatrix.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.pipeline.mdl_scores import ScoreConsolidationParams
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class ScoresMatrixRepository(Protocol):
    """Persistence port for the object-to-object scores matrices, in their
    group-by-concepts and adjusted-scores flavours per research, education, and
    ontology domain. Replaces the legacy GraphRegistry.CacheManagement
    calculate_scores_matrix and consolidate_scores_matrix methods.
    """

    # Public Method: Rebuild the group-by-concepts matrix of one edge family from
    # the object-to-concept final scores, applying the consolidation thresholds.
    # Returns None when the pair is excluded from matrix calculation, as the
    # ontology tuples are.
    def calculate_matrix(self, type_pair: EdgeTypePair, params: ScoreConsolidationParams, actions: ActionSet = ('commit',)) -> PropagationStats | None:
        ...

    # Public Method: Consolidate the adjusted-scores matrix of one edge family,
    # optionally refreshing the rolling averages, mirroring the legacy
    # consolidate_scores_matrix method.
    def consolidate_matrix(self, type_pair: EdgeTypePair, params: ScoreConsolidationParams, update_averages: bool = False, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Compose calculation and consolidation over the edge
    # families selected by the processing scope and the scores configuration,
    # mirroring the legacy update_scores_matrix command.
    def update_matrix(self, scope: ProcessingScope, params: ScoreConsolidationParams, actions: ActionSet = ('commit',)) -> None:
        ...
