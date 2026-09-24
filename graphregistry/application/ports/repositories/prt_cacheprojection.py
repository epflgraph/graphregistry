# graphregistry/application/ports/repositories/prt_cacheprojection.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class CacheProjectionRepository(Protocol):
    """Persistence port for the dirty flags of the downstream cache projections:
    page profiles, degree scores, parent-child edges, index buildup tables,
    scores matrices, and final scores. Replaces the propagation half of the
    legacy GraphRegistry.Orchestration.propagate command and the cache and
    traversals resets.
    """

    # Public Method: Propagate dirty flags from the airflow tracking tables to the
    # cache projections selected by the processing scope, mirroring the legacy
    # propagate command.
    def propagate(self, scope: ProcessingScope, include_fields: bool = True, include_scores: bool = True, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        ...

    # Public Method: Clear the to_process flags of the cache and traversals
    # projections, mirroring the cache and traversals options of the legacy
    # reset command.
    def reset_flags(self, include_cache: bool = True, include_traversals: bool = True, actions: ActionSet = ('commit',)) -> None:
        ...
