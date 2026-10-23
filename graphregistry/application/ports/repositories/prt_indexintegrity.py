# graphregistry/application/ports/repositories/prt_indexintegrity.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class IndexIntegrityRepository(Protocol):
    """Persistence port for the integrity maintenance of the search-index
    tables: pruning the loose ends that accumulate across patch cycles.

    The command walks five steps, mirroring the legacy
    GraphRegistry.IndexDB.delete_loose_ends: overflow rank rows are removed
    from the semantic tables, deleted nodes are cleaned from the page
    profile, the largest connected component is computed over the index
    links (cached across runs), every index table is pruned against that
    component, dangling doc-link references are removed, and a final
    verification reports any remaining orphans.
    """

    # Public Method: Prune the loose ends from the index tables; the largest
    # connected graph is recomputed unless the cached one is reused.
    def delete_loose_ends(self, refresh_graph: bool = True, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...
