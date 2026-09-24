# graphregistry/application/ports/repositories/prt_pageprofile.py
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
class PageProfileRepository(Protocol):
    """Persistence port for the page-profile projection of the search index:
    the graphsearch copy of the graph_cache page-profile table, holding the
    presentation fields of every indexed object.

    The patch copies the flagged page-profile rows into the graphsearch
    schema, driven by the fields-changed flags and the active typeflags. The
    patched column list is the live table schema, discovered by the adapter
    as the legacy patch discovers it. Snapshots and rollbacks remain
    unimplemented stubs in the legacy and are not ported.
    """

    # Public Method: Patch the page-profile projection from the flagged
    # graph_cache rows into the graphsearch schema.
    def patch(self, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...
