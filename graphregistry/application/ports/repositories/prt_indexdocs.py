# graphregistry/application/ports/repositories/prt_indexdocs.py
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
class IndexDocRepository(Protocol):
    """Persistence port for the search-index doc projections, physically the
    Index_D_{doc_type} tables of the graphsearch and es_cache schemas: the
    flattened, denormalised documents consumed by the search index.

    The patch refreshes the document rows from the page profiles and the
    index buildup tables, resolving content drift at the document level. The
    settle step closes the tracking cycle for the patched documents by
    stamping the airflow change records, and the flag cleanup resets the
    upstream buildup flags. The patched field lists are index configuration,
    loaded by the adapters exactly as the legacy patch commands load them.
    """

    # Public Method: Patch the doc projection of one doc type in the
    # graphsearch schema from the page profiles and the buildup rows.
    def patch(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Patch the doc projection of one doc type in the
    # Elasticsearch cache schema, feeding the search index export.
    def patch_es_cache(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Settle the airflow change records of the patched
    # documents, stamping the cache date and clearing the processing flags,
    # mirroring the legacy 'settle' action of the docs patch.
    def settle(self, doc_type: str, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Reset the processing flags of the page profiles and
    # buildup rows of one doc type after the patch cycle completed.
    def cleanup_flags(self, doc_type: str, actions: ActionSet = ('commit',)) -> None:
        ...
