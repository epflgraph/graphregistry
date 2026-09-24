# graphregistry/application/ports/repositories/prt_indexbuildup.py
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
class IndexBuildupRepository(Protocol):
    """Persistence port for the index buildup tables of the graph cache:
    the IndexBuildup_Fields_Docs_{doc_type} and
    IndexBuildup_Fields_Links_ParentChild_{doc}_{link} staging tables that
    feed the doc and doc-link projections.

    The build replaces the flagged rows of the staging tables from the page
    profiles, degree scores, and flattened parent-child edges, joining the
    per-language custom fields from the all-fields tables. The field lists
    and the include-code option are index configuration, loaded by the
    adapters exactly as the legacy build commands load them.
    """

    # Public Method: Build the doc-fields staging table of one doc type from
    # the flagged page profiles and degree scores.
    def build_docs_fields(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Build the parent-child links staging table of one pair
    # from the flagged symmetric edges, resolving the canonical edge context.
    def build_links_parentchild(self, doc_type: str, link_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...
