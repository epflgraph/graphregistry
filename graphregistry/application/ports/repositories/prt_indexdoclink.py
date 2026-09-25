# graphregistry/application/ports/repositories/prt_indexdoclink.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_policies import LinkSelectionPolicy
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class IndexDocLinkRepository(Protocol):
    """Persistence port for the search-index doc-link projections, physically the
    Index_D_{doc_type}_L_{link_type}_T_{partition} tables. It covers the two
    drift dimensions of the state machine: content drift, resolved by the
    vertical patch, and membership and ranking drift, resolved by the
    horizontal patch. The names follow the platform's own metaphor, as
    vertical integration in a company: vertical patches travel along the
    relation to the linked document, horizontal patches operate across the
    document's link set.

    The dirty state of the projections lives on their upstream sources (the
    index buildup tables, the parent-child edges, and the scores matrices),
    which belong to the CacheProjection family; the projection tables
    themselves carry no to_process flag, so this port exposes no state
    methods. The patched field lists and ordering rules are index
    configuration, loaded by the adapters exactly as the legacy patch
    commands load them. Cross-environment deployment of the same tables is
    served by the separate IndexDeployRepository port.
    """

    # Public Method: Ensure the graphsearch doc-link tables of the given keys
    # exist. The legacy IndexDB constructor created every configured
    # doc-link table eagerly, so pairs that are never patched still carry
    # their (empty) table in the schema export.
    def ensure_link_tables(self, keys: list[DocLinkTypeKey]) -> None:
        ...

    # Public Method: Patch the denormalised link fields of one projection from
    # the linked documents' profiles, resolving content drift. The ORG
    # partition additionally refreshes the parent-child link fields.
    def vertical_patch(self, key: DocLinkTypeKey, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Patch and re-rank the links of one projection, adding
    # missing links, replacing stale ones with new scores, and reordering,
    # truncated by the rank threshold; resolves membership and ranking drift.
    def horizontal_patch(self, key: DocLinkTypeKey, policy: LinkSelectionPolicy, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Patch the denormalised link fields of the Elasticsearch
    # cache mirror of one projection, resolving content drift for the search
    # index export.
    def vertical_patch_es_cache(self, key: DocLinkTypeKey, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...

    # Public Method: Patch and re-rank the links of the Elasticsearch cache
    # mirror of one projection, resolving membership and ranking drift for
    # the search index export; reads the graphsearch projection tables,
    # including the mixed views of the pairs configured for mixed scoring.
    def horizontal_patch_es_cache(self, key: DocLinkTypeKey, policy: LinkSelectionPolicy, actions: ActionSet = ('commit',)) -> PropagationStats:
        ...
