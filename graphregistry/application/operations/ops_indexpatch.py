# graphregistry/application/operations/ops_indexpatch.py
from __future__ import annotations
from loguru import logger as sysmsg
from graphregistry.application.ports.repositories.prt_indexbuildup import IndexBuildupRepository
from graphregistry.application.ports.repositories.prt_indexdoclink import IndexDocLinkRepository
from graphregistry.application.ports.repositories.prt_indexdocs import IndexDocRepository
from graphregistry.application.ports.repositories.prt_pageprofile import PageProfileRepository
from graphregistry.common.config import IndexConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_policies import LinkSelectionPolicy, OrderRule
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig

#==================#
# Class Definition #
#==================#
class IndexPatchOperations:
    """Application operation composing the full index patch cycle, replacing
    the legacy GraphRegistry.IndexDB.patch command: the page-profile patch,
    the docs patch over the fields-active doc types, and the vertical and
    horizontal doc-link patches over the derived doc-link pairs, each in its
    graphsearch and Elasticsearch cache variants.

    The available doc types and doc-link pairs are derived from the index
    and scores configurations exactly as the legacy DynamicSQL derives them;
    the active types come from the typeflags configuration. The 'settle'
    action of the legacy docs patch is honoured for the docs settle step.
    """

    # Public Method: Initialize the operation with the buildup and patch
    # repositories and the configurations driving the type derivation.
    def __init__(self, pageprofile_repo: PageProfileRepository, docs_repo: IndexDocRepository,
                 doclinks_repo: IndexDocLinkRepository, index_config: IndexConfig, scores_config: ScoresConfig,
                 buildup_repo: IndexBuildupRepository | None = None) -> None:
        self.pageprofile_repo = pageprofile_repo
        self.docs_repo = docs_repo
        self.doclinks_repo = doclinks_repo
        self.index_config = index_config
        self.scores_config = scores_config
        self.buildup_repo = buildup_repo

    #================================================================#
    # Method Group: Available type derivation                        #
    #================================================================#

    # Internal Method: Derive the available doc types from the index
    # configuration, as the legacy DynamicSQL does.
    def _available_doc_types(self) -> list[str]:
        return list(self.index_config.settings.get("doc_types", []))

    # Internal Method: Derive the available doc-link pairs with their
    # partitions from the configurations, as the legacy DynamicSQL does:
    # the semantic pairs from the scored tuples plus the ontology edges,
    # the organisational pairs from the configured parent-child links plus
    # the ontology tuples, both in their two stored directions.
    def _available_doclink_types(self) -> list[tuple[str, str, str]]:
        doc_types = self._available_doc_types()

        # Semantic pairs: scored tuples plus every doc type against Concept
        # and Category, plus the ontology self- and cross-pairs.
        edge_types_to_score = (
            self.scores_config.settings.get('scored_edge_tuples', {}).get('research', [])
            + self.scores_config.settings.get('scored_edge_tuples', {}).get('education', [])
        )
        edge_types_to_score += [[d, 'Concept'] for d in doc_types]
        edge_types_to_score += [[d, 'Category'] for d in doc_types]
        edge_types_to_score += [['Concept', 'Concept'], ['Category', 'Category'], ['Category', 'Concept']]
        canonical_sem = sorted({tuple(sorted(t)) for t in edge_types_to_score})

        # Organisational pairs: the configured parent-child links plus the
        # ontology tuples.
        parent_child = self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("links", {}).get("parent_child", {})
        p2c_tuples = [tuple(sorted([k1, k2])) for k1 in parent_child for k2 in parent_child[k1]]
        p2c_tuples += [('Category', 'Category'), ('Category', 'Concept')]
        canonical_org = sorted(set(p2c_tuples))

        # Expand both families into their two stored directions.
        available = set()
        for pair in canonical_sem:
            available.add((pair[0], pair[1], 'SEM'))
            if pair[0] != pair[1]:
                available.add((pair[1], pair[0], 'SEM'))
        for pair in canonical_org:
            available.add((pair[0], pair[1], 'ORG'))
            if pair[0] != pair[1]:
                available.add((pair[1], pair[0], 'ORG'))
        return sorted(available)

    #================================================================#
    # Method Group: Pair derivation                                  #
    #================================================================#

    # Internal Method: Derive the doc-link pairs to patch, mirroring the
    # legacy _all orchestrators: organisational pairs restricted to the
    # active edge flags, pairs whose link side is an active doc type, and
    # the semantic ontology-object pairs whose object side is active, in
    # both stored directions. The edge-pair restriction differs between the
    # vertical patch (fields flags only) and the horizontal patch (fields
    # and scores flags), matching the legacy commands.
    def _derive_doclink_pairs(self, flags: TypeFlagConfig, edge_flags: str) -> list[tuple[str, str, str]]:
        available = self._available_doclink_types()
        available_set = set(available)

        # Active doc types across both flag families.
        active_doc_types = set(flags.active_node_types("fields")) | set(flags.active_node_types("scores"))

        # Active edge pairs in both directions, from the requested flag
        # family, intersected with the indexable contexts as the legacy
        # get_types_to_process does.
        contexts = {tuple(sorted(k)) for k in self.index_config.settings.get("edge_selection_contexts", {})}
        active_edge_pairs = set()
        for pair in flags.active_edge_pairs():
            canonical = tuple(sorted(pair.as_tuple))
            if canonical in contexts:
                active_edge_pairs.add(canonical)
                active_edge_pairs.add((canonical[1], canonical[0]))

        # Organisational pairs restricted to the active edge flags.
        pairs = {t for t in available if (t[0], t[1]) in active_edge_pairs and t[2] == 'ORG'}

        # Pairs whose link side is an active doc type.
        pairs |= {t for t in available if t[1] in active_doc_types}

        # Semantic ontology-object pairs whose object side is active, covering
        # pairs like Category-Course even without explicit typeflags edges.
        ontology_types = {'Concept', 'Category'}
        for doc_type, link_type, partition in available:
            if partition != 'SEM':
                continue
            if (doc_type in ontology_types) != (link_type in ontology_types):
                object_type = link_type if doc_type in ontology_types else doc_type
                if object_type in active_doc_types:
                    pairs.add((doc_type, link_type, partition))

        # Process links in both directions.
        pairs |= {(t[1], t[0], t[2]) for t in pairs if (t[1], t[0], t[2]) in available_set}
        return sorted(pairs)

    #================================================================#
    # Method Group: Policy construction                              #
    #================================================================#

    # Internal Method: Build the selection policy of one doc-link pair for
    # the SQL-side patch, with the ordering rules of its partition and the
    # default rank threshold; the Elasticsearch cache patch uses the same
    # policy at half the threshold, as the legacy defaults do.
    def _policy_for(self, doc_type: str, link_type: str, partition: str, es_cache: bool = False) -> LinkSelectionPolicy:
        policy = LinkSelectionPolicy.default_for_partition(partition)
        if partition == 'ORG':
            rules = (
                self.index_config.settings.get("graphsearch", {}).get("order_by", {})
                .get("links", {}).get("parent_child", {}).get(doc_type, {}).get(link_type, [])
            )
        else:
            rules = (
                self.index_config.settings.get("graphsearch", {}).get("order_by", {})
                .get("links", {}).get("default", {}).get(link_type, [])
            )
        if rules:
            policy.ordering = [OrderRule(field=rule[0], direction=rule[1]) for rule in rules]
        if es_cache:
            policy.rank_threshold = 16
        return policy

    #================================================================#
    # Method Group: Patch cycle                                      #
    #================================================================#

    # Public Method: Build the index staging tables over the fields-active
    # types: the doc-fields buildup per doc type and the parent-child links
    # buildup per canonical edge pair, mirroring the legacy
    # CacheBuildup.build_all command.
    def build(self, flags: TypeFlagConfig, actions: tuple = ('commit',)) -> list[PropagationStats]:
        if self.buildup_repo is None:
            raise RuntimeError("The index patch operation was built without a buildup repository.")
        stats = []
        sysmsg.info("🚜 📝 Build up and/or update index field tables [actions: {}].".format(actions))

        # The build scope: fields-active doc types and canonical edge pairs,
        # intersected with the indexable contexts as the legacy
        # get_types_to_process does.
        doc_types_to_process = sorted(set(flags.active_node_types("fields")))
        contexts = {tuple(sorted(k)) for k in self.index_config.settings.get("edge_selection_contexts", {})}
        doclink_pairs = sorted({
            tuple(sorted(pair.as_tuple))
            for pair in flags.active_edge_pairs()
            if tuple(sorted(pair.as_tuple)) in contexts
        })
        if not doc_types_to_process and not doclink_pairs:
            sysmsg.warning("No type flags found. Nothing to do.")
            return []

        # Print the list of staging tables that will be built.
        print('\n[🐬 GraphSearch DB] [B-BD] The following tables will be affected:')
        for doc_type in doc_types_to_process:
            print(f" - IndexBuildup_Fields_Docs_{doc_type}")
        for doc_type, link_type in doclink_pairs:
            print(f" - IndexBuildup_Fields_Links_ParentChild_{doc_type}_{link_type}")
        print('')

        # Build the doc staging tables, then the link staging tables.
        for doc_type in doc_types_to_process:
            stats.append(self.buildup_repo.build_docs_fields(doc_type=doc_type, actions=actions))
        for doc_type, link_type in doclink_pairs:
            stats.append(self.buildup_repo.build_links_parentchild(doc_type=doc_type, link_type=link_type, actions=actions))
        sysmsg.success("🚜 ✅ Done building up and/or updating index field tables.")
        return stats

    # Public Method: Run the full index patch cycle over the active
    # typeflags: page profiles, docs, and doc-links in both patch
    # dimensions and both projection families.
    def patch(self, flags: TypeFlagConfig, actions: tuple = ('commit',)) -> list[PropagationStats]:
        stats = []
        sysmsg.info("🚜 📝 Patching index tables [page profile, docs, doc-links].")

        # Page profile patch first, feeding the presentation fields.
        stats.append(self.pageprofile_repo.patch(actions=actions))

        # Docs patch over the fields-active doc types.
        stats.extend(self._patch_docs(flags, actions))

        # Doc-link patches over the derived pairs: vertical with the fields
        # edge flags, horizontal with both flag families.
        vertical_pairs = self._derive_doclink_pairs(flags, edge_flags='fields')
        stats.extend(self._patch_doclinks(vertical_pairs, actions, vertical=True))
        horizontal_pairs = self._derive_doclink_pairs(flags, edge_flags='fields+scores')
        stats.extend(self._patch_doclinks(horizontal_pairs, actions, vertical=False))

        # Report the completion of the full cycle.
        sysmsg.success("🚜 ✅ Done patching index tables.")
        return stats

    # Internal Method: Patch the doc projections of the fields-active doc
    # types, in both projection families, settling the change records when
    # the legacy 'settle' action is requested.
    def _patch_docs(self, flags: TypeFlagConfig, actions: tuple) -> list[PropagationStats]:
        active_doc_types = set(flags.active_node_types("fields"))
        doc_types_to_process = sorted({t for t in self._available_doc_types() if t in active_doc_types})
        if not doc_types_to_process:
            sysmsg.warning("No type flags found for 'docs'. Nothing to do.")
            return []

        # Print the list of doc types that will be patched.
        print('\n[🐬 GraphSearch DB] [D-P-DB] The following doc types will be affected:')
        for doc_type in doc_types_to_process:
            print(f" - {doc_type}")
        print('')

        # Patch every doc type in both projection families.
        stats = []
        for doc_type in doc_types_to_process:
            stats.append(self.docs_repo.patch(doc_type=doc_type, actions=actions))
            stats.append(self.docs_repo.patch_es_cache(doc_type=doc_type, actions=actions))
            if 'settle' in actions:
                self.docs_repo.settle(doc_type=doc_type, actions=actions)
        return stats

    # Internal Method: Patch the doc-link projections of the derived pairs,
    # in the graphsearch and Elasticsearch cache families.
    def _patch_doclinks(self, pairs: list[tuple[str, str, str]], actions: tuple, vertical: bool) -> list[PropagationStats]:
        if not pairs:
            sysmsg.warning("No type flags found for 'doc-links'. Nothing to do.")
            return []

        # Print the list of doc-link tables that will be patched.
        dimension = 'vertical' if vertical else 'horizontal'
        print(f'\n[🐬 GraphSearch DB] [DL-{"V" if vertical else "H"}P-DB] The following doc-link tables will be affected ({dimension}):')
        for doc_type, link_type, partition in pairs:
            print(f" - Index_D_{doc_type}_L_{link_type}_T_{partition}")
        print('')

        # Patch every derived pair in the graphsearch family.
        stats = []
        for doc_type, link_type, partition in pairs:
            key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition=partition)
            policy = self._policy_for(doc_type, link_type, partition)
            if vertical:
                stats.append(self.doclinks_repo.vertical_patch(key=key, actions=actions))
            else:
                stats.append(self.doclinks_repo.horizontal_patch(key=key, policy=policy, actions=actions))

        # The Elasticsearch cache patch runs per deduplicated pair, without
        # the partition distinction, using the SEM variant when available.
        es_pairs = sorted({(doc_type, link_type) for doc_type, link_type, _ in pairs})
        available = set(self._available_doclink_types())
        print(f'\n[⚡️ ElasticSearch] [DL-{"V" if vertical else "H"}P-ES] The following cache tables will be affected ({dimension}):')
        for doc_type, link_type in es_pairs:
            print(f" - Index_D_{doc_type}_L_{link_type}")
        print('')

        # The es_cache variant picks the SEM table when the pair carries one.
        for doc_type, link_type in es_pairs:
            partition = 'SEM' if (doc_type, link_type, 'SEM') in available else 'ORG'
            key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition=partition)
            es_policy = self._policy_for(doc_type, link_type, partition, es_cache=True)
            if vertical:
                stats.append(self.doclinks_repo.vertical_patch_es_cache(key=key, actions=actions))
            else:
                stats.append(self.doclinks_repo.horizontal_patch_es_cache(key=key, policy=es_policy, actions=actions))
        return stats
