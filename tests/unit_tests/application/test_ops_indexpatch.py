# graphregistry/tests/unit_tests/application/test_ops_indexpatch.py
"""Unit tests for IndexPatchOperations using in-memory repository fakes.

The fakes record every patch call so the tests verify the pair derivation of
the orchestrators — the organisational restriction, the link-side activation,
the semantic ontology-object special case, the bidirectional expansion, and
the Elasticsearch cache deduplication — and the composition of the cycle.
"""
from __future__ import annotations
from graphregistry.application.operations.ops_indexpatch import IndexPatchOperations
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig

#==================#
# Class Definition #
#==================#
class FakePageProfileRepo:
    """In-memory fake recording the page-profile patch calls."""

    # Public Method: Initialize the fake with an empty call log.
    def __init__(self) -> None:
        self.calls = []

    # Public Method: Record a patch call.
    def patch(self, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("patch", actions))
        return PropagationStats(target="pageprofile", rows_flagged=0)

#==================#
# Class Definition #
#==================#
class FakeDocsRepo:
    """In-memory fake recording the docs patch calls."""

    # Public Method: Initialize the fake with an empty call log.
    def __init__(self) -> None:
        self.calls = []

    # Public Method: Record a graphsearch patch call.
    def patch(self, doc_type: str, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("patch", doc_type))
        return PropagationStats(target=f"docs:{doc_type}", rows_flagged=0)

    # Public Method: Record an es_cache patch call.
    def patch_es_cache(self, doc_type: str, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("patch_es_cache", doc_type))
        return PropagationStats(target=f"docs_es:{doc_type}", rows_flagged=0)

    # Public Method: Record a settle call.
    def settle(self, doc_type: str, actions: tuple = ('commit',)) -> None:
        self.calls.append(("settle", doc_type))

#==================#
# Class Definition #
#==================#
class FakeDocLinksRepo:
    """In-memory fake recording the doc-link patch calls with their keys."""

    # Public Method: Initialize the fake with an empty call log.
    def __init__(self) -> None:
        self.calls = []

    # Public Method: Record a vertical patch call.
    def vertical_patch(self, key: DocLinkTypeKey, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("vertical", key.doc_type, key.link_type, key.partition))
        return PropagationStats(target=f"links:{key.doc_type}:{key.link_type}:{key.partition}", rows_flagged=0)

    # Public Method: Record a horizontal patch call.
    def horizontal_patch(self, key: DocLinkTypeKey, policy, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("horizontal", key.doc_type, key.link_type, key.partition, policy.rank_threshold))
        return PropagationStats(target=f"links_h:{key.doc_type}:{key.link_type}:{key.partition}", rows_flagged=0)

    # Public Method: Record an es_cache vertical patch call.
    def vertical_patch_es_cache(self, key: DocLinkTypeKey, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("vertical_es", key.doc_type, key.link_type, key.partition))
        return PropagationStats(target=f"links_es:{key.doc_type}:{key.link_type}", rows_flagged=0)

    # Public Method: Record an es_cache horizontal patch call.
    def horizontal_patch_es_cache(self, key: DocLinkTypeKey, policy, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("horizontal_es", key.doc_type, key.link_type, key.partition, policy.rank_threshold))
        return PropagationStats(target=f"links_h_es:{key.doc_type}:{key.link_type}", rows_flagged=0)

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration driving the type derivation."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "doc_types"   : ["Person", "Course", "Exercise", "Concept", "Category"],
            "graphsearch" : {
                "fields"  : {"links" : {"parent_child" : {"Person": {"Course": []}, "Course": {"Person": []}}}},
                "order_by": {"links" : {"default": {"Course": [["degree_score", "DESC"]]}, "parent_child": {}}},
            },
            "edge_selection_contexts" : {("Course", "Person"): "teacher"},
        }

#==================#
# Class Definition #
#==================#
class FakeScoresConfig:
    """Fixed scores configuration driving the pair derivation."""

    # Public Method: Initialize the fake with the scores settings.
    def __init__(self) -> None:
        self.settings = {
            'scored_edge_tuples' : {
                'education' : [['Course', 'Exercise']],
                'research'  : [],
            },
            'mixed_scoring_tuples' : [('Course', 'Person')],
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build the operation over the recording fakes.
def _make_ops() -> tuple[IndexPatchOperations, FakePageProfileRepo, FakeDocsRepo, FakeDocLinksRepo]:
    pageprofile_repo = FakePageProfileRepo()
    docs_repo = FakeDocsRepo()
    doclinks_repo = FakeDocLinksRepo()
    ops = IndexPatchOperations(
        pageprofile_repo = pageprofile_repo,
        docs_repo        = docs_repo,
        doclinks_repo    = doclinks_repo,
        index_config     = FakeIndexConfig(),
        scores_config    = FakeScoresConfig(),
    )
    return ops, pageprofile_repo, docs_repo, doclinks_repo

#================================================================#
# Function Group: Pair derivation tests                          #
#================================================================#

# Public Function: Verify the vertical pair derivation with the ontology case.
def test_derive_doclink_pairs_vertical() -> None:
    ops, _pp, _d, _dl = _make_ops()
    flags = TypeFlagConfig.from_json({
        "nodes": [["Person", True, True], ["Course", True, False], ["Exercise", False, True]],
        "edges": [["Person", "Course", True]],
    })

    # Fields edge flags restrict the ORG pairs to the configured context;
    # link-side activation adds the SEM pairs of the active doc types; the
    # ontology special case adds Concept/Category SEM pairs of active
    # object types; both directions are expanded.
    pairs = ops._derive_doclink_pairs(flags, edge_flags='fields')
    assert ('Course', 'Person', 'ORG') in pairs and ('Person', 'Course', 'ORG') in pairs
    assert ('Course', 'Exercise', 'SEM') in pairs and ('Exercise', 'Course', 'SEM') in pairs
    assert ('Concept', 'Person', 'SEM') in pairs and ('Category', 'Person', 'SEM') in pairs
    assert ('Person', 'Concept', 'SEM') in pairs

# Public Function: Verify the horizontal derivation includes scores flags.
def test_derive_doclink_pairs_horizontal() -> None:
    ops, _pp, _d, _dl = _make_ops()
    flags = TypeFlagConfig.from_json({
        "nodes": [["Exercise", False, True]],
        "edges": [["Person", "Course", True]],
    })

    # Scores-active doc types drive the link-side pairs even without fields.
    pairs = ops._derive_doclink_pairs(flags, edge_flags='fields+scores')
    assert ('Course', 'Exercise', 'SEM') in pairs and ('Exercise', 'Course', 'SEM') in pairs
    assert ('Concept', 'Exercise', 'SEM') in pairs

# Public Function: Verify the vertical derivation excludes scores-only edges.
def test_derive_doclink_pairs_vertical_ignores_scores_edges() -> None:
    ops, _pp, _d, _dl = _make_ops()
    flags = TypeFlagConfig.from_json({
        "nodes": [["Person", True, True]],
        "edges": [],
    })

    # The Exercise-Course education pair is available but not fields-active
    # on the link side in the vertical derivation, so it is excluded.
    pairs = ops._derive_doclink_pairs(flags, edge_flags='fields')
    assert not any(link_type == 'Exercise' for _doc, link_type, _part in pairs)

#================================================================#
# Function Group: Cycle composition tests                        #
#================================================================#

# Public Function: Verify the full cycle composes the four phases in order.
def test_patch_composes_the_cycle() -> None:
    ops, pageprofile_repo, docs_repo, doclinks_repo = _make_ops()
    flags = TypeFlagConfig.from_json({
        "nodes": [["Person", True, True], ["Course", True, False]],
        "edges": [["Person", "Course", True]],
    })

    # Run the full patch cycle in commit mode.
    stats = ops.patch(flags=flags, actions=('commit',))

    # The page profile patch runs once, first.
    assert pageprofile_repo.calls == [("patch", ('commit',))]

    # The docs patch runs per fields-active doc type, in both families.
    assert ("patch", "Course") in docs_repo.calls and ("patch_es_cache", "Course") in docs_repo.calls
    assert not any(doc_type == "Exercise" for _kind, doc_type in docs_repo.calls)

    # The doc-link patches run per derived pair in both dimensions, and the
    # es_cache patches run per deduplicated pair with the SEM variant.
    vertical = [c for c in doclinks_repo.calls if c[0] == 'vertical']
    horizontal = [c for c in doclinks_repo.calls if c[0] == 'horizontal']
    vertical_es = [c for c in doclinks_repo.calls if c[0] == 'vertical_es']
    horizontal_es = [c for c in doclinks_repo.calls if c[0] == 'horizontal_es']
    assert len(vertical) > 0 and len(horizontal) > 0

    # The es_cache dedup drops the partition distinction: one call per pair.
    assert len({(c[1], c[2]) for c in vertical_es}) == len(vertical_es)
    assert len({(c[1], c[2]) for c in horizontal_es}) == len(horizontal_es)

    # The es_cache horizontal policy carries the halved threshold.
    assert all(c[4] == 16 for c in horizontal_es)

    # Every phase contributes to the returned stats.
    assert len(stats) == 1 + len(docs_repo.calls) + len(vertical) + len(vertical_es) + len(horizontal) + len(horizontal_es)

# Public Function: Verify the settle action settles the docs.
def test_patch_settles_docs() -> None:
    ops, _pp, docs_repo, _dl = _make_ops()
    flags = TypeFlagConfig.from_json({
        "nodes": [["Person", True, False]],
        "edges": [],
    })

    # The settle action settles every patched doc type after both patches.
    ops.patch(flags=flags, actions=('commit', 'settle'))
    assert ("settle", "Person") in docs_repo.calls
    assert docs_repo.calls.index(("settle", "Person")) > docs_repo.calls.index(("patch_es_cache", "Person"))

#================================================================#
# Function Group: Build composition tests                        #
#================================================================#

#==================#
# Class Definition #
#==================#
class FakeBuildupRepo:
    """In-memory fake recording the buildup build calls."""

    # Public Method: Initialize the fake with an empty call log.
    def __init__(self) -> None:
        self.calls = []

    # Public Method: Record a docs build call.
    def build_docs_fields(self, doc_type: str, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("docs", doc_type))
        return PropagationStats(target=f"buildup:{doc_type}", rows_flagged=0)

    # Public Method: Record a links build call.
    def build_links_parentchild(self, doc_type: str, link_type: str, actions: tuple = ('commit',)) -> PropagationStats:
        self.calls.append(("links", doc_type, link_type))
        return PropagationStats(target=f"buildup:{doc_type}:{link_type}", rows_flagged=0)

# Public Function: Verify the build composes docs then canonical link pairs.
def test_build_composes_docs_then_links() -> None:
    ops, _pp, _d, _dl = _make_ops()
    buildup_repo = FakeBuildupRepo()
    ops.buildup_repo = buildup_repo
    flags = TypeFlagConfig.from_json({
        "nodes": [["Person", True, False], ["Course", True, False]],
        "edges": [["Person", "Course", True]],
    })

    # Build over the fields-active types with canonical pairs.
    stats = ops.build(flags=flags, actions=('commit',))

    # The docs build runs per fields-active doc type; the links build runs
    # per canonical pair intersected with the configured contexts.
    assert ("docs", "Person") in buildup_repo.calls and ("docs", "Course") in buildup_repo.calls
    assert ("links", "Course", "Person") in buildup_repo.calls
    assert not any(call[0] == "links" and call[1] == "Exercise" for call in buildup_repo.calls)

    # The docs builds precede the links builds, as in the legacy command.
    kinds = [call[0] for call in buildup_repo.calls]
    assert kinds.index("docs") < kinds.index("links")
    assert len(stats) == len(buildup_repo.calls)

# Public Function: Verify the build without a buildup repository errors.
def test_build_without_buildup_repo_errors() -> None:
    import pytest
    ops, _pp, _d, _dl = _make_ops()
    flags = TypeFlagConfig.from_json({"nodes": [["Person", True, False]], "edges": []})
    with pytest.raises(RuntimeError, match="buildup repository"):
        ops.build(flags=flags, actions=('commit',))
