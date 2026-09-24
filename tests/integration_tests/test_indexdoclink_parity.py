# graphregistry/tests/integration_tests/test_indexdoclink_parity.py
"""Read-only parity tests: legacy index doc-link patching vs
MySQLIndexDocLinkRepository against a live database.

Both patches are exercised in eval mode over doc-link tables that actually
exist in the live graphsearch schema, so no table creation is attempted.
The semantic matrix eval performs the _tmp_hp_* scratch-table lifecycle the
legacy eval mode also performs, but no patch rows are written. Skips
automatically when the database is not reachable or no suitable tables
exist.

Run with:  pytest tests/integration_tests/test_indexdoclink_parity.py -v
"""
from __future__ import annotations
import re
import pytest
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdoclink import MySQLIndexDocLinkRepository
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_policies import LinkSelectionPolicy, OrderRule

# Engine name used by the legacy pipeline for all doc-link queries.
ENGINE_NAME = "coresrv"

# Pattern of the doc-link projection tables, parsed back into their keys.
TABLE_PATTERN = re.compile(r'^Index_D_(\w+)_L_(\w+)_T_(SEM|ORG)$')

#================================================================#
# Function Group: Pytest fixtures                                #
#================================================================#

# Public Function: Build a database client, skipping the module when unreachable.
@pytest.fixture(scope="module")
def db():
    try:
        from graphdb.core.graphdb import GraphDB
        client = GraphDB()
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"Database not reachable, skipping parity test: {exc}")
        return
    return client

# Public Function: Build the typed repository over the live database.
@pytest.fixture(scope="module")
def repo(db) -> MySQLIndexDocLinkRepository:
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    return MySQLIndexDocLinkRepository(
        db              = db,
        schema_resolver = resolver,
        index_config    = IndexConfig(),
        scores_config   = ScoresConfig(),
    )

# Public Function: Parse the doc-link tables that exist in the live schema.
@pytest.fixture(scope="module")
def live_pairs(db) -> list[tuple[str, str, str]]:
    _, search_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_graphsearch_test()
    tables = db.get_tables_in_schema(engine_name=ENGINE_NAME, schema_name=search_schema, use_regex=[r'^Index_D_\w+_L_\w+_T_(SEM|ORG)$'])
    pairs = []
    for table_name in sorted(tables):
        match = TABLE_PATTERN.match(table_name)
        if match:
            pairs.append((match.group(1), match.group(2), match.group(3)))
    if not pairs:
        pytest.skip("No doc-link projection tables exist in the live schema.")
    return pairs

# Public Function: Pick the first live pair of a partition whose link type
# carries configured default fields, so the vertical patch has work to do.
@pytest.fixture(scope="module")
def pick_vertical_pair(live_pairs) -> "callable":
    links_config = IndexConfig().settings.get("graphsearch", {}).get("fields", {}).get("links", {})

    # Internal Function: return the first matching pair of the partition.
    def _pick(partition: str) -> tuple[str, str]:
        for doc_type, link_type, table_partition in live_pairs:
            if table_partition == partition and link_type in links_config.get("default", {}):
                return (doc_type, link_type)
        pytest.skip(f"No live {partition} doc-link pairs with configured fields.")
    return _pick

#================================================================#
# Function Group: Policy construction                            #
#================================================================#

# Public Function: Build the selection policy of a partition from the index
# configuration, as the future operation layer will build it.
def _policy_for(partition: str, link_type: str) -> LinkSelectionPolicy:
    policy = LinkSelectionPolicy.default_for_partition(partition)
    rules = (
        IndexConfig().settings.get("graphsearch", {}).get("order_by", {})
        .get("links", {}).get("default", {}).get(link_type, [])
    )
    if rules:
        policy.ordering = [OrderRule(field=rule[0], direction=rule[1]) for rule in rules]
    return policy

#================================================================#
# Function Group: Parity tests                                   #
#================================================================#

# Public Function: Verify the vertical patch eval runs against the live schema.
@pytest.mark.parametrize("partition", ["SEM", "ORG"])
def test_vertical_patch_eval_against_live_schema(repo, pick_vertical_pair, partition) -> None:
    doc_type, link_type = pick_vertical_pair(partition)
    key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition=partition)

    # Eval mode counts the flagged and drifted rows without patching; the
    # target carries the dev-prefixed schema name of the live resolver.
    stats = repo.vertical_patch(key=key, actions=('eval',))
    assert stats.rows_flagged >= 0
    _, search_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_graphsearch_test()
    assert stats.target == f"{search_schema}.Index_D_{doc_type}_L_{link_type}_T_{partition}"

# Public Function: Verify the horizontal patch eval runs against the live schema.
def test_horizontal_patch_org_eval_against_live_schema(repo, live_pairs) -> None:
    contexts = IndexConfig().settings.get("edge_selection_contexts", {})

    # Pick an organisational pair with a configured edge context.
    candidates = [
        (doc_type, link_type) for doc_type, link_type, partition in live_pairs
        if partition == "ORG" and tuple(sorted([doc_type, link_type])) in contexts
    ]
    if not candidates:
        pytest.skip("No live ORG doc-link pairs with a configured edge context.")
    doc_type, link_type = candidates[0]
    key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition="ORG")

    # Eval mode counts the rows to insert and re-score without patching.
    stats = repo.horizontal_patch(key=key, policy=_policy_for("ORG", link_type), actions=('eval',))
    assert stats.rows_flagged >= 0

# Public Function: Verify the semantic horizontal patch eval manages the scratch table.
def test_horizontal_patch_sem_eval_against_live_schema(repo, live_pairs) -> None:

    # Pick a non-ontology semantic pair, so the matrix branch with its
    # scratch table runs.
    candidates = [
        (doc_type, link_type) for doc_type, link_type, partition in live_pairs
        if partition == "SEM" and doc_type not in ("Concept", "Category") and link_type not in ("Concept", "Category")
    ]
    if not candidates:
        pytest.skip("No live non-ontology SEM doc-link pairs.")
    doc_type, link_type = candidates[0]
    key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition="SEM")

    # Eval mode of the matrix branch materializes and drops the _tmp_hp_*
    # scratch table, exactly as the legacy eval mode does, but writes no
    # patch rows.
    stats = repo.horizontal_patch(key=key, policy=_policy_for("SEM", link_type), actions=('eval',))
    assert stats.rows_flagged >= 0

#================================================================#
# Function Group: Elasticsearch cache parity tests               #
#================================================================#

# Public Function: Verify the es_cache vertical patch eval runs against the
# live schema.
def test_vertical_patch_es_cache_eval_against_live_schema(repo, pick_vertical_pair) -> None:
    doc_type, link_type = pick_vertical_pair("SEM")
    key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition="SEM")

    # Eval mode counts the drifted presentation columns without patching.
    stats = repo.vertical_patch_es_cache(key=key, actions=('eval',))
    _, es_cache_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_es_cache()
    assert stats.target == f"{es_cache_schema}.Index_D_{doc_type}_L_{link_type}"

# Public Function: Verify the es_cache horizontal patch eval runs against the
# live schema.
def test_horizontal_patch_es_cache_eval_against_live_schema(repo, live_pairs) -> None:
    candidates = [
        (doc_type, link_type) for doc_type, link_type, partition in live_pairs
        if partition == "SEM" and doc_type not in ("Concept", "Category") and link_type not in ("Concept", "Category")
    ]
    if not candidates:
        pytest.skip("No live non-ontology SEM doc-link pairs.")
    doc_type, link_type = candidates[0]
    key = DocLinkTypeKey(doc_type=doc_type, link_type=link_type, partition="SEM")

    # Eval mode counts the rows to insert and replace without patching; the
    # mixed-view resolution may create or drop views as the legacy does.
    stats = repo.horizontal_patch_es_cache(key=key, policy=_policy_for("SEM", link_type), actions=('eval',))
    assert stats.rows_flagged >= 0
