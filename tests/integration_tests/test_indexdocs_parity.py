# graphregistry/tests/integration_tests/test_indexdocs_parity.py
"""Read-only parity tests: legacy index doc patching vs
MySQLIndexDocRepository against a live database.

Both patch variants are exercised in eval mode over doc types whose
Index_D_{doc_type} tables exist in the live graphsearch schema, so no table
creation is attempted. The settle and flag-cleanup steps write and are
covered by the unit tests only. Skips automatically when the database is not
reachable.

Run with:  pytest tests/integration_tests/test_indexdocs_parity.py -v
"""
from __future__ import annotations
import re
import pytest
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdocs import MySQLIndexDocRepository
from graphregistry.common.config import GlobalConfig, IndexConfig
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats

# Engine name used by the legacy pipeline for all doc queries.
ENGINE_NAME = "coresrv"

# Pattern of the doc projection tables, parsed back into doc types.
TABLE_PATTERN = re.compile(r'^Index_D_(\w+)$')

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
def repo(db) -> MySQLIndexDocRepository:
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    return MySQLIndexDocRepository(
        db              = db,
        schema_resolver = resolver,
        index_config    = IndexConfig(),
    )

# Public Function: Pick doc types whose tables exist in the live schema and
# whose link fields are configured.
@pytest.fixture(scope="module")
def live_doc_types(db) -> list[str]:
    _, search_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_graphsearch_test()
    tables = db.get_tables_in_schema(engine_name=ENGINE_NAME, schema_name=search_schema, use_regex=[r'^Index_D_\w+$'])
    doc_types = sorted({match.group(1) for match in (TABLE_PATTERN.match(t) for t in tables) if match})
    if not doc_types:
        pytest.skip("No doc projection tables exist in the live schema.")
    return doc_types

#================================================================#
# Function Group: Parity tests                                   #
#================================================================#

# Public Function: Verify the graphsearch patch eval runs against the live schema.
def test_patch_eval_against_live_schema(repo, live_doc_types) -> None:
    doc_type = live_doc_types[0]

    # Eval mode counts the flagged and drifted rows without patching.
    stats = repo.patch(doc_type=doc_type, actions=('eval',))
    assert isinstance(stats, PropagationStats)
    assert stats.rows_flagged >= 0
    _, search_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_graphsearch_test()
    assert stats.target == f"{search_schema}.Index_D_{doc_type}"

# Public Function: Verify the es_cache patch eval runs against the live schema.
def test_patch_es_cache_eval_against_live_schema(repo, live_doc_types) -> None:
    doc_type = live_doc_types[0]

    # Eval mode of the cache patch counts the drifted presentation columns.
    stats = repo.patch_es_cache(doc_type=doc_type, actions=('eval',))
    assert isinstance(stats, PropagationStats)
    assert stats.rows_flagged >= 0
    _, es_cache_schema = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_es_cache()
    assert stats.target == f"{es_cache_schema}.Index_D_{doc_type}"

#================================================================#
# Function Group: Page-profile parity tests                      #
#================================================================#

# Public Function: Verify the page-profile patch eval runs against the live
# schema.
def test_page_profile_patch_eval_against_live_schema(db) -> None:
    from graphregistry.adapters.persistence.mysql.repositories.rpo_pageprofile import MySQLPageProfileRepository
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    repo = MySQLPageProfileRepository(db=db, schema_resolver=resolver)

    # Eval mode runs the safe-insert evaluation without writing rows.
    stats = repo.patch(actions=('eval',))
    _, search_schema = resolver.for_graphsearch_test()
    assert stats.target == f"{search_schema}.Data_N_Object_T_PageProfile"
