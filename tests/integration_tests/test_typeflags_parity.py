# graphregistry/tests/integration_tests/test_typeflags_parity.py
"""Read-only parity test: MySQLTypeFlagsRepository vs the legacy contract.

Runs against the live database configured by config/environment and compares
the typed repository's load().to_json() with an inline oracle reproducing the
retired GraphRegistry.Orchestration.TypeFlags.get_config_json() queries
verbatim. Skips automatically when the database is not reachable.

Deliberately read-only: save and reset mutate the real airflow tables, so
write parity is verified by the unit tests and manual review instead.

Run with:  pytest tests/integration_tests/test_typeflags_parity.py -v
"""
from __future__ import annotations
import pytest
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_typeflags import MySQLTypeFlagsRepository
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig

# Engine name used by the legacy pipeline for all airflow queries.
ENGINE_NAME = "coresrv"

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
def repo(db) -> MySQLTypeFlagsRepository:
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    return MySQLTypeFlagsRepository(db=db, schema_resolver=resolver)

# Public Function: Reproduce the retired legacy get_config_json oracle.
# The legacy GraphRegistry.Orchestration.TypeFlags.get_config_json() read the
# live typeflags tables with the two queries below; they are preserved here
# verbatim as the contract the typed repository must reproduce.
@pytest.fixture(scope="module")
def legacy_json(db) -> dict:
    _, schema_name = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_airflow()
    sql_nodes = f"""
         SELECT t1.object_type, t1.to_process AS process_fields, t2.to_process AS process_scores
           FROM {schema_name}.Operations_N_Object_T_TypeFlags t1
     INNER JOIN {schema_name}.Operations_N_Object_T_TypeFlags t2
          USING (object_type)
          WHERE t1.flag_type = 'fields'
            AND t2.flag_type = 'scores'
            AND (t1.to_process = 1 OR t2.to_process = 1)
    """
    sql_edges = f"""
        SELECT DISTINCT    LEAST(from_object_type, to_object_type) AS from_object_type,
                        GREATEST(from_object_type, to_object_type) AS to_object_type
                   FROM {schema_name}.Operations_N_Object_N_Object_T_TypeFlags
                  WHERE to_process = 1
    """
    nodes = [[row[0], row[1] > 0.5, row[2] > 0.5] for row in db.execute_query(engine_name=ENGINE_NAME, query=sql_nodes, query_id='4bcoW1KT')]
    edges = [[row[0], row[1], True] for row in db.execute_query(engine_name=ENGINE_NAME, query=sql_edges, query_id='9K34TTeQ')]
    return {'nodes': nodes, 'edges': edges}

#================================================================#
# Function Group: Parity tests                                   #
#================================================================#

# Public Function: Verify the repository reproduces the legacy configuration JSON.
def test_load_matches_legacy_get_config_json(repo, legacy_json) -> None:
    assert repo.load().to_json() == legacy_json

# Public Function: Verify the legacy output is a fixed point of the model round-trip.
def test_legacy_json_round_trips_through_model(legacy_json) -> None:
    assert TypeFlagConfig.from_json(legacy_json).to_json() == legacy_json

# Public Function: Verify the loaded model activates the same types as the legacy query.
def test_loaded_scope_matches_legacy(repo, legacy_json) -> None:
    config = repo.load()

    # The active node types per family must match the legacy JSON columns.
    assert sorted(config.active_node_types("fields")) == sorted(
        node_type for node_type, process_fields, _ in legacy_json["nodes"] if process_fields
    )
    assert sorted(config.active_node_types("scores")) == sorted(
        node_type for node_type, _, process_scores in legacy_json["nodes"] if process_scores
    )

    # The active edge families must match the legacy edge rows.
    assert sorted(pair.as_tuple for pair in config.active_edge_pairs()) == sorted(
        (from_type, to_type) for from_type, to_type, _ in legacy_json["edges"]
    )
