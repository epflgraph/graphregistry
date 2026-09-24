# graphregistry/tests/integration_tests/test_typeflags_parity.py
"""Read-only parity test: legacy TypeFlags vs MySQLTypeFlagsRepository.

Runs against the live database configured by config/environment and compares
the legacy GraphRegistry.Orchestration.TypeFlags.get_config_json() output with
the typed repository's load().to_json(). Skips automatically when the
database is not reachable.

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

# Public Function: Load the legacy typeflags configuration as the oracle.
@pytest.fixture(scope="module")
def legacy_json():
    try:
        from graphregistry.application.core.cor_registry import GraphRegistry
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"Legacy cor_registry could not be imported: {exc}")
        return
    return GraphRegistry().orchestrator.typeflags.get_config_json()

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
