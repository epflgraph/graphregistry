# graphregistry/tests/integration_tests/test_changetracking_parity.py
"""Read-only parity tests: legacy FieldsChanged/ScoresExpired vs
MySQLChangeTrackingRepository against a live database.

The tests pick a few tracking rows dynamically, read them through the legacy
orchestrator and through the typed repository, and compare the mapped values.
Skips automatically when the database is not reachable or the tables are
empty. No writes are issued.

Run with:  pytest tests/integration_tests/test_changetracking_parity.py -v
"""
from __future__ import annotations
from datetime import datetime
import pytest
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_changetracking import MySQLChangeTrackingRepository
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey

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
def repo(db) -> MySQLChangeTrackingRepository:
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    return MySQLChangeTrackingRepository(db=db, schema_resolver=resolver, global_config=GlobalConfig())

# Public Function: Load the legacy orchestrator as the comparison oracle.
@pytest.fixture(scope="module")
def legacy():
    try:
        from graphregistry.application.core.cor_registry import GraphRegistry
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"Legacy cor_registry could not be imported: {exc}")
        return
    return GraphRegistry().orchestrator

# Public Function: Sample a few node keys from the live FieldsChanged table.
@pytest.fixture(scope="module")
def node_keys(db) -> list[NodeKey]:
    _, schema_name = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_airflow()
    rows = db.execute_query(
        engine_name = ENGINE_NAME,
        query       = f"SELECT object_type, object_id FROM {schema_name}.Operations_N_Object_T_FieldsChanged LIMIT 3",
        query_id    = 'changetracking-parity-node-keys',
    )
    if not rows:
        pytest.skip("FieldsChanged table is empty; nothing to compare.")
    return [NodeKey(object_type=row[0], object_id=row[1]) for row in rows]

# Public Function: Sample a few edge keys from the live FieldsChanged table.
@pytest.fixture(scope="module")
def edge_keys(db) -> list[EdgeKey]:
    _, schema_name = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_airflow()
    rows = db.execute_query(
        engine_name = ENGINE_NAME,
        query       = f"SELECT from_object_type, from_object_id, to_object_type, to_object_id, context FROM {schema_name}.Operations_N_Object_N_Object_T_FieldsChanged LIMIT 3",
        query_id    = 'changetracking-parity-edge-keys',
    )
    if not rows:
        pytest.skip("Edge FieldsChanged table is empty; nothing to compare.")
    return [EdgeKey(from_object_type=row[0], from_object_id=row[1], to_object_type=row[2], to_object_id=row[3], context=row[4]) for row in rows]

#================================================================#
# Function Group: Parity tests                                   #
#================================================================#

# Public Function: Verify node record reads map the live table rows.
def test_get_node_record_maps_live_rows(repo, db, node_keys) -> None:
    _, schema_name = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig()).for_airflow()

    # The legacy FieldsChanged.get oracle is broken for node keys: its table
    # selection sends 2-tuples to the edge table while selecting node columns,
    # which fails with an unknown-column error (discovered 2026-09-23). The
    # oracle is therefore a direct read of the node table.
    for key in node_keys:
        rows = db.execute_query(
            engine_name = ENGINE_NAME,
            query       = (
                f"SELECT object_type, object_id, checksum_current, checksum_previous, has_changed, "
                f"last_date_cached, has_expired, to_process, deleted "
                f"FROM {schema_name}.Operations_N_Object_T_FieldsChanged "
                f"WHERE object_type = '{key.object_type}' AND object_id = '{key.object_id}'"
            ),
            query_id    = 'changetracking-parity-node-direct',
        )
        record = repo.get_node_record(key=key)
        if not rows:
            assert record is None
            continue
        assert record is not None, f"Repository missed record for {key}"

        # Compare the mapped fields against the raw row, normalising the date
        # column the same way the adapter does.
        row = rows[0]
        assert record.key.object_type == row[0] and record.key.object_id == row[1]
        assert (record.checksums.current.value if record.checksums.current else None) == row[2]
        assert (record.checksums.previous.value if record.checksums.previous else None) == row[3]
        expected_date = row[5].date() if isinstance(row[5], datetime) else row[5]
        assert record.state.last_date_cached == expected_date
        assert record.state.has_expired == bool(row[6])
        assert record.state.to_process == bool(row[7])
        assert record.deleted == bool(row[8])

# Public Function: Verify edge record reads match the legacy FieldsChanged get.
def test_get_edge_record_matches_legacy(repo, legacy, edge_keys) -> None:
    for key in edge_keys:

        # Read the record first: the legacy get requires a has_expired or
        # older_than filter, so the filter is aligned with the record's own
        # state to make the comparison meaningful.
        record = repo.get_edge_record(key=key)
        has_expired_filter = bool(record.state.has_expired) if record is not None else False

        # The legacy get accepts edge keys only as 4-tuples without context;
        # the context is matched on the returned rows instead.
        legacy_rows = legacy.fieldschanged.get(
            (key.from_object_type, key.from_object_id, key.to_object_type, key.to_object_id),
            has_expired=has_expired_filter,
        )
        matching = [row for row in (legacy_rows or []) if row[4] == key.context]
        if record is None:
            assert not matching
            continue
        assert matching, f"Legacy has no row for {key}"
        row = matching[0]

        # Compare the mapped fields against the legacy tuple.
        assert record.key == key
        assert (record.checksums.current.value if record.checksums.current else None) == row[5]
        assert (record.checksums.previous.value if record.checksums.previous else None) == row[6]
        assert record.state.has_expired == bool(row[9])
        assert record.state.to_process == bool(row[10])

# Public Function: Verify score-expiry reads match the legacy ScoresExpired get.
def test_get_score_expiry_record_matches_legacy(repo, legacy, node_keys) -> None:
    for key in node_keys:
        legacy_rows = legacy.scoresexpired.get((key.object_type, key.object_id), has_expired=False)
        record = repo.get_score_expiry_record(key=key)
        if not legacy_rows:
            assert record is None
            continue
        assert record is not None, f"Repository missed score-expiry record for {key}"
        assert record.state.to_process == bool(legacy_rows[0][4])
