# scripts/diagnose_refresh.py
"""Read-only diagnostic for the refresh flag chain.

Dumps the FieldsChanged/ScoresExpired table states, the typeflags, and
tests the refresh selection WHERE clauses directly, so the exact point
where the flag chain breaks is visible. No writes are issued.

Usage (from repo root, project venv active):
    python scripts/diagnose_refresh.py
"""
from __future__ import annotations
from loguru import logger as sysmsg

# Silence the graphdb TRACE spam so only the diagnostic output shows.
sysmsg.remove()

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH

glbcfg = GlobalConfig()
db = GraphDB(config=GraphDBConfig.from_file(CONFIG_DB_PATH))
schema = glbcfg.schema_airflow


# Public Function: Run a read-only query and return the rows.
def q(sql: str) -> list[tuple]:
    return db.execute_query(engine_name="coresrv", query=sql, query_id="diagnose_refresh")


print("=" * 70)
print("1. FieldsChanged node table: per-type flag state")
print("=" * 70)
for row in q(f"""
    SELECT object_type,
           COUNT(*)                                          AS n_rows,
           SUM(to_process)                                  AS n_to_process,
           SUM(has_changed = 1)                              AS n_has_changed_1,
           SUM(has_changed = 0)                              AS n_has_changed_0,
           SUM(has_changed IS NULL)                          AS n_has_changed_null,
           SUM(has_expired = 1)                              AS n_has_expired_1,
           SUM(last_date_cached IS NULL)                    AS n_never_cached,
           SUM(checksum_current IS NULL)                     AS n_checksum_null,
           SUM(deleted = 0)                                  AS n_alive
      FROM {schema}.Operations_N_Object_T_FieldsChanged
  GROUP BY object_type
"""):
    print(row)

print()
print("=" * 70)
print("2. FieldsChanged edge table: per-pair flag state")
print("=" * 70)
for row in q(f"""
    SELECT from_object_type, to_object_type,
           COUNT(*)                                          AS n_rows,
           SUM(to_process)                                  AS n_to_process,
           SUM(last_date_cached IS NULL)                    AS n_never_cached,
           SUM(deleted = 0)                                  AS n_alive
      FROM {schema}.Operations_N_Object_N_Object_T_FieldsChanged
  GROUP BY from_object_type, to_object_type
"""):
    print(row)

print()
print("=" * 70)
print("3. ScoresExpired table: per-type flag state")
print("=" * 70)
for row in q(f"""
    SELECT object_type,
           COUNT(*)                                          AS n_rows,
           SUM(to_process)                                  AS n_to_process,
           SUM(last_date_cached IS NULL)                    AS n_never_cached,
           SUM(deleted = 0)                                  AS n_alive
      FROM {schema}.Operations_N_Object_T_ScoresExpired
  GROUP BY object_type
"""):
    print(row)

print()
print("=" * 70)
print("4. Node typeflags table contents")
print("=" * 70)
for row in q(f"SELECT object_type, flag_type, to_process FROM {schema}.Operations_N_Object_T_TypeFlags ORDER BY object_type, flag_type"):
    print(row)

print()
print("=" * 70)
print("5. Edge typeflags table contents")
print("=" * 70)
for row in q(f"SELECT from_object_type, to_object_type, to_process FROM {schema}.Operations_N_Object_N_Object_T_TypeFlags ORDER BY from_object_type, to_object_type"):
    print(row)

print()
print("=" * 70)
print("6. Refresh selection test: which node rows would the WHERE match?")
print("   (has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL)")
print("=" * 70)
for row in q(f"""
    SELECT object_type,
           SUM(has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL) AS n_would_match,
           COUNT(*)                                                            AS n_total
      FROM {schema}.Operations_N_Object_T_FieldsChanged
     WHERE deleted = 0
  GROUP BY object_type
"""):
    print(row)

print()
print("=" * 70)
print("7. Refresh selection test: which edge rows would the WHERE match?")
print("=" * 70)
for row in q(f"""
    SELECT from_object_type, to_object_type,
           SUM(has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL) AS n_would_match,
           COUNT(*)                                                            AS n_total
      FROM {schema}.Operations_N_Object_N_Object_T_FieldsChanged
     WHERE deleted = 0
  GROUP BY from_object_type, to_object_type
"""):
    print(row)

print()
print("=" * 70)
print("8. Has_changed derivation state (current vs previous checksums)")
print("=" * 70)
for row in q(f"""
    SELECT
           SUM(checksum_current IS NOT NULL AND checksum_previous IS NULL)     AS n_current_only,
           SUM(checksum_current IS NULL AND checksum_previous IS NOT NULL)     AS n_previous_only,
           SUM(checksum_current IS NOT NULL AND checksum_previous IS NOT NULL) AS n_both,
           SUM(checksum_current IS NULL AND checksum_previous IS NULL)         AS n_neither
      FROM {schema}.Operations_N_Object_T_FieldsChanged
"""):
    print(row)
