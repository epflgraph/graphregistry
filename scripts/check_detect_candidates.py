# scripts/check_detect_candidates.py
"""Check whether 'ai detect-concepts' would find candidates on the current
E2E database: raw_text population, concept-edge presence, and the exact
find_keys_with_no_concepts query result.

Usage (from repo root, project venv active):
    python scripts/check_detect_candidates.py
"""
from __future__ import annotations

from loguru import logger as sysmsg

sysmsg.remove()

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH

glbcfg = GlobalConfig()
db = GraphDB(config=GraphDBConfig.from_file(CONFIG_DB_PATH))
registry = glbcfg.schema_registry
airflow = glbcfg.schema_airflow

print("=" * 70)
print("1. Nodes_N_Object raw_text population (registry schema)")
print("=" * 70)
for row in db.execute_query(
    engine_name="coresrv",
    query=f"""
        SELECT object_type, COUNT(*), SUM(raw_text IS NOT NULL), SUM(raw_text != '')
          FROM {registry}.Nodes_N_Object
         WHERE record_deleted = 0
      GROUP BY object_type
    """,
    query_id="rawtext_check",
):
    print(f"  {row}")

print()
print("=" * 70)
print("2. Concept detection edges (registry schema)")
print("=" * 70)
for row in db.execute_query(
    engine_name="coresrv",
    query=f"""
        SELECT c.object_type, COUNT(*)
          FROM {registry}.Edges_N_Object_N_Concept_T_ConceptDetection c
         WHERE c.record_deleted = 0
      GROUP BY c.object_type
    """,
    query_id="concept_edges_check",
):
    print(f"  {row}")

print()
print("=" * 70)
print("3. The exact find_keys_with_no_concepts query (as detect-concepts runs it)")
print("=" * 70)
rows = db.execute_query(
    engine_name="coresrv",
    query=f"""
        SELECT DISTINCT n.object_type, n.object_id
          FROM {registry}.Nodes_N_Object n
          LEFT JOIN {registry}.Edges_N_Object_N_Concept_T_ConceptDetection c
                ON n.object_type    = c.object_type
               AND n.object_id      = c.object_id
               AND c.record_deleted = 0
          INNER JOIN {airflow}.Operations_N_Object_T_FieldsChanged fc
                ON n.object_type = fc.object_type
               AND n.object_id   = fc.object_id
          INNER JOIN {airflow}.Operations_N_Object_T_TypeFlags tf
                ON n.object_type = tf.object_type
         WHERE n.object_type IN ('Course', 'Person', 'Publication', 'Unit')
           AND n.object_id   LIKE '%'
           AND n.raw_text    IS NOT NULL
           AND n.record_deleted = 0
           AND c.concept_id  IS NULL
           AND tf.to_process = 1
      ORDER BY n.object_type, n.object_id
    """,
    query_id="detect_candidates_check",
)
print(f"  candidates: {len(rows)}")
for row in rows[:10]:
    print(f"    {row}")
if len(rows) > 10:
    print(f"    ... and {len(rows) - 10} more")
