# scripts/reset_flag_state.py
"""Reset the airflow tracking tables to a fresh-cycle state, then run the
plan + compute chain with row counts after each stage.

This pinpoints where the E2E pipeline breaks: the refresh's flagging, the
cache view materialization, or the buildup build.

Usage (from repo root, project venv active):
    python scripts/reset_flag_state.py           # reset + report current state
    python scripts/reset_flag_state.py --chain   # reset + plan + status + compute + counts
"""
from __future__ import annotations
import os
import subprocess
import sys

from loguru import logger as sysmsg

sysmsg.remove()

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH

glbcfg = GlobalConfig()
db = GraphDB(config=GraphDBConfig.from_file(CONFIG_DB_PATH))
schema = glbcfg.schema_airflow
cache = glbcfg.schema_graph_cache_test
graphsearch = glbcfg.schema_graphsearch_test

CLI = ["/home/dockerhost/dev/graphregistry/.venv.registry/bin/graphregistry"]
ENV = {**os.environ, "PATH": "/home/dockerhost/dev/graphregistry/.venv.registry/bin:" + os.environ.get("PATH", "")}


# Public Function: Execute one write query.
def w(sql: str) -> None:
    db.execute_query_in_shell(engine_name="coresrv", query=sql, query_id="reset_flag_state")


# Public Function: Read one scalar query result.
def r(sql: str) -> list:
    return db.execute_query(engine_name="coresrv", query=sql, query_id="reset_check")


# Public Function: Run one CLI command, streaming output live.
def cli(*args: str) -> int:
    print(f"\n{'=' * 70}\n$ graphregistry {' '.join(args)}\n{'=' * 70}", flush=True)
    return subprocess.run(CLI + list(args), cwd="/data/dockerhost/dev/graphregistry", env=ENV).returncode


# Stage 1: Reset the tracking tables to the fresh-post-sync state. The
# FieldsChanged family carries the checksum and drift columns; the
# ScoresExpired family only tracks dates and expiry.
for table in [
    "Operations_N_Object_T_FieldsChanged",
    "Operations_N_Object_N_Object_T_FieldsChanged",
]:
    w(f"""
        UPDATE {schema}.{table}
           SET checksum_current  = NULL,
               checksum_previous = NULL,
               has_changed       = NULL,
               has_expired       = NULL,
               last_date_cached  = NULL,
               to_process        = 0
         WHERE deleted = 0
    """)
w(f"""
    UPDATE {schema}.Operations_N_Object_T_ScoresExpired
       SET has_expired      = NULL,
           last_date_cached = NULL,
           to_process       = 0
     WHERE deleted = 0
""")

print("Reset complete. FieldsChanged node state after reset:")
for row in r(f"""
    SELECT object_type, COUNT(*), SUM(to_process), SUM(last_date_cached IS NULL)
      FROM {schema}.Operations_N_Object_T_FieldsChanged
     WHERE deleted = 0
  GROUP BY object_type
"""):
    print(f"  {row}")

if "--chain" not in sys.argv:
    print("\n(Re-run with --chain to execute the plan + compute chain.)")
    sys.exit(0)

# Stage 2: Run the plan, then report the refreshed flags.
code = cli("airflow", "plan", "-c", "-l", "10000")
print(f"\n[plan exit code: {code}]")
print("\nFieldsChanged node flags after plan:")
for row in r(f"""
    SELECT object_type, COUNT(*), SUM(to_process)
      FROM {schema}.Operations_N_Object_T_FieldsChanged
     WHERE deleted = 0
  GROUP BY object_type
"""):
    print(f"  {row}")
print("FieldsChanged edge flags after plan:")
for row in r(f"""
    SELECT from_object_type, to_object_type, COUNT(*), SUM(to_process)
      FROM {schema}.Operations_N_Object_N_Object_T_FieldsChanged
     WHERE deleted = 0
  GROUP BY from_object_type, to_object_type
"""):
    print(f"  {row}")

# Stage 3: Show the status command output.
cli("airflow", "status")

# Stage 4: Run the compute, then report the cache materialization counts.
code = cli("kgraph", "compute")
print(f"\n[compute exit code: {code}]")
print("\nCache materialization counts after compute:")
for table in [
    "Data_N_Object_T_PageProfile",
    "Data_N_Object_T_AllFields",
    "Data_N_Object_T_AllFieldsSymmetric",
    "Edges_N_Object_N_Object_T_ParentChildSymmetric",
    "Data_N_Object_T_CalculatedFields",
    "Data_N_Object_N_Object_T_CalculatedFields",
]:
    exists = r(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{cache}' AND table_name = '{table}'")
    if exists and exists[0][0]:
        n = r(f"SELECT COUNT(*), SUM(to_process = 1) FROM {cache}.{table}")
        print(f"  {table:55s} rows={n[0][0]:>7}  to_process={n[0][1]}")
    else:
        print(f"  {table:55s} MISSING")
print("\nRegistry-side page profile rows (the view's INNER JOIN source):")
for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:
    exists = r(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = 'Data_N_Object_T_PageProfile'")
    if exists and exists[0][0]:
        n = r(f"SELECT COUNT(*) FROM {schema_name}.Data_N_Object_T_PageProfile")
        print(f"  {schema_name}.Data_N_Object_T_PageProfile: {n[0][0]} rows")
    else:
        print(f"  {schema_name}.Data_N_Object_T_PageProfile: MISSING")
