#!/usr/bin/env bash
# Sanity check after pruning: display the row counts of the GraphSearch tables.
# Written against the published graphdb.client surface (Environments + schema
# gateways), since the legacy GraphDB facade was removed from graphdb-client.
set -euo pipefail

python - <<'PY'
import re

from graphdb.client import Environments
from graphdb.domain.models.mdl_config import GraphDBConfig
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH
from tabulate import tabulate

# Resolve the same database configuration and engine the pipeline CLI used.
config = GraphDBConfig.from_file(str(CONFIG_DB_PATH))
gateway = Environments(config).get("coresrv")
glbcfg = GlobalConfig()

schema_name = "_1_DEV_" + glbcfg.settings["database"]["schema_names"]["graphsearch_test"]

# Same exclusion scope as the legacy print_database_stats sanity display.
re_exclude = [r".*(MOOC|Lecture|Widget|Startup|Specialisation|StudyPlan|Notebook|Exercise).*"]

# Count rows for every table of the schema that survives the exclusion filter.
tables = gateway.table.get_tables(schema_name=schema_name, include_views=False)
shown = [t for t in tables if not any(re.search(pattern, t) for pattern in re_exclude)]
rows = [[name, f"{gateway.table.count_rows_in_table(schema_name, name):,}"] for name in shown]

print(f"\n📊 Row counts in {schema_name} ({len(shown)} tables):")
print(tabulate(rows, headers=["table", "rows"], tablefmt="fancy_grid", showindex=False))
print()
PY
