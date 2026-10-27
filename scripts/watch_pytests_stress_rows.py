# graphregistry/scripts/watch_pytests_stress_rows.py
"""Watch live/soft-deleted stress-test row counts in a registry test schema.

A `watch`-style poller for the e2e stress test data: every interval it queries
the node table of a registry schema and shows the row counts grouped by
``record_deleted`` for rows whose object id matches a prefix (default the
``E2E-STRESS-`` ids written by ``tests/end2end_tests/test_api_stress_save_many.py``).

Credentials are read from the platform's own GraphDB client config (by default
``config/environment/config_graphdb.yml``), so no passwords are passed on the
command line or stored in the script.

Usage:
    python scripts/watch_pytests_stress_rows.py                     # watch mode
    python scripts/watch_pytests_stress_rows.py --once              # single sample
    python scripts/watch_pytests_stress_rows.py --schema _0_PYTESTS_graph_registry
"""
from __future__ import annotations
import argparse
import sys
import time
from pathlib import Path
import pymysql
import yaml

# Default schema watched by the script: the pytest isolation schema produced by
# temp_test_global_config() in the e2e stress test.
DEFAULT_SCHEMA = "_0_PYTESTS_graph_registry"
DEFAULT_PREFIX = "E2E-STRESS-"
DEFAULT_ENGINE = "coresrv"
CLEAR_SCREEN = "\033[2J\033[H"

# Internal Function: Parse the command-line arguments for the watcher.
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch stress-test row counts in a registry schema.")
    parser.add_argument("--config", default="config/environment/config_graphdb.yml", help="GraphDB client config YAML.")
    parser.add_argument("--engine", default=DEFAULT_ENGINE, help="Engine name; reads environments.<engine>_env.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Registry schema (database) to watch.")
    parser.add_argument("--table", default="Nodes_N_Object", help="Node table to watch.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="object_id LIKE prefix identifying stress rows.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between samples.")
    parser.add_argument("--once", action="store_true", help="Take one sample and exit (no screen clearing).")
    return parser.parse_args()

# Internal Function: Read the connection settings for one engine from the GraphDB config.
def _connection_settings(config_path: Path, engine: str) -> dict:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    environments = config.get("environments", {})
    settings = environments.get(f"{engine}_env")
    if settings is None:
        available = ", ".join(sorted(environments)) or "none"
        raise SystemExit(f"Engine '{engine}' not found in {config_path}. Available: {available}")
    return settings

# Internal Function: Open a database connection using the config settings,
# enabling SSL only when the config declares a CA certificate.
def _connect(settings: dict) -> pymysql.Connection:
    connect_kwargs = {
        "host"     : settings["host_address"],
        "port"     : int(settings["port"]),
        "user"     : settings["username"],
        "password" : settings["password"],
    }
    ssl_options = settings.get("ssl") or {}
    if ssl_options.get("ca"):
        connect_kwargs["ssl"] = {"ca": ssl_options["ca"]}
    return pymysql.connect(**connect_kwargs)

# Internal Function: Query the live and soft-deleted row counts for the prefix.
def _sample_counts(connection: pymysql.Connection, schema: str, table: str, prefix: str) -> dict[int, int]:
    query = (
        f"SELECT record_deleted, COUNT(*) FROM `{schema}`.`{table}` "
        f"WHERE object_id LIKE %s GROUP BY record_deleted"
    )
    with connection.cursor() as cursor:
        cursor.execute(query, (f"{prefix}%",))
        return {int(record_deleted): int(count) for record_deleted, count in cursor.fetchall()}

# Internal Function: Render one watch frame with counts, totals and deltas.
def _render_frame(counts: dict[int, int], schema: str, prefix: str, previous: dict[int, int] | None, started: float) -> str:
    live = counts.get(0, 0)
    deleted = counts.get(1, 0)
    total = live + deleted
    lines = [
        f"Watching `{schema}` for object_id LIKE '{prefix}%' - {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "-" * 64,
        f"record_deleted = 0 (live)          : {live:>8,}",
        f"record_deleted = 1 (soft-deleted)  : {deleted:>8,}",
        f"total matching rows                : {total:>8,}",
    ]
    if previous is not None:
        delta_live = live - previous.get(0, 0)
        delta_deleted = deleted - previous.get(1, 0)
        lines.append(f"delta since last sample            : live {delta_live:+,}, deleted {delta_deleted:+,}")
    lines.append(f"watching for {time.time() - started:,.0f}s - Ctrl+C to stop")
    return "\n".join(lines)

# Public Function: Poll the schema and render counts until interrupted.
def main() -> None:
    args = _parse_args()
    settings = _connection_settings(Path(args.config), args.engine)
    previous: dict[int, int] | None = None
    started = time.time()

    # Reconnect on every sample so the watcher survives server restarts and
    # idle timeouts without accumulating connection state.
    while True:
        try:
            connection = _connect(settings)
            try:
                counts = _sample_counts(connection, args.schema, args.table, args.prefix)
            finally:
                connection.close()
            frame = _render_frame(counts, args.schema, args.prefix, previous, started)
            print(frame if args.once else CLEAR_SCREEN + frame, flush=True)
            previous = counts
            if args.once:
                return
        except pymysql.MySQLError as error:
            print(f"Connection/query failed ({error}); retrying in {args.interval:.0f}s", flush=True)
        except KeyboardInterrupt:
            print("\nStopped.", flush=True)
            return
        time.sleep(args.interval)

# Entry point for direct script execution.
if __name__ == "__main__":
    main()
