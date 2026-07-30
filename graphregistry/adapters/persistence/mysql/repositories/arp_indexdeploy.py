# graphregistry/adapters/persistence/mysql/repositories/arp_indexdeploy.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from graphregistry.domain.repositories.rpo_indexdeploy import (
    IndexDeployRepository,
    IndexTableSpec,
)
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.common.logger import GraphLogger
import rich

if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.common.config import GlobalConfig


class MySQLIndexDeploy(IndexDeployRepository):
    """
    MySQL adapter for deploying GraphSearch index tables from a source
    environment (e.g. graphsearch_test on coresrv) to a target environment
    (e.g. graphsearch_prod on xaas_prod).

    The SQL templates live under database/queries/patching and are loaded via
    graphregistry.common.dbstruct.sql_queries_paths.
    """

    # Columns that should never be copied or compared during a sync.
    META_COLUMNS = frozenset(
        {
            "row_id",
            "to_process",
            "deleted",
            "last_date_cached",
            "checksum_val",
        }
    )

    def __init__(self, db: GraphDB, glbcfg: GlobalConfig) -> None:
        self.db = db
        self.glbcfg = glbcfg
        self.msg = GraphLogger()

    def _log(self, emoji: str, color: str, message: str) -> None:
        """Print a coloured, emoji-prefixed status message."""
        rich.print(f"[{color}]{emoji} {message}[/{color}]")

    # --------------------------
    # High-level orchestration
    # --------------------------

    def create_patch(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...] = (),
        schema_overrides: dict[str, str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """
        Evaluate the diff for every table in table_specs.
        Returns {table_name: {"insert": n, "delete": n, "replace": n}}.
        """
        results: dict[str, dict[str, int]] = {}
        for spec in table_specs:
            table_name = self._table_name(spec)
            self._log("🔍", "cyan", f"Evaluating patch for {table_name}")
            results[table_name] = self.count_table_changes(
                source_engine, target_engine, spec, schema_overrides
            )
        return results

    def apply_patch(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...] = (),
        schema_overrides: dict[str, str] | None = None,
    ) -> None:
        """
        Apply the sync to the target engine.
        With actions containing 'commit' it executes replace/insert/delete.
        """
        if "commit" not in actions:
            self._log("⚠️", "yellow", "No 'commit' in actions; patch not applied.")
            return

        for spec in table_specs:
            self.commit_table_changes(
                source_engine, target_engine, spec, actions, schema_overrides
            )

    def generate_patch_files(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...] = (),
        schema_overrides: dict[str, str] | None = None,
    ) -> Path:
        """
        Generate forward patch SQL files and corresponding rollback SQL files.

        Files are written under:
            <index_patch_path>/YYYY-MM-DD_hh-mm/{patch,rollback}/

        Returns the path to the generated patch directory.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        patch_root = self.glbcfg.index_patch_path / timestamp
        patch_dir = patch_root / "patch"
        rollback_dir = patch_root / "rollback"

        self._log("📁", "blue", f"Generating patch files in {patch_root}")
        self._log("🔧", "blue", f"Source engine: {source_engine}")
        self._log("🔧", "blue", f"Target engine: {target_engine}")
        if schema_overrides:
            for key, value in schema_overrides.items():
                self._log("🔧", "blue", f"Schema override: {key} -> {value}")

        patch_dir.mkdir(parents=True, exist_ok=True)
        rollback_dir.mkdir(parents=True, exist_ok=True)

        for spec in table_specs:
            self._generate_table_patch_files(
                source_engine,
                target_engine,
                spec,
                patch_dir,
                rollback_dir,
                actions,
                schema_overrides,
            )

        self._log("✅", "green", f"Patch files written to {patch_root}")
        return patch_root

    # --------------------------
    # Per-table operations
    # --------------------------

    def count_table_changes(
        self,
        source_engine: str,
        target_engine: str,
        spec: IndexTableSpec,
        schema_overrides: dict[str, str] | None = None,
    ) -> dict[str, int]:
        """Return insert/delete/replace counts for a single table."""
        source_schema, target_schema = self._resolve_schemas(
            source_engine, target_engine, spec, schema_overrides
        )
        table_name = self._table_name(spec)
        key_columns = self._key_columns(spec.table_type)
        payload_columns = self._payload_columns(
            source_engine, source_schema, target_engine, target_schema, table_name, key_columns
        )

        kwargs = self._base_kwargs(
            source_schema,
            target_schema,
            spec,
            data_columns=self._build_data_columns(payload_columns),
        )

        counts: dict[str, int] = {"insert": 0, "delete": 0, "replace": 0}

        # Insert / delete counts
        if spec.table_type == "page_profile":
            for op in ("insert", "delete"):
                query = self._render("eval", f"page_profile_{op}_count", **kwargs)
                rows = self.db.execute_query(engine_name=source_engine, query=query)
                counts[op] = sum(row[1] for row in rows) if rows else 0
        else:
            plural = f"{spec.table_type}s"  # doc -> docs, doclink -> doclinks
            for op in ("insert", "delete"):
                query = self._render("eval", f"index_{plural}_{op}_count", **kwargs)
                rows = self.db.execute_query(engine_name=source_engine, query=query)
                counts[op] = rows[0][0] if rows else 0

        # Replace count: there is no dedicated template yet.
        # TODO: add eval/index_*_replace_count.sql templates, or compute via
        # a COUNT(*) wrapper around the replace query.
        counts["replace"] = 0

        self._log(
            "📊",
            "cyan",
            f"{table_name}: insert={counts['insert']}, delete={counts['delete']}, replace={counts['replace']}",
        )

        return counts

    def commit_table_changes(
        self,
        source_engine: str,
        target_engine: str,
        spec: IndexTableSpec,
        actions: tuple[str, ...],
        schema_overrides: dict[str, str] | None = None,
    ) -> None:
        """Execute replace, insert, and delete commit queries for one table."""
        source_schema, target_schema = self._resolve_schemas(
            source_engine, target_engine, spec, schema_overrides
        )
        table_name = self._table_name(spec)
        key_columns = self._key_columns(spec.table_type)
        payload_columns = self._payload_columns(
            source_engine, source_schema, target_engine, target_schema, table_name, key_columns
        )

        kwargs = self._base_kwargs(
            source_schema,
            target_schema,
            spec,
            data_columns=self._build_data_columns(payload_columns),
            update_set_clause=self._build_update_set_clause(payload_columns),
            changed_condition=self._build_changed_condition(payload_columns),
        )

        self._log("🚀", "magenta", f"Applying patch to {table_name}")

        # 1. Replace rows that exist in both but differ
        self._log("🔁", "magenta", f"{table_name}: replacing changed rows")
        if spec.table_type == "page_profile":
            replace_query_name = "page_profile_replace_rows"
        else:
            plural = f"{spec.table_type}s"
            replace_query_name = f"index_{plural}_replace_rows"

        replace_query = self._render("commit", replace_query_name, **kwargs)
        self.db.execute_query_in_shell(
            engine_name=target_engine, query=replace_query
        )

        # 2. Insert rows that exist only in source
        self._log("➕", "magenta", f"{table_name}: inserting new rows")
        if spec.table_type == "page_profile":
            insert_query_name = "page_profile_insert_rows"
        else:
            plural = f"{spec.table_type}s"
            insert_query_name = f"index_{plural}_insert_rows"

        insert_query = self._render("commit", insert_query_name, **kwargs)
        self.db.execute_query_in_shell(
            engine_name=target_engine, query=insert_query
        )

        # 3. Delete rows that exist only in target
        self._log("➖", "magenta", f"{table_name}: deleting obsolete rows")
        if spec.table_type == "page_profile":
            delete_query_name = "page_profile_delete_rows"
        else:
            plural = f"{spec.table_type}s"
            delete_query_name = f"index_{plural}_delete_rows"

        delete_query = self._render("commit", delete_query_name, **kwargs)
        self.db.execute_query_in_shell(
            engine_name=target_engine, query=delete_query
        )

        self._log("✅", "green", f"Finished applying patch to {table_name}")

    # --------------------------
    # Patch file generation
    # --------------------------

    def _generate_table_patch_files(
        self,
        source_engine: str,
        target_engine: str,
        spec: IndexTableSpec,
        patch_dir: Path,
        rollback_dir: Path,
        actions: tuple[str, ...],
        schema_overrides: dict[str, str] | None = None,
    ) -> None:
        """Generate forward and rollback SQL files for a single table."""
        source_schema, target_schema = self._resolve_schemas(
            source_engine, target_engine, spec, schema_overrides
        )
        table_name = self._table_name(spec)

        # Skip tables that do not exist in the source or target.
        source_exists = self.db.table_exists(
            engine_name=source_engine, schema_name=source_schema, table_name=table_name
        )
        target_exists = self.db.table_exists(
            engine_name=target_engine, schema_name=target_schema, table_name=table_name
        )
        if not source_exists and not target_exists:
            self._log(
                "⏭️",
                "yellow",
                f"Skipping {table_name}: does not exist in source or target.",
            )
            return
        if not source_exists:
            self._log(
                "⏭️",
                "yellow",
                f"Skipping {table_name}: does not exist in source {source_schema}.",
            )
            return
        if not target_exists:
            self._log(
                "⏭️",
                "yellow",
                f"Skipping {table_name}: does not exist in target {target_schema}.",
            )
            return

        self._log(
            "📋",
            "cyan",
            f"{table_name}: source={source_schema}, target={target_schema}",
        )

        key_columns = self._key_columns(spec.table_type)
        payload_columns = self._payload_columns(
            source_engine, source_schema, target_engine, target_schema, table_name, key_columns
        )

        self._log(
            "🧩",
            "cyan",
            f"{table_name}: syncing {len(payload_columns)} common payload columns: {', '.join(payload_columns)}",
        )

        base_kwargs = self._base_kwargs(
            source_schema,
            target_schema,
            spec,
            data_columns=self._build_data_columns(payload_columns),
            update_set_clause=self._build_update_set_clause(payload_columns),
            changed_condition=self._build_changed_condition(payload_columns),
        )

        # Forward patch files: REPLACE, INSERT, DELETE
        for op in ("replace", "insert", "delete"):
            query_name = self._commit_query_name(spec, op)
            query = self._render("commit", query_name, **base_kwargs)
            file_path = patch_dir / f"{table_name}_{op.upper()}.sql"
            self._log("📝", "green", f"Writing forward patch: {file_path.name}")
            self._write_sql_file(file_path, query)

        # Rollback files
        for op in ("replace", "insert", "delete"):
            self._generate_rollback_file(
                source_engine=source_engine,
                target_engine=target_engine,
                spec=spec,
                op=op,
                source_schema=source_schema,
                target_schema=target_schema,
                table_name=table_name,
                key_columns=key_columns,
                payload_columns=payload_columns,
                rollback_dir=rollback_dir,
                schema_overrides=schema_overrides,
            )

    def _generate_rollback_file(
        self,
        source_engine: str,
        target_engine: str,
        spec: IndexTableSpec,
        op: str,
        source_schema: str,
        target_schema: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        rollback_dir: Path,
        schema_overrides: dict[str, str] | None = None,
    ) -> None:
        """Build and write one rollback SQL file."""
        keys = ", ".join(key_columns)
        key_select = ", ".join(f"p.{c}" for c in key_columns)
        payload_select = ", ".join(f"p.{c}" for c in payload_columns)
        no_match_source = f"t.{key_columns[0]} IS NULL"

        file_path = rollback_dir / f"{table_name}_{op.upper()}.sql"
        self._log(
            "🔄",
            "blue",
            f"Building rollback for {table_name} [{op.upper()}] -> {file_path.name}",
        )

        if op == "delete":
            # Forward DELETE -> rollback INSERT the deleted rows.
            query = f"""
                SELECT {key_select}, {payload_select}
                  FROM {target_schema}.{table_name} p
                  LEFT JOIN {source_schema}.{table_name} t
                    USING ({keys})
                 WHERE {no_match_source}
            """
            rows = self.db.execute_query(
                engine_name=target_engine, query=query
            )
            self._log(
                "📊",
                "cyan",
                f"{table_name} rollback [DELETE]: fetched {len(rows)} deleted rows to restore",
            )
            sql = self._build_insert_sql(
                target_schema, table_name, key_columns, payload_columns, rows
            )

        elif op == "insert":
            # Forward INSERT -> rollback DELETE the inserted keys.
            key_select_t = ", ".join(f"t.{c}" for c in key_columns)
            no_match_target = f"p.{key_columns[0]} IS NULL"
            query = f"""
                SELECT {key_select_t}
                  FROM {source_schema}.{table_name} t
                  LEFT JOIN {target_schema}.{table_name} p
                    USING ({keys})
                 WHERE {no_match_target}
            """
            rows = self.db.execute_query(
                engine_name=source_engine, query=query
            )
            self._log(
                "📊",
                "cyan",
                f"{table_name} rollback [INSERT]: fetched {len(rows)} inserted keys to remove",
            )
            sql = self._build_delete_sql(
                target_schema, table_name, key_columns, rows
            )

        elif op == "replace":
            # Forward REPLACE -> rollback UPDATE to old values.
            query = f"""
                SELECT {key_select}, {payload_select}
                  FROM {target_schema}.{table_name} p
                  JOIN {source_schema}.{table_name} t
                    USING ({keys})
                 WHERE {self._build_changed_condition(payload_columns)}
            """
            rows = self.db.execute_query(
                engine_name=target_engine, query=query
            )
            self._log(
                "📊",
                "cyan",
                f"{table_name} rollback [REPLACE]: fetched {len(rows)} rows to revert",
            )
            sql = self._build_update_sql(
                target_schema, table_name, key_columns, payload_columns, rows
            )

        else:
            raise ValueError(f"Unknown rollback operation: {op}")

        self._write_sql_file(file_path, sql)
        self._log(
            "📝",
            "green",
            f"Wrote rollback file: {file_path.name}",
        )

    def _build_insert_sql(
        self,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        rows: list[tuple],
    ) -> str:
        """Build a multi-row INSERT statement from fetched rows."""
        if not rows:
            return "-- No rows to restore.\n"
        all_columns = key_columns + payload_columns
        values_list = []
        for row in rows:
            values = ", ".join(self._sql_literal(v) for v in row)
            values_list.append(f"({values})")
        values_str = ",\n    ".join(values_list)
        return (
            f"INSERT INTO {schema_name}.{table_name} "
            f"({', '.join(all_columns)})\n"
            f"VALUES\n    {values_str};\n"
        )

    def _build_delete_sql(
        self,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        rows: list[tuple],
    ) -> str:
        """Build a DELETE ... WHERE (keys) IN (...) statement."""
        if not rows:
            return "-- No rows to remove.\n"
        tuples = ",\n    ".join(
            "(" + ", ".join(self._sql_literal(v) for v in row) + ")"
            for row in rows
        )
        return (
            f"DELETE FROM {schema_name}.{table_name}\n"
            f" WHERE ({', '.join(key_columns)}) IN (\n"
            f"    {tuples}\n);\n"
        )

    def _build_update_sql(
        self,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        rows: list[tuple],
    ) -> str:
        """Build one UPDATE statement per row to revert a REPLACE."""
        if not rows:
            return "-- No rows to revert.\n"
        key_len = len(key_columns)
        statements = []
        for row in rows:
            key_values = row[:key_len]
            payload_values = row[key_len:]
            set_clause = ", ".join(
                f"{col} = {self._sql_literal(val)}"
                for col, val in zip(payload_columns, payload_values)
            )
            where_clause = " AND ".join(
                f"{col} = {self._sql_literal(val)}"
                for col, val in zip(key_columns, key_values)
            )
            statements.append(
                f"UPDATE {schema_name}.{table_name}\n"
                f"   SET {set_clause}\n"
                f" WHERE {where_clause};"
            )
        return "\n".join(statements) + "\n"

    def _sql_literal(self, value) -> str:
        """Escape a Python value for use in a generated SQL statement."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _write_sql_file(self, file_path: Path, sql: str) -> None:
        """Write a SQL string to disk."""
        file_path.write_text(sql.strip() + "\n", encoding="utf-8")

    def _commit_query_name(self, spec: IndexTableSpec, op: str) -> str:
        """Return the commit template name for a table spec and operation."""
        if spec.table_type == "page_profile":
            return f"page_profile_{op}_rows"
        plural = f"{spec.table_type}s"
        return f"index_{plural}_{op}_rows"

    # --------------------------
    # Placeholder / query helpers
    # --------------------------

    def _render(self, group: str, name: str, **kwargs) -> str:
        """Load and render a SQL template from database/queries/patching."""
        file_path = sql_queries_paths["patching"][group][name]
        return resolve_sql_query(file_path, **kwargs)

    def _base_kwargs(
        self,
        source_schema: str,
        target_schema: str,
        spec: IndexTableSpec,
        **extra: str,
    ) -> dict[str, str]:
        """Build the common placeholder dictionary for the SQL templates."""
        kwargs: dict[str, str] = {
            "graphsearch_test": source_schema,
            "graphsearch_prod_mirror": target_schema,
        }

        if spec.table_type in ("doc", "doclink"):
            kwargs["doc_type"] = spec.doc_type or ""

        if spec.table_type == "doclink":
            kwargs["link_type"] = spec.link_type or ""
            kwargs["sem_or_org"] = spec.link_subtype or ""
            kwargs["special_suffix"] = spec.special_suffix

        kwargs.update(extra)
        return kwargs

    def _resolve_schemas(
        self,
        source_engine: str,
        target_engine: str,
        spec: IndexTableSpec,
        schema_overrides: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """
        Resolve source and target schema names from the engine configuration.
        Page profiles live in graph_cache; index tables live in graphsearch.

        schema_overrides can contain:
            source_graphsearch, source_graph_cache,
            target_graphsearch, target_graph_cache
        to bypass engine-based resolution.
        """
        overrides = schema_overrides or {}

        if spec.table_type == "page_profile":
            source_schema = overrides.get(
                "source_graph_cache",
                self.glbcfg.mysql_schema_names[source_engine]["graph_cache"],
            )
            target_schema = overrides.get(
                "target_graph_cache",
                self.glbcfg.mysql_schema_names[target_engine]["graph_cache"],
            )
        else:
            source_schema = overrides.get(
                "source_graphsearch",
                self.glbcfg.mysql_schema_names[source_engine]["graphsearch"],
            )
            target_schema = overrides.get(
                "target_graphsearch",
                self.glbcfg.mysql_schema_names[target_engine]["graphsearch"],
            )

        return source_schema, target_schema

    def _table_name(self, spec: IndexTableSpec) -> str:
        """Return the physical table name for a given spec."""
        if spec.table_type == "doc":
            return f"Index_D_{spec.doc_type}"
        if spec.table_type == "doclink":
            return (
                f"Index_D_{spec.doc_type}_L_{spec.link_type}_T_{spec.link_subtype}"
                f"{spec.special_suffix}"
            )
        if spec.table_type == "page_profile":
            return "Data_N_Object_T_PageProfile"
        raise ValueError(f"Unknown table_type: {spec.table_type}")

    def _key_columns(self, table_type: str) -> list[str]:
        """Return the business-key columns for a given table type."""
        if table_type == "doc":
            return ["doc_type", "doc_id"]
        if table_type == "doclink":
            return [
                "doc_type",
                "doc_id",
                "link_type",
                "link_subtype",
                "link_id",
            ]
        if table_type == "page_profile":
            return ["object_type", "object_id"]
        raise ValueError(f"Unknown table_type: {table_type}")

    def _payload_columns(
        self,
        source_engine: str,
        source_schema: str,
        target_engine: str,
        target_schema: str,
        table_name: str,
        key_columns: list[str],
    ) -> list[str]:
        """
        Return payload columns that exist in both source and target tables.
        Excludes key columns and surrogate/meta columns.
        Using the intersection prevents errors when one side has extra columns.
        """
        source_columns = self.db.get_column_names(
            engine_name=source_engine,
            schema_name=source_schema,
            table_name=table_name,
        )
        target_columns = self.db.get_column_names(
            engine_name=target_engine,
            schema_name=target_schema,
            table_name=table_name,
        )
        excluded = set(key_columns) | self.META_COLUMNS
        return [
            c
            for c in source_columns
            if c in target_columns and c not in excluded
        ]

    def _build_data_columns(self, payload_columns: list[str]) -> str:
        """
        Comma-separated column list for INSERT/SELECT.

        NOTE: the current templates use a single [[data_columns]] placeholder
        for both the INSERT column list and the SELECT list. For the SELECT
        side this can be ambiguous when joining two tables with the same
        column names. Consider splitting the template placeholders into
        [[insert_columns]] (plain names) and [[select_columns]] (e.g. t.col1)
        if you hit 'ambiguous column' errors at runtime.
        """
        return ", ".join(payload_columns)

    def _build_update_set_clause(self, payload_columns: list[str]) -> str:
        """Build the SET clause for the UPDATE ... JOIN replace query."""
        return ", ".join(f"p.{col} = t.{col}" for col in payload_columns)

    def _build_changed_condition(self, payload_columns: list[str]) -> str:
        """
        Build the WHERE clause that detects changed rows using the null-safe
        equality operator <=>.
        """
        return " OR ".join(
            f"NOT (p.{col} <=> t.{col})" for col in payload_columns
        )
