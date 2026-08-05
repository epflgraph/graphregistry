# graphregistry/adapters/persistence/mysql/repositories/arp_indexdeploy.py
from __future__ import annotations
from datetime import datetime
import gzip
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from graphdb.models.sqlquery import print_sql
from pymysql.converters import escape_string

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

    def __init__(
        self,
        db: GraphDB,
        glbcfg: GlobalConfig,
        debug: bool = False,
        use_unhex: bool = False,
    ) -> None:
        self.db = db
        self.glbcfg = glbcfg
        self.msg = GraphLogger()
        self.debug = debug
        self.use_unhex = use_unhex

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

    def apply_patch_files(
        self,
        patch_dir: Path,
        target_engine: str,
        target_schema: str,
        table_specs: list[IndexTableSpec],
        dry_run: bool = False,
    ) -> None:
        """
        Apply generated patch SQL files from patch_dir to the target engine/schema.

        Files are applied in the safe order: REPLACE, then INSERT, then DELETE.
        Only files that exist are executed (missing files mean no diff for that
        operation). Rollback files are not touched by this method.

        With dry_run=True, no SQL is executed; the first 256 characters of each
        file are printed instead.
        """
        for spec in table_specs:
            table_name = self._table_name(spec)

            if not self.db.table_exists(
                engine_name=target_engine,
                schema_name=target_schema,
                table_name=table_name,
            ):
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping {table_name}: does not exist in target {target_schema}.",
                )
                continue

            for op in ("replace", "insert", "delete"):
                file_path = patch_dir / f"{table_name}_{op.upper()}.sql.gz"
                if not file_path.exists():
                    continue

                self._log(
                    "🚀" if not dry_run else "🧪",
                    "magenta" if not dry_run else "cyan",
                    f"{'Applying' if not dry_run else 'Dry-run'} {op.upper()} patch file "
                    f"for {table_name} to database {target_schema}",
                )
                if dry_run:
                    with gzip.open(file_path, "rt", encoding="utf-8") as gz:
                        snippet = gz.read(256)
                    self._log(
                        "📄",
                        "cyan",
                        f"{file_path.name}: {snippet}",
                    )
                    continue

                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", suffix=".sql", delete=False
                ) as tmp:
                    tmp_path = Path(tmp.name)
                    with gzip.open(file_path, "rt", encoding="utf-8") as gz:
                        shutil.copyfileobj(gz, tmp)
                try:
                    self.db.execute_query_from_file(
                        engine_name=target_engine,
                        file_path=tmp_path,
                        database=target_schema,
                        verbose=False,
                    )
                finally:
                    os.unlink(tmp_path)
                self._log(
                    "✅",
                    "green",
                    f"Applied {op.upper()} patch file for {table_name}",
                )

    def generate_patch_files(
        self,
        source_engine: str,
        target_engine: str,
        table_specs: list[IndexTableSpec],
        actions: tuple[str, ...] = (),
        schema_overrides: dict[str, str] | None = None,
        replace_batch_size: int = 100,
        delete_batch_size: int = 100,
        insert_batch_size: int = 100,
        skip_count: bool = False,
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
                replace_batch_size=replace_batch_size,
                delete_batch_size=delete_batch_size,
                insert_batch_size=insert_batch_size,
                skip_count=skip_count,
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

        # Skip tables that do not exist in source or target.
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
        replace_batch_size: int = 100,
        delete_batch_size: int = 100,
        insert_batch_size: int = 100,
        skip_count: bool = False,
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

        keys = ", ".join(key_columns)
        key_select_source = ", ".join(f"t.{c}" for c in key_columns)
        key_select_target = ", ".join(f"p.{c}" for c in key_columns)
        payload_select_source = ", ".join(f"t.{c}" for c in payload_columns)
        no_match_source = f"t.{key_columns[0]} IS NULL"
        no_match_target = f"p.{key_columns[0]} IS NULL"
        changed_condition = self._build_changed_condition(payload_columns)

        # ---------- Forward DELETE ----------
        delete_query = f"""
            SELECT {key_select_target}
              FROM {target_schema}.{table_name} p
              LEFT JOIN {source_schema}.{table_name} t
                USING ({keys})
             WHERE {no_match_source}
        """
        if self.debug:
            print_sql(delete_query, title="forward DELETE base")
        delete_path = patch_dir / f"{table_name}_DELETE.sql.gz"
        if skip_count:
            self._log(
                "📝",
                "blue",
                f"Streaming forward DELETE patch for {table_name}",
            )
            delete_written = self._stream_delete_from_query(
                delete_path,
                target_schema,
                table_name,
                key_columns,
                target_engine,
                delete_query,
                batch_size=delete_batch_size,
            )
            if delete_written:
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward DELETE patch ({delete_written} rows): {delete_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward DELETE patch for {table_name}: no rows",
                )
        else:
            self._log(
                "🔢",
                "blue",
                f"Counting forward DELETE rows for {table_name}",
            )
            delete_count = self._count_rows(target_engine, delete_query)
            if delete_count > self.glbcfg.patch_max_rows:
                self._log(
                    "🚫",
                    "red",
                    f"Skipping forward DELETE patch for {table_name}: "
                    f"{delete_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                )
            elif delete_count:
                self._stream_delete_from_query(
                    delete_path,
                    target_schema,
                    table_name,
                    key_columns,
                    target_engine,
                    delete_query,
                    batch_size=delete_batch_size,
                )
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward DELETE patch ({delete_count} rows): {delete_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward DELETE patch for {table_name}: no rows",
                )

        # ---------- Forward INSERT ----------
        insert_query = f"""
            SELECT {key_select_source}, {payload_select_source}
              FROM {source_schema}.{table_name} t
              LEFT JOIN {target_schema}.{table_name} p
                USING ({keys})
             WHERE {no_match_target}
        """
        if self.debug:
            print_sql(insert_query, title="forward INSERT base")
        insert_path = patch_dir / f"{table_name}_INSERT.sql.gz"
        if skip_count:
            self._log(
                "📝",
                "blue",
                f"Streaming forward INSERT patch for {table_name}",
            )
            insert_written = self._stream_insert_from_query(
                insert_path,
                target_schema,
                table_name,
                key_columns,
                payload_columns,
                source_engine,
                insert_query,
                batch_size=insert_batch_size,
            )
            if insert_written:
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward INSERT patch ({insert_written} rows): {insert_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward INSERT patch for {table_name}: no rows",
                )
        else:
            self._log(
                "🔢",
                "blue",
                f"Counting forward INSERT rows for {table_name}",
            )
            insert_count = self._count_rows(source_engine, insert_query)
            if insert_count > self.glbcfg.patch_max_rows:
                self._log(
                    "🚫",
                    "red",
                    f"Skipping forward INSERT patch for {table_name}: "
                    f"{insert_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                )
            elif insert_count:
                self._stream_insert_from_query(
                    insert_path,
                    target_schema,
                    table_name,
                    key_columns,
                    payload_columns,
                    source_engine,
                    insert_query,
                    batch_size=insert_batch_size,
                )
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward INSERT patch ({insert_count} rows): {insert_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward INSERT patch for {table_name}: no rows",
                )

        # ---------- Forward REPLACE ----------
        replace_query = f"""
            SELECT {key_select_source}, {payload_select_source}
              FROM {source_schema}.{table_name} t
              JOIN {target_schema}.{table_name} p
                USING ({keys})
             WHERE {changed_condition}
        """
        if self.debug:
            print_sql(replace_query, title="forward REPLACE base")
        replace_path = patch_dir / f"{table_name}_REPLACE.sql.gz"
        if skip_count:
            self._log(
                "📝",
                "blue",
                f"Streaming forward REPLACE patch for {table_name}",
            )
            replace_written = self._stream_replace_from_query(
                replace_path,
                target_schema,
                table_name,
                key_columns,
                payload_columns,
                source_engine,
                replace_query,
                batch_size=replace_batch_size,
            )
            if replace_written:
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward REPLACE patch ({replace_written} rows): {replace_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward REPLACE patch for {table_name}: no rows",
                )
        else:
            self._log(
                "🔢",
                "blue",
                f"Counting forward REPLACE rows for {table_name}",
            )
            replace_count = self._count_rows(source_engine, replace_query)
            if replace_count > self.glbcfg.patch_max_rows:
                self._log(
                    "🚫",
                    "red",
                    f"Skipping forward REPLACE patch for {table_name}: "
                    f"{replace_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                )
            elif replace_count:
                self._stream_replace_from_query(
                    replace_path,
                    target_schema,
                    table_name,
                    key_columns,
                    payload_columns,
                    source_engine,
                    replace_query,
                    batch_size=replace_batch_size,
                )
                self._log(
                    "📝",
                    "green",
                    f"Wrote forward REPLACE patch ({replace_count} rows): {replace_path.name}",
                )
            else:
                self._log(
                    "⏭️",
                    "yellow",
                    f"Skipping forward REPLACE patch for {table_name}: no rows",
                )

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
                replace_batch_size=replace_batch_size,
                delete_batch_size=delete_batch_size,
                insert_batch_size=insert_batch_size,
                skip_count=skip_count,
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
        replace_batch_size: int = 100,
        delete_batch_size: int = 100,
        insert_batch_size: int = 100,
        skip_count: bool = False,
    ) -> None:
        """Build and write one rollback SQL file using chunked queries."""
        keys = ", ".join(key_columns)
        key_select = ", ".join(f"p.{c}" for c in key_columns)
        payload_select = ", ".join(f"p.{c}" for c in payload_columns)
        no_match_source = f"t.{key_columns[0]} IS NULL"

        file_path = rollback_dir / f"{table_name}_{op.upper()}.sql.gz"
        self._log(
            "🔄",
            "blue",
            f"Building rollback for {table_name} [{op.upper()}] -> {file_path.name}",
        )

        row_count = 0
        written = False

        if op == "delete":
            # Forward DELETE -> rollback INSERT the deleted rows.
            query = f"""
                SELECT {key_select}, {payload_select}
                  FROM {target_schema}.{table_name} p
                  LEFT JOIN {source_schema}.{table_name} t
                    USING ({keys})
                 WHERE {no_match_source}
            """
            if self.debug:
                print_sql(query, title=f"rollback {op.upper()} base")
            if skip_count:
                self._log(
                    "📝",
                    "blue",
                    f"Streaming rollback [{op.upper()}] for {table_name}",
                )
                row_count = self._stream_insert_from_query(
                    file_path,
                    target_schema,
                    table_name,
                    key_columns,
                    payload_columns,
                    target_engine,
                    query,
                    batch_size=insert_batch_size,
                )
                written = row_count > 0
            else:
                self._log(
                    "🔢",
                    "blue",
                    f"Counting rollback [DELETE] rows for {table_name}",
                )
                row_count = self._count_rows(target_engine, query)
                if row_count > self.glbcfg.patch_max_rows:
                    self._log(
                        "🚫",
                        "red",
                        f"Skipping rollback [DELETE] for {table_name}: "
                        f"{row_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                    )
                elif row_count:
                    self._stream_insert_from_query(
                        file_path,
                        target_schema,
                        table_name,
                        key_columns,
                        payload_columns,
                        target_engine,
                        query,
                        batch_size=insert_batch_size,
                    )
                    written = True

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
            if self.debug:
                print_sql(query, title=f"rollback {op.upper()} base")
            if skip_count:
                self._log(
                    "📝",
                    "blue",
                    f"Streaming rollback [{op.upper()}] for {table_name}",
                )
                row_count = self._stream_delete_from_query(
                    file_path,
                    target_schema,
                    table_name,
                    key_columns,
                    source_engine,
                    query,
                    batch_size=delete_batch_size,
                )
                written = row_count > 0
            else:
                self._log(
                    "🔢",
                    "blue",
                    f"Counting rollback [INSERT] rows for {table_name}",
                )
                row_count = self._count_rows(source_engine, query)
                if row_count > self.glbcfg.patch_max_rows:
                    self._log(
                        "🚫",
                        "red",
                        f"Skipping rollback [INSERT] for {table_name}: "
                        f"{row_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                    )
                elif row_count:
                    self._stream_delete_from_query(
                        file_path,
                        target_schema,
                        table_name,
                        key_columns,
                        source_engine,
                        query,
                        batch_size=delete_batch_size,
                    )
                    written = True

        elif op == "replace":
            # Forward REPLACE -> rollback UPDATE to old values.
            query = f"""
                SELECT {key_select}, {payload_select}
                  FROM {target_schema}.{table_name} p
                  JOIN {source_schema}.{table_name} t
                    USING ({keys})
                 WHERE {self._build_changed_condition(payload_columns)}
            """
            if self.debug:
                print_sql(query, title=f"rollback {op.upper()} base")
            if skip_count:
                self._log(
                    "📝",
                    "blue",
                    f"Streaming rollback [{op.upper()}] for {table_name}",
                )
                row_count = self._stream_update_from_query(
                    file_path,
                    target_schema,
                    table_name,
                    key_columns,
                    payload_columns,
                    target_engine,
                    query,
                )
                written = row_count > 0
            else:
                self._log(
                    "🔢",
                    "blue",
                    f"Counting rollback [REPLACE] rows for {table_name}",
                )
                row_count = self._count_rows(target_engine, query)
                if row_count > self.glbcfg.patch_max_rows:
                    self._log(
                        "🚫",
                        "red",
                        f"Skipping rollback [REPLACE] for {table_name}: "
                        f"{row_count} rows > patch_max_rows ({self.glbcfg.patch_max_rows})",
                    )
                elif row_count:
                    self._stream_update_from_query(
                        file_path,
                        target_schema,
                        table_name,
                        key_columns,
                        payload_columns,
                        target_engine,
                        query,
                    )
                    written = True

        else:
            raise ValueError(f"Unknown rollback operation: {op}")

        if written:
            self._log(
                "📝",
                "green",
                f"Wrote rollback file ({row_count} rows): {file_path.name}",
            )
        elif row_count == 0:
            self._log(
                "⏭️",
                "yellow",
                f"Skipping rollback [{op.upper()}] for {table_name}: no rows",
            )

    def _fetch_rows_chunked(
        self,
        engine_name: str,
        query: str,
        chunk_size: int = 5000,
    ):
        """Yield result rows in chunks using LIMIT/OFFSET."""
        offset = 0
        while True:
            chunk_query = f"{query.rstrip()} LIMIT {chunk_size} OFFSET {offset}"
            if self.debug:
                print_sql(chunk_query, title=f"patch fetch chunk offset={offset}")
            rows = self.db.execute_query(engine_name=engine_name, query=chunk_query)
            if not rows:
                break
            yield rows
            if len(rows) < chunk_size:
                break
            offset += chunk_size

    def _count_rows(self, engine_name: str, query: str) -> int:
        """Return the total row count for a query by wrapping it in a COUNT."""
        count_query = f"SELECT COUNT(*) FROM ({query.strip()}) AS _patch_count"
        if self.debug:
            print_sql(count_query, title="patch count")
        rows = self.db.execute_query(engine_name=engine_name, query=count_query)
        return rows[0][0] if rows else 0

    def _write_value_rows(
        self,
        f,
        rows: list[tuple],
        first: bool,
    ) -> bool:
        """Write (v1, v2, ...) value lines for INSERT/REPLACE/DELETE IN."""
        for row in rows:
            values = ", ".join(self._sql_literal(v) for v in row)
            prefix = "    " if first else "  , "
            f.write(f"{prefix}({values})\n")
            first = False
        return first

    def _batched_rows(
        self,
        first_chunk: list[tuple],
        chunks,
        batch_size: int,
    ):
        """Yield fixed-size batches from a chunked row iterator."""
        batch: list[tuple] = []
        for row in first_chunk:
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
        for chunk in chunks:
            for row in chunk:
                batch.append(row)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _write_batched_value_rows(
        self,
        f,
        header: str,
        first_chunk: list[tuple],
        chunks,
        batch_size: int = 100,
        footer: str = "",
    ) -> int:
        """Write multiple INSERT/REPLACE/DELETE IN statements in batches."""
        rows_written = 0
        first_statement = True
        for batch in self._batched_rows(first_chunk, chunks, batch_size):
            if not first_statement:
                f.write(header)
            first = True
            for row in batch:
                values = ", ".join(self._sql_literal(v) for v in row)
                prefix = "    " if first else "  , "
                f.write(f"{prefix}({values})\n")
                first = False
            f.write(f"{footer};\n")
            first_statement = False
            rows_written += len(batch)
        return rows_written

    def _write_update_rows(
        self,
        f,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        rows: list[tuple],
    ) -> int:
        """Write one UPDATE statement per row."""
        key_len = len(key_columns)
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
            f.write(
                f"UPDATE {table_name}\n"
                f"   SET {set_clause}\n"
                f" WHERE {where_clause};\n"
            )
        return len(rows)

    def _stream_insert_from_query(
        self,
        file_path: Path,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        engine_name: str,
        query: str,
        chunk_size: int = 5000,
        batch_size: int = 100,
    ) -> int:
        """Stream a chunked query into batched INSERT gzip file."""
        all_columns = key_columns + payload_columns
        header = (
            f"INSERT INTO {table_name} "
            f"({', '.join(all_columns)})\nVALUES\n"
        )
        chunks = self._fetch_rows_chunked(engine_name, query, chunk_size)
        try:
            first_chunk = next(chunks)
        except StopIteration:
            return 0

        with gzip.open(file_path, "wt", encoding="utf-8", compresslevel=9) as f:
            f.write("SET NAMES utf8mb4;\n")
            f.write(header)
            rows_written = self._write_batched_value_rows(
                f, header, first_chunk, chunks, batch_size=batch_size
            )
        return rows_written

    def _stream_replace_from_query(
        self,
        file_path: Path,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        engine_name: str,
        query: str,
        chunk_size: int = 5000,
        batch_size: int = 100,
    ) -> int:
        """Stream a chunked query into batched REPLACE gzip file."""
        all_columns = key_columns + payload_columns
        header = (
            f"REPLACE INTO {table_name} "
            f"({', '.join(all_columns)})\nVALUES\n"
        )
        chunks = self._fetch_rows_chunked(engine_name, query, chunk_size)
        try:
            first_chunk = next(chunks)
        except StopIteration:
            return 0

        with gzip.open(file_path, "wt", encoding="utf-8", compresslevel=9) as f:
            f.write("SET NAMES utf8mb4;\n")
            f.write(header)
            rows_written = self._write_batched_value_rows(
                f, header, first_chunk, chunks, batch_size=batch_size
            )
        return rows_written

    def _stream_delete_from_query(
        self,
        file_path: Path,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        engine_name: str,
        query: str,
        chunk_size: int = 5000,
        batch_size: int = 100,
    ) -> int:
        """Stream a chunked query into batched DELETE ... IN gzip file."""
        header = (
            f"DELETE FROM {table_name}\n"
            f" WHERE ({', '.join(key_columns)}) IN (\n"
        )
        chunks = self._fetch_rows_chunked(engine_name, query, chunk_size)
        try:
            first_chunk = next(chunks)
        except StopIteration:
            return 0

        with gzip.open(file_path, "wt", encoding="utf-8", compresslevel=9) as f:
            f.write("SET NAMES utf8mb4;\n")
            f.write(header)
            rows_written = self._write_batched_value_rows(
                f, header, first_chunk, chunks, batch_size=batch_size, footer=")"
            )
        return rows_written

    def _stream_update_from_query(
        self,
        file_path: Path,
        schema_name: str,
        table_name: str,
        key_columns: list[str],
        payload_columns: list[str],
        engine_name: str,
        query: str,
        chunk_size: int = 5000,
    ) -> int:
        """Stream a chunked query into one-UPDATE-per-row gzip file."""
        chunks = self._fetch_rows_chunked(engine_name, query, chunk_size)
        try:
            first_chunk = next(chunks)
        except StopIteration:
            return 0

        rows_written = 0
        with gzip.open(file_path, "wt", encoding="utf-8", compresslevel=9) as f:
            f.write("SET NAMES utf8mb4;\n")
            rows_written += self._write_update_rows(
                f, schema_name, table_name, key_columns, payload_columns, first_chunk
            )
            for chunk in chunks:
                rows_written += self._write_update_rows(
                    f, schema_name, table_name, key_columns, payload_columns, chunk
                )
        return rows_written

    def _sql_literal(self, value) -> str:
        """Escape a Python value for use in a generated SQL statement.

        When ``self.use_unhex`` is True, strings and bytes are encoded as
        MySQL UNHEX literals. This avoids any escaping issues but makes the
        generated SQL hard to read.

        When ``self.use_unhex`` is False (the default), strings are rendered
        as readable, quoted literals with backslash/single-quote escaping.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, bytes):
            return f"UNHEX('{value.hex()}')"
        if self.use_unhex:
            return f"UNHEX('{value.encode('utf-8').hex()}')"
        return self._quote_string(value)

    def _quote_string(self, value: str) -> str:
        """Return a MySQL-safe single-quoted string literal."""
        # Use pymysql's MySQL-compatible escaping for Unicode, quotes,
        # backslashes, newlines, etc. The file is UTF-8 and SET NAMES utf8mb4
        # is set, so emojis and other multi-byte characters are preserved.
        return f"'{escape_string(value)}'"

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
