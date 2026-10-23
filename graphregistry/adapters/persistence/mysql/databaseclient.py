# graphregistry/adapters/persistence/mysql/databaseclient.py
"""MySQL adapter for the DatabaseClient port, over the graphdb-client public surface.

The adapter wraps the published ``graphdb.client`` contract (Environments +
EnvironmentGateway) and re-exposes it under the engine-name-addressed call
convention the graphregistry persistence adapters are built on. All
graphdb-client coupling of the persistence layer is confined to this file;
the executor methods route to the per-environment query executor, the schema
methods to the schema gateways, and the small SQL-builder conveniences are
implemented locally over the executor.
"""
from __future__ import annotations
import re
from typing import Optional
from loguru import logger as sysmsg
from graphdb.adapters.rendering.rdr_sqlquery import print_sql
from graphdb.client import Environments
from graphregistry.application.ports.repositories.prt_databaseclient import DatabaseClient

#==================#
# Class Definition #
#==================#
class MySQLDatabaseClient(DatabaseClient):
    """Database-client adapter routing the engine-name call convention to the
    per-environment gateways of the graphdb-client public surface."""

    # Public Method: Initialise the adapter with the Environments composition point.
    def __init__(self, environments: Environments) -> None:
        self.environments = environments

    #================================================================#
    # Method Group: Raw SQL execution                                #
    #================================================================#

    # Public Method: Execute one SQL statement and return the result rows.
    def execute_query(self, engine_name: str, query: str, schema_name: Optional[str] = None, params: Optional[dict] = None, commit: bool = False, return_exception: bool = False, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query(
            engine_name      = engine_name,
            query            = query,
            schema_name      = schema_name,
            params           = params,
            commit           = commit,
            return_exception = return_exception,
            verbose          = verbose,
            query_id         = query_id,
        )

    # Public Method: Execute one statement through the MySQL client binary.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_in_shell(
            engine_name = engine_name,
            query       = query,
            verbose     = verbose,
            query_id    = query_id,
        )

    # Public Method: Execute an INSERT..SELECT with update-if-different semantics.
    def execute_query_as_safe_inserts(self, engine_name: str, schema_name: str, table_name: str, query: str, key_column_names: list, upd_column_names: list, eval_column_names: Optional[list] = None, actions: tuple = (), verbose: bool = False, query_id: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_as_safe_inserts(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = table_name,
            query             = query,
            key_column_names  = key_column_names,
            upd_column_names  = upd_column_names,
            eval_column_names = eval_column_names,
            actions           = actions,
            verbose           = verbose,
            query_id          = query_id,
        )

    # Public Method: Execute a safe-insert query chunked by the target table.
    def execute_query_as_safe_inserts_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, key_column_names: list, upd_column_names: list, eval_column_names: Optional[list] = None, actions: tuple = (), table_to_chunk: Optional[str] = None, chunk_filter: Optional[str] = None, chunk_size: Optional[int] = None, row_id_name: Optional[str] = None, show_progress: bool = False, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_as_safe_inserts_in_chunks(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = table_name,
            query             = query,
            key_column_names  = key_column_names,
            upd_column_names  = upd_column_names,
            eval_column_names = eval_column_names,
            actions           = actions,
            table_to_chunk    = table_to_chunk,
            chunk_filter      = chunk_filter,
            chunk_size        = chunk_size,
            row_id_name       = row_id_name,
            show_progress     = show_progress,
            verbose           = verbose,
            query_id          = query_id,
        )

    # Public Method: Execute a query chunked by the target table's row_id.
    def execute_query_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, has_filters: Optional[bool] = None, table_to_chunk: Optional[str] = None, chunk_filter: Optional[str] = None, chunk_size: int = 1000000, row_id_name: str = 'row_id', show_progress: bool = False, verbose: bool = False, query_id: Optional[str] = None, desc: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_in_chunks(
            engine_name    = engine_name,
            schema_name    = schema_name,
            table_name     = table_name,
            query          = query,
            has_filters    = has_filters,
            table_to_chunk = table_to_chunk,
            chunk_filter   = chunk_filter,
            chunk_size     = chunk_size,
            row_id_name    = row_id_name,
            show_progress  = show_progress,
            verbose        = verbose,
            query_id       = query_id,
        )

    # Public Method: Stream query results into a local file.
    def execute_query_stream_to_file(self, engine_name: str, query: str, schema_name: Optional[str] = None, params: Optional[dict] = None, fetch_size: int = 1000, output_file: Optional[str] = None, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_stream_to_file(
            engine_name = engine_name,
            query       = query,
            schema_name = schema_name,
            params      = params,
            fetch_size  = fetch_size,
            output_file = output_file,
            verbose     = verbose,
            query_id    = query_id,
        )

    # Public Method: Execute a SQL file through the MySQL client binary.
    def execute_query_from_file(self, engine_name: str, file_path: str, database: Optional[str] = None, verbose: bool = False) -> Any:
        return self.environments.get(engine_name).query_executor.execute_query_from_file(
            engine_name = engine_name,
            file_path   = file_path,
            database    = database,
            verbose     = verbose,
        )

    #================================================================#
    # Method Group: Script execution and SQL-builder conveniences    #
    #================================================================#

    # Public Method: Execute a SQL script statement-by-statement, chunking UPDATEs.
    def execute_sql_statements_in_chunks(self, engine_name: str, sql: str, chunk_size: int = 100000, schema_mapping: Optional[dict] = None, show_progress: bool = True, verbose: bool = False, query_id: Optional[str] = None) -> None:
        """Execute a SQL script statement-by-statement, chunking UPDATE statements."""

        # Apply the schema mapping so template placeholders resolve to real names.
        if schema_mapping:
            for placeholder, real_name in schema_mapping.items():
                sql = sql.replace(f"[[{placeholder}]]", real_name)

        # Strip comments and split the script into individual statements.
        cleaned_sql = self._strip_sql_comments(sql)
        statements = self._split_sql_statements(cleaned_sql)

        # Execute each statement: DDL through the shell, UPDATEs in chunks.
        for idx, stmt in enumerate(statements, start=1):
            stmt_id = f"{query_id or 'sql_chunk'}-{idx}"
            upper = stmt.upper()

            # Empty statements carry no executable content.
            if not upper:
                continue

            # DDL cannot be chunked safely, so it always runs through the shell.
            if any(kw in upper for kw in ("CREATE", "DROP", "ALTER", "TRUNCATE")):
                if verbose:
                    print_sql(stmt, title=f"Executing DDL [{stmt_id}]")
                self.execute_query_in_shell(engine_name=engine_name, query=stmt + ";", verbose=verbose, query_id=stmt_id)
                continue

            # Chunk only simple UPDATE statements; everything else runs as-is.
            parsed = self._parse_update_target(stmt) if upper.startswith("UPDATE") else None
            if parsed is None:
                if verbose:
                    print_sql(stmt, title=f"Executing statement [{stmt_id}]")
                self.execute_query_in_shell(engine_name=engine_name, query=stmt + ";", verbose=verbose, query_id=stmt_id)
                continue

            # Resolve the update target and run the statement in row_id chunks.
            schema_name, table_name, alias = parsed
            if not schema_name:
                raise ValueError(f"Could not determine schema for statement [{stmt_id}]: {stmt[:120]}")

            # Derive the chunking parameters from the statement shape.
            target_label = f"{schema_name}.{table_name}"
            sysmsg.info(f"Executing statement {idx}/{len(statements)} in chunks: {target_label}")
            row_id_name = f"{alias}.row_id" if alias else "row_id"
            has_filters = "WHERE" in upper
            if verbose:
                print_sql(stmt, title=f"Executing in chunks [{stmt_id}]")

            # Execute the chunked UPDATE through the query executor.
            self.execute_query_in_chunks(
                engine_name   = engine_name,
                schema_name   = schema_name,
                table_name    = table_name,
                query         = stmt,
                has_filters   = has_filters,
                chunk_size    = chunk_size,
                row_id_name   = row_id_name,
                show_progress = show_progress,
                verbose       = verbose,
                query_id      = stmt_id,
            )

    # Public Method: Update rows matching simple equality conditions.
    def set_cells(self, engine_name: str, schema_name: str, table_name: str, set: tuple = (), where: tuple = (), verbose: bool = False) -> None:
        """Update rows matching simple equality conditions."""

        # No columns to set means there is nothing to update.
        if len(set) == 0:
            sysmsg.error("No columns to set. Please provide at least one column and value pair.")
            return

        # Build the SET and WHERE clauses from the column/value tuples.
        set_clause = ', '.join([f"{col} = '{val}'" for col, val in set])
        where_clause = ' AND '.join([f"{col} = '{val}'" for col, val in where]) if len(where) > 0 else "TRUE"

        # Execute the UPDATE through the MySQL client binary.
        sql_query = f"""
            UPDATE {schema_name}.{table_name}
               SET {set_clause}
             WHERE {where_clause}
        """
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose=verbose)

    # Public Method: Select rows matching simple equality conditions.
    def get_cells(self, engine_name: str, schema_name: str, table_name: str, select: tuple = (), where: tuple = (), verbose: bool = False) -> list:
        """Select rows matching simple equality conditions."""

        # Build the WHERE clause; a None column means a raw SQL condition.
        where_clause = ' AND '.join([f"{col} = '{val}'" if col is not None else f"({val})" for col, val in where]) if len(where) > 0 else "TRUE"

        # Execute the SELECT and return the rows as a list of tuples.
        sql_query = f"""
            SELECT {', '.join(select) if len(select) > 0 else '*'}
              FROM {schema_name}.{table_name}
             WHERE {where_clause}
        """
        result = self.execute_query(engine_name=engine_name, query=sql_query)
        return result if result else []

    #================================================================#
    # Method Group: Schema introspection and DDL                     #
    #================================================================#

    # Public Method: Check whether a table exists in the schema.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        return self.environments.get(engine_name).table.table_exists(schema_name, table_name, exclude_views=exclude_views)

    # Public Method: List the tables of a schema.
    def get_tables_in_schema(self, engine_name: str, schema_name: str, include_views: bool = False, filter_by: Any = False, use_regex: Any = False) -> list:
        return self.environments.get(engine_name).table.get_tables(
            schema_name   = schema_name,
            include_views = include_views,
            filter_by     = filter_by or None,
            use_regex     = use_regex or None,
        )

    # Public Method: Count the rows of a table.
    def count_rows_in_table(self, engine_name: str, schema_name: str, table_name: str, where_clause: Optional[str] = None) -> int:
        return self.environments.get(engine_name).table.count_rows_in_table(schema_name, table_name, where_clause=where_clause)

    # Public Method: List the column names of a table.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list:
        return self.environments.get(engine_name).column.get_column_names(schema_name, table_name)

    # Public Method: Check whether a table has a column (falsy match on type).
    def has_column(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        return self.environments.get(engine_name).column.has_column(schema_name, table_name, column_name)

    # Public Method: Check whether a table has a column.
    def column_exists(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        return self.environments.get(engine_name).column.column_exists(schema_name, table_name, column_name)

    # Public Method: Apply the configured column datatypes to a table.
    def apply_datatypes(self, engine_name: str, schema_name: str, table_name: str, datatypes_json: dict, display_elapsed_time: bool = False, estimated_num_rows: bool = False) -> None:
        return self.environments.get(engine_name).column.apply_datatypes(
            schema_name          = schema_name,
            table_name           = table_name,
            datatypes_json       = datatypes_json,
            display_elapsed_time = display_elapsed_time,
            estimated_num_rows   = estimated_num_rows,
        )

    # Public Method: Check whether a database exists.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        return self.environments.get(engine_name).database.database_exists(schema_name)

    # Public Method: Create a database, optionally dropping an existing one.
    def create_database(self, engine_name: str, schema_name: str, drop_database: bool = False) -> None:
        return self.environments.get(engine_name).database.create_database(schema_name, drop_existing=drop_database)

    # Public Method: Apply the configured keys to a table.
    def apply_keys(self, engine_name: str, schema_name: str, table_name: str, keys_json: dict, display_elapsed_time: bool = False, estimated_num_rows: bool = False) -> None:
        return self.environments.get(engine_name).key.apply_keys(
            schema_name          = schema_name,
            table_name           = table_name,
            keys_json            = keys_json,
            display_elapsed_time = display_elapsed_time,
            estimated_num_rows   = estimated_num_rows,
        )

    # Public Method: Check whether a key/index exists on a table.
    def key_exists(self, engine_name: str, schema_name: str, table_name: str, key_name: str) -> bool:
        return self.environments.get(engine_name).key.key_exists(schema_name, table_name, key_name)

    # Public Method: Import a dumped database folder into one environment.
    def import_database(self, engine_name: str, schema_name: str, input_folder: str, create_keys_after_import: bool = False, ignore_existing: bool = False, verbose: bool = False, compress: bool = False) -> None:
        """Import a dumped database folder into one environment.

        Routes to the published ImportOperations use case of graphdb-client;
        the engine name maps 1:1 to the operation's environment name.
        """
        from graphdb.application.operations.ops_import import ImportOperations

        # Delegate to the published import operation over the shared Environments.
        ImportOperations(self.environments).import_database(
            env_name                 = engine_name,
            schema_name              = schema_name,
            input_folder             = input_folder,
            create_keys_after_import = create_keys_after_import,
            ignore_existing          = ignore_existing,
            verbose                  = verbose,
            compress                 = compress,
        )

    # Public Method: Return the SQLAlchemy engine pool of one environment.
    def get_engine(self, engine_name: str) -> Any:
        return self.environments.get(engine_name).engine

    #================================================================#
    # Method Group: Internal SQL-script helpers                      #
    #================================================================#

    # Internal Function: Remove SQL line and block comments, preserving string literals.
    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        """Remove SQL line comments (-- ...) and block comments (/* ... */)."""

        # Scan character-by-character, tracking string-literal state.
        result = []
        i = 0
        n = len(sql)
        in_string = None
        while i < n:
            ch = sql[i]

            # Inside a string literal everything is preserved verbatim.
            if in_string:
                result.append(ch)
                if ch == '\\' and i + 1 < n:
                    result.append(sql[i + 1])
                    i += 2
                    continue
                if ch == in_string:
                    in_string = None
                i += 1
                continue

            # A quote character opens a string literal.
            if ch in ("'", '"', '`'):
                in_string = ch
                result.append(ch)
                i += 1
                continue

            # Line comments run to the end of the line.
            if ch == '-' and i + 1 < n and sql[i + 1] == '-':
                while i < n and sql[i] != '\n':
                    i += 1
                continue

            # Block comments run to the closing marker.
            if ch == '/' and i + 1 < n and sql[i + 1] == '*':
                i += 2
                while i < n and not (sql[i] == '*' and i + 1 < n and sql[i + 1] == '/'):
                    i += 1
                i += 2
                continue

            # Regular characters pass through untouched.
            result.append(ch)
            i += 1
        return ''.join(result)

    # Internal Function: Split a SQL script into statements on semicolons.
    @staticmethod
    def _split_sql_statements(sql: str) -> list[str]:
        """Split a SQL script into individual statements, ignoring semicolons in strings."""

        # Scan character-by-character, splitting outside string literals.
        statements = []
        current = []
        i = 0
        n = len(sql)
        in_string = None
        while i < n:
            ch = sql[i]

            # Inside a string literal everything is preserved verbatim.
            if in_string:
                current.append(ch)
                if ch == '\\' and i + 1 < n:
                    current.append(sql[i + 1])
                    i += 2
                    continue
                if ch == in_string:
                    in_string = None
                i += 1
                continue

            # A quote character opens a string literal.
            if ch in ("'", '"', '`'):
                in_string = ch
                current.append(ch)
                i += 1
                continue

            # A semicolon outside a string terminates the statement.
            if ch == ';':
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
                i += 1
                continue

            # Regular characters accumulate into the current statement.
            current.append(ch)
            i += 1

        # Flush the trailing statement when the script has no final semicolon.
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        return statements

    # Internal Function: Extract the update target of a simple UPDATE statement.
    @staticmethod
    def _parse_update_target(query: str) -> Optional[tuple[Optional[str], str, Optional[str]]]:
        """Extract (schema_name, table_name, alias) from a simple UPDATE statement."""

        # Match the leading UPDATE clause of the statement.
        pattern = re.compile(
            r"^\s*UPDATE\s+"
            r"(?:`?([\w_]+)`?\.)?"
            r"`?([\w_]+)`?"
            r"(?:\s+AS)?"
            r"(?:\s+([A-Za-z_][A-Za-z0-9_]*)\b"
            r"(?=\s+(?:INNER|LEFT|RIGHT|FULL|CROSS|JOIN|SET|WHERE|;|$)))?",
            re.IGNORECASE,
        )
        match = pattern.match(query)
        if not match:
            return None
        schema, table, alias = match.groups()
        return schema, table, alias
