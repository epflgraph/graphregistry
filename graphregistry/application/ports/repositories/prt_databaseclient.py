# graphregistry/application/ports/repositories/prt_databaseclient.py
from __future__ import annotations
from typing import Any, Optional, Protocol, runtime_checkable

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class DatabaseClient(Protocol):
    """Port for the MySQL database client used by the persistence adapters.

    The port carries the engine-name-addressed call convention the adapters
    are built on: every method names the target environment explicitly, and
    the adapter resolves it to a per-environment gateway. It covers raw SQL
    execution (interactive, shell, chunked, file-based), schema
    introspection/DDL, and the small SQL-builder conveniences the cache
    pipeline relies on.
    """

    # Public Method: Execute one SQL statement and return the result rows.
    def execute_query(self, engine_name: str, query: str, schema_name: Optional[str] = None, params: Optional[dict] = None, commit: bool = False, return_exception: bool = False, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        ...

    # Public Method: Execute one statement through the MySQL client binary.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        ...

    # Public Method: Execute an INSERT..SELECT with update-if-different semantics.
    def execute_query_as_safe_inserts(self, engine_name: str, schema_name: str, table_name: str, query: str, key_column_names: list, upd_column_names: list, eval_column_names: Optional[list] = None, actions: tuple = (), verbose: bool = False, query_id: Optional[str] = None) -> Any:
        ...

    # Public Method: Execute a safe-insert query chunked by the target table.
    def execute_query_as_safe_inserts_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, key_column_names: list, upd_column_names: list, eval_column_names: Optional[list] = None, actions: tuple = (), table_to_chunk: Optional[str] = None, chunk_filter: Optional[str] = None, chunk_size: Optional[int] = None, row_id_name: Optional[str] = None, show_progress: bool = False, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        ...

    # Public Method: Execute a query chunked by the target table's row_id.
    def execute_query_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, has_filters: Optional[bool] = None, table_to_chunk: Optional[str] = None, chunk_filter: Optional[str] = None, chunk_size: int = 1000000, row_id_name: str = 'row_id', show_progress: bool = False, verbose: bool = False, query_id: Optional[str] = None, desc: Optional[str] = None) -> Any:
        ...

    # Public Method: Stream query results into a local file.
    def execute_query_stream_to_file(self, engine_name: str, query: str, schema_name: Optional[str] = None, params: Optional[dict] = None, fetch_size: int = 1000, output_file: Optional[str] = None, verbose: bool = False, query_id: Optional[str] = None) -> Any:
        ...

    # Public Method: Execute a SQL file through the MySQL client binary.
    def execute_query_from_file(self, engine_name: str, file_path: str, database: Optional[str] = None, verbose: bool = False) -> Any:
        ...

    # Public Method: Execute a SQL script statement-by-statement, chunking UPDATEs.
    def execute_sql_statements_in_chunks(self, engine_name: str, sql: str, chunk_size: int = 100000, schema_mapping: Optional[dict] = None, show_progress: bool = True, verbose: bool = False, query_id: Optional[str] = None) -> None:
        ...

    # Public Method: Update rows matching simple equality conditions.
    def set_cells(self, engine_name: str, schema_name: str, table_name: str, set: tuple = (), where: tuple = (), verbose: bool = False) -> None:
        ...

    # Public Method: Select rows matching simple equality conditions.
    def get_cells(self, engine_name: str, schema_name: str, table_name: str, select: tuple = (), where: tuple = (), verbose: bool = False) -> list:
        ...

    # Public Method: Check whether a table exists in the schema.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        ...

    # Public Method: List the tables of a schema.
    def get_tables_in_schema(self, engine_name: str, schema_name: str, include_views: bool = False, filter_by: Any = False, use_regex: Any = False) -> list:
        ...

    # Public Method: Count the rows of a table.
    def count_rows_in_table(self, engine_name: str, schema_name: str, table_name: str, where_clause: Optional[str] = None) -> int:
        ...

    # Public Method: List the column names of a table.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list:
        ...

    # Public Method: Check whether a table has a column (falsy match on type).
    def has_column(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        ...

    # Public Method: Check whether a table has a column.
    def column_exists(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        ...

    # Public Method: Apply the configured column datatypes to a table.
    def apply_datatypes(self, engine_name: str, schema_name: str, table_name: str, datatypes_json: dict, display_elapsed_time: bool = False, estimated_num_rows: bool = False) -> None:
        ...

    # Public Method: Check whether a database exists.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        ...

    # Public Method: Create a database, optionally dropping an existing one.
    def create_database(self, engine_name: str, schema_name: str, drop_database: bool = False) -> None:
        ...

    # Public Method: Apply the configured keys to a table.
    def apply_keys(self, engine_name: str, schema_name: str, table_name: str, keys_json: dict, display_elapsed_time: bool = False, estimated_num_rows: bool = False) -> None:
        ...

    # Public Method: Check whether a key/index exists on a table.
    def key_exists(self, engine_name: str, schema_name: str, table_name: str, key_name: str) -> bool:
        ...

    # Public Method: Import a dumped database folder into one environment.
    def import_database(self, engine_name: str, schema_name: str, input_folder: str, create_keys_after_import: bool = False, ignore_existing: bool = False, verbose: bool = False, compress: bool = False) -> None:
        ...

    # Public Method: Return the SQLAlchemy engine pool of one environment.
    def get_engine(self, engine_name: str) -> Any:
        ...
