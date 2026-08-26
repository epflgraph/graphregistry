# graphregistry/adapters/persistence/mysql/repositories/_helpers.py
"""Shared SQL helpers for MySQL repository adapters.

These functions are intentionally low-level and stateless so they can be reused
by node, edge, and future repository adapters without forcing an inheritance
relationship.
"""
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.session import MySQLSession

#================================================================#
# Function Group: SQL identifier helpers                         #
#================================================================#

# Public Method: Backtick-quote a single SQL identifier safely.
def quote_identifier(name: str) -> str:
    """Backtick-quote a single SQL identifier safely."""
    return f"`{name.replace('`', '``')}`"

# Public Method: Return a safely quoted schema-qualified table name.
def qualified_table(schema_name: str, table_name: str) -> str:
    """Return a safely quoted schema-qualified table name."""
    return f"{quote_identifier(schema_name)}.{quote_identifier(table_name)}"

#================================================================#
# Function Group: Key predicate helpers                          #
#================================================================#

#----------------------------------------------------------------#
# Internal Function: Build a parameterized ``(col1, col2, ...) IN (...)``
# clause.
#----------------------------------------------------------------#
# Public Method: key tuple in list predicate
def key_tuple_in_list_predicate(
    key_tuples: list[tuple[Any, ...]],
    key_column_names: list[str],
    prefix: str = "key",
) -> tuple[str, dict[str, Any]]:
#----------------------------------------------------------------#
    """Build a parameterized ``(col1, col2, ...) IN (...)`` clause.

    Returns a tuple of (placeholders_sql, params_dict).
    """
    if not key_tuples:
        return "FALSE", {}

    # Declare the placeholders data structure.
    placeholders: list[str] = []
    params: dict[str, Any] = {}

    # Build per-row placeholders and bind each key-column value.
    for i, key_tuple in enumerate(key_tuples):
        row_placeholders = [f":{prefix}_{col}_{i}" for col in key_column_names]
        placeholders.append(f"({', '.join(row_placeholders)})")
        for col, value in zip(key_column_names, key_tuple):
            params[f"{prefix}_{col}_{i}"] = value

    # Return the computed result.
    return ", ".join(placeholders), params

#================================================================#
# Function Group: Batch write helpers                            #
#================================================================#

#----------------------------------------------------------------#
# Internal Function: Insert or update a batch of rows in a single statement.
#----------------------------------------------------------------#
# Public Method: upsert rows
def upsert_rows(
    session: MySQLSession,
    table_path: str,
    key_column_names: list[str],
    upd_column_names: list[str],
    rows: list[dict[str, Any]],
) -> None:
#----------------------------------------------------------------#
    """Insert or update a batch of rows in a single statement.

    Mirrors the safe-upsert semantics used by the legacy graphdb client:
    unchanged columns keep their value and ``record_updated_date`` is only
    touched when at least one column changes.
    """
    if not rows:
        return

    # Prepare all_column_names for the following steps.
    all_column_names = key_column_names + upd_column_names
    value_placeholders: list[str] = []
    params: dict[str, Any] = {}

    # Build per-row value placeholders and bind every column value.
    for i, row in enumerate(rows):
        row_placeholders = [f":{col}_{i}" for col in all_column_names]
        value_placeholders.append(f"({', '.join(row_placeholders)})")
        for col in all_column_names:
            params[f"{col}_{i}"] = row.get(col)

    # Handle the conditional case.
    if upd_column_names:
        changed_expr = " OR ".join(
            f"COALESCE({table_path}.{col}, '__null__') != COALESCE(VALUES({col}), '__null__')"
            for col in upd_column_names
        )
        update_clauses = [
            f"{col} = IF(COALESCE({table_path}.{col}, '__null__') != COALESCE(VALUES({col}), '__null__'), VALUES({col}), {table_path}.{col})"
            for col in upd_column_names
        ]
        update_sql = (
            f"record_updated_date = IF({changed_expr}, CURRENT_TIMESTAMP, {table_path}.record_updated_date), "
            f"{', '.join(update_clauses)}"
        )
    else:
        # Insert-ignore semantics when there are no update columns.
        update_sql = "record_updated_date = record_updated_date"

    sql = f"""
        INSERT INTO {table_path} ({', '.join(all_column_names)})
        VALUES {', '.join(value_placeholders)}
        ON DUPLICATE KEY UPDATE
            {update_sql}
    """
    # Continue with the next step.
    session.execute(sql, params)

#----------------------------------------------------------------#
# Internal Function: Soft-delete rows whose key columns match the given tuples.
#----------------------------------------------------------------#
# Public Method: soft delete by key tuples
def soft_delete_by_key_tuples(
    session: MySQLSession,
    schema_name: str,
    table_name: str,
    key_column_names: list[str],
    key_tuples: list[tuple[Any, ...]],
) -> None:
#----------------------------------------------------------------#
    """Soft-delete rows whose key columns match the given tuples."""
    if not key_tuples:
        return

    # Continue with the next step.
    placeholders, params = key_tuple_in_list_predicate(key_tuples, key_column_names, prefix="sd")
    sql = f"""
        UPDATE {qualified_table(schema_name, table_name)}
           SET record_deleted = 1
         WHERE ({', '.join(key_column_names)}) IN ({placeholders})
    """
    session.execute(sql, params)
