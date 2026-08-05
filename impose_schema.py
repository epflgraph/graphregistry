#!/usr/bin/env python3
"""Impose MySQL table definitions from a source schema onto a target schema.

Default use case:
    python impose_schema.py --dry-run
    python impose_schema.py --commit --yes

This makes the target schema (graphsearch_prod_mirror by default) match the
source schema (graphsearch_test by default) at the table-definition level.
The source schema is read-only; only the target schema is modified.

Existing target tables are migrated in place with ALTER TABLE statements.
Data is preserved. Missing target tables are created with CREATE TABLE ... LIKE.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH, REPO_ROOT


DEFAULT_ENGINE = "xaas_coresrv"
DEFAULT_SOURCE_SCHEMA = "graphsearch_test"
DEFAULT_TARGET_SCHEMA = "graphsearch_prod_mirror"
DEFAULT_TABLE_PATTERNS = [r"^Index_D_.+$", r"^Data_N_Object_T_PageProfile$"]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "schema_impositions"


# SQL reserved words that start an index/constraint clause in SHOW CREATE TABLE.
_INDEX_KEYWORDS = (
    "PRIMARY",
    "UNIQUE",
    "KEY",
    "INDEX",
    "CONSTRAINT",
    "FULLTEXT",
    "SPATIAL",
)


@dataclass(frozen=True)
class IndexDef:
    name: str
    columns: tuple[str, ...]
    is_unique: bool
    is_primary: bool


def _q(name: str) -> str:
    """Backtick-quote a single SQL identifier safely."""
    return f"`{name.replace('`', '``')}`"


def _qt(schema_name: str, table_name: str) -> str:
    """Backtick-quote a schema-qualified table name safely."""
    return f"{_q(schema_name)}.{_q(table_name)}"


def discover_tables(
    db: GraphDB,
    engine_name: str,
    schema_name: str,
    patterns: list[str] | None = None,
    include_views: bool = False,
) -> list[str]:
    """Return source table names matching the configured patterns."""
    regexes = patterns or DEFAULT_TABLE_PATTERNS
    candidates = db.get_tables_in_schema(
        engine_name=engine_name,
        schema_name=schema_name,
        include_views=include_views,
        use_regex=regexes,
    )
    # Skip internal/temporary names (e.g. backup or staging copies).
    return sorted(t for t in candidates if not t.startswith("_"))


def fetch_create_table_ddl(
    db: GraphDB, engine_name: str, schema_name: str, table_name: str
) -> str:
    """Return the raw SHOW CREATE TABLE result for a table."""
    if hasattr(db, "get_create_table"):
        return db.get_create_table(engine_name, schema_name, table_name)

    query = f"SHOW CREATE TABLE {_qt(schema_name, table_name)}"
    rows = db.execute_query(engine_name=engine_name, query=query)
    if not rows:
        raise RuntimeError(f"Could not retrieve DDL for {_qt(schema_name, table_name)}")
    return rows[0][1]


def parse_column_definitions(ddl: str) -> dict[str, str]:
    """Parse a SHOW CREATE TABLE result into {column_name: column_definition}.

    The definition includes the column name and all attributes (type, null,
    default, auto_increment, comment, charset/collation).
    """
    # Extract the body between the first '(' and the matching final ')'.
    first_paren = ddl.find("(")
    last_paren = ddl.rfind(")")
    if first_paren == -1 or last_paren == -1 or last_paren <= first_paren:
        raise ValueError("Could not parse CREATE TABLE body")

    body = ddl[first_paren + 1 : last_paren]

    # Split the body into top-level clauses, respecting parentheses so that
    # ENUM('a','b'), DECIMAL(10,2), etc. are not split apart.
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_string = False
    string_char = ""
    i = 0
    while i < len(body):
        char = body[i]

        if in_string:
            current.append(char)
            if char == string_char:
                # Check for escaped quote (doubled quote inside string).
                if i + 1 < len(body) and body[i + 1] == string_char:
                    current.append(body[i + 1])
                    i += 2
                    continue
                in_string = False
            i += 1
            continue

        if char in ("'", '"'):
            in_string = True
            string_char = char
            current.append(char)
            i += 1
            continue

        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            # Top-level comma separates clauses.
            clause = "".join(current).strip()
            if clause:
                parts.append(clause)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    trailing = "".join(current).strip()
    if trailing:
        parts.append(trailing)

    columns: dict[str, str] = {}
    for part in parts:
        if not part:
            continue

        # Skip index/constraint clauses.
        first_token = part.split()[0].upper()
        if first_token in _INDEX_KEYWORDS or first_token.startswith("CONSTRAINT"):
            continue

        # Column name is the first backtick-quoted or bare identifier.
        match = re.match(r"(?:`([^`]+)`|(\w+))", part)
        if not match:
            continue
        col_name = match.group(1) or match.group(2)
        columns[col_name] = part

    return columns


def normalize_column_def(definition: str) -> str:
    """Normalize a column definition for comparison.

    Strips leading/trailing whitespace and trailing commas.
    """
    return definition.strip().rstrip(",")


def get_indexes_from_info_schema(
    db: GraphDB, engine_name: str, schema_name: str, table_name: str
) -> dict[str, IndexDef]:
    """Return {index_name: IndexDef} from INFORMATION_SCHEMA.STATISTICS."""
    query = f"""
        SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, NON_UNIQUE
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = {repr(schema_name)}
          AND TABLE_NAME   = {repr(table_name)}
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
    """
    index_columns: dict[str, list[tuple[str, int | None]]] = {}
    index_unique: dict[str, bool] = {}
    for row in db.execute_query(engine_name=engine_name, query=query):
        index_name, _seq, col_name, sub_part, non_unique = row
        if index_name not in index_columns:
            index_columns[index_name] = []
            index_unique[index_name] = non_unique == 0
        col_spec = col_name if sub_part is None else f"{col_name}({sub_part})"
        index_columns[index_name].append((col_spec, sub_part))

    indexes: dict[str, IndexDef] = {}
    for name, cols in index_columns.items():
        col_strs = tuple(c[0] for c in cols)
        is_primary = name == "PRIMARY"
        indexes[name] = IndexDef(
            name=name,
            columns=col_strs,
            is_unique=index_unique[name],
            is_primary=is_primary,
        )
    return indexes


def build_column_alterations(
    source_columns: dict[str, str],
    target_columns: dict[str, str],
    target_qualified: str,
    drop_extra_columns: bool,
) -> list[str]:
    """Build ALTER TABLE sub-statements for column differences."""
    alterations: list[str] = []

    for col_name, src_def in source_columns.items():
        src_norm = normalize_column_def(src_def)
        if col_name not in target_columns:
            alterations.append(f"ADD COLUMN {src_norm}")
        else:
            tgt_norm = normalize_column_def(target_columns[col_name])
            if src_norm != tgt_norm:
                alterations.append(f"MODIFY COLUMN {src_norm}")

    if drop_extra_columns:
        for col_name in target_columns:
            if col_name not in source_columns:
                alterations.append(f"DROP COLUMN {_q(col_name)}")

    return alterations


def build_index_alterations(
    source_indexes: dict[str, IndexDef],
    target_indexes: dict[str, IndexDef],
    target_qualified: str,
    drop_extra_indexes: bool,
) -> list[str]:
    """Build ALTER TABLE sub-statements for index differences."""
    alterations: list[str] = []

    for idx_name, src_idx in source_indexes.items():
        if idx_name not in target_indexes:
            cols = ", ".join(src_idx.columns)
            if src_idx.is_primary:
                alterations.append(f"ADD PRIMARY KEY ({cols})")
            elif src_idx.is_unique:
                alterations.append(f"ADD UNIQUE INDEX {_q(idx_name)} ({cols})")
            else:
                alterations.append(f"ADD INDEX {_q(idx_name)} ({cols})")
        elif target_indexes[idx_name] != src_idx:
            # Definition differs: drop and re-add.
            if idx_name == "PRIMARY":
                alterations.append("DROP PRIMARY KEY")
            else:
                alterations.append(f"DROP INDEX {_q(idx_name)}")
            cols = ", ".join(src_idx.columns)
            if src_idx.is_primary:
                alterations.append(f"ADD PRIMARY KEY ({cols})")
            elif src_idx.is_unique:
                alterations.append(f"ADD UNIQUE INDEX {_q(idx_name)} ({cols})")
            else:
                alterations.append(f"ADD INDEX {_q(idx_name)} ({cols})")

    if drop_extra_indexes:
        for idx_name in target_indexes:
            if idx_name not in source_indexes:
                if idx_name == "PRIMARY":
                    alterations.append("DROP PRIMARY KEY")
                else:
                    alterations.append(f"DROP INDEX {_q(idx_name)}")

    return alterations


def build_impose_statements(
    db: GraphDB,
    engine_name: str,
    source_schema: str,
    target_schema: str,
    table_names: Iterable[str],
    drop_extra: bool,
    drop_extra_columns: bool,
    drop_extra_indexes: bool,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Build the SQL statements needed to impose source definitions on target.

    Returns:
        create_statements:     CREATE TABLE ... LIKE statements for missing tables.
        alter_statements:      ALTER TABLE statements for existing tables.
        drop_extra_statements: DROP TABLE statements for target-only tables (if requested).
        no_change_statements:  Comments for target tables that already match the source.
    """
    target_tables = set(
        db.get_tables_in_schema(
            engine_name=engine_name,
            schema_name=target_schema,
            include_views=False,
        )
    )

    create_statements: list[str] = []
    alter_statements: list[str] = []
    no_change_statements: list[str] = []

    for table_name in table_names:
        source_qualified = _qt(source_schema, table_name)
        target_qualified = _qt(target_schema, table_name)

        if table_name not in target_tables:
            create_statements.append(
                f"CREATE TABLE IF NOT EXISTS {target_qualified} LIKE {source_qualified};"
            )
            continue

        source_columns = parse_column_definitions(
            fetch_create_table_ddl(db, engine_name, source_schema, table_name)
        )
        target_columns = parse_column_definitions(
            fetch_create_table_ddl(db, engine_name, target_schema, table_name)
        )

        source_indexes = get_indexes_from_info_schema(db, engine_name, source_schema, table_name)
        target_indexes = get_indexes_from_info_schema(db, engine_name, target_schema, table_name)

        alterations: list[str] = []
        alterations.extend(
            build_column_alterations(
                source_columns, target_columns, target_qualified, drop_extra_columns
            )
        )
        alterations.extend(
            build_index_alterations(
                source_indexes, target_indexes, target_qualified, drop_extra_indexes
            )
        )

        if alterations:
            alter_statements.append(
                f"ALTER TABLE {target_qualified}\n  " + ",\n  ".join(alterations) + ";"
            )
        else:
            no_change_statements.append(
                f"-- {target_qualified} already matches {source_qualified}; no action needed."
            )

    drop_extra_statements: list[str] = []
    if drop_extra:
        source_tables = set(table_names)
        for table_name in sorted(target_tables):
            if table_name not in source_tables:
                drop_extra_statements.append(
                    f"DROP TABLE IF EXISTS {_qt(target_schema, table_name)};"
                )

    return create_statements, alter_statements, drop_extra_statements, no_change_statements


def execute_statements(
    db: GraphDB,
    engine_name: str,
    statements: list[str],
    commit: bool,
    verbose: bool,
) -> None:
    """Execute a list of SQL statements if commit is True; print them otherwise."""
    for statement in statements:
        stripped = statement.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if commit:
            if verbose:
                print(f"Executing: {statement}")
            db.execute_query(engine_name=engine_name, query=statement, commit=True)
        else:
            print(statement)


def write_review_file(
    output_path: Path,
    create_statements: list[str],
    alter_statements: list[str],
    drop_extra_statements: list[str],
    no_change_statements: list[str],
    source_schema: str,
    target_schema: str,
) -> None:
    """Write all generated DDL to a timestamped file for manual review."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"-- Schema imposition plan generated at {datetime.now().isoformat()}",
        f"-- Source schema: {source_schema}",
        f"-- Target schema: {target_schema}",
        "--",
        "-- Existing target tables are altered in place; data is preserved.",
        "-- Missing target tables are created with CREATE TABLE ... LIKE.",
        "--",
        "",
    ]

    if create_statements:
        lines.append("-- Tables to create")
        lines.extend(create_statements)
        lines.append("")

    if alter_statements:
        lines.append("-- Tables to alter")
        lines.extend(alter_statements)
        lines.append("")

    if drop_extra_statements:
        lines.append("-- Extra target-only tables to drop")
        lines.extend(drop_extra_statements)
        lines.append("")

    if no_change_statements:
        lines.append("-- Tables already matching (no action)")
        lines.extend(no_change_statements)
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Impose MySQL table definitions from a source schema onto a target schema."
    )
    parser.add_argument(
        "--source-schema",
        default=DEFAULT_SOURCE_SCHEMA,
        help=f"Source schema name (default: {DEFAULT_SOURCE_SCHEMA})",
    )
    parser.add_argument(
        "--target-schema",
        default=DEFAULT_TARGET_SCHEMA,
        help=f"Target schema name (default: {DEFAULT_TARGET_SCHEMA})",
    )
    parser.add_argument(
        "--engine",
        default=DEFAULT_ENGINE,
        help=f"GraphDB engine name (default: {DEFAULT_ENGINE})",
    )
    parser.add_argument(
        "--table-pattern",
        action="append",
        dest="table_patterns",
        help="Regex for table names to consider (can be given multiple times). "
             f"Defaults to {DEFAULT_TABLE_PATTERNS}.",
    )
    parser.add_argument(
        "--include-views",
        action="store_true",
        help="Include views when discovering source tables (default: tables only).",
    )
    parser.add_argument(
        "--drop-extra",
        action="store_true",
        help="Drop target tables that do not exist in the source schema.",
    )
    parser.add_argument(
        "--drop-extra-columns",
        action="store_true",
        help="Drop target columns that do not exist in the source table definitions.",
    )
    parser.add_argument(
        "--drop-extra-indexes",
        action="store_true",
        help="Drop target indexes that do not exist in the source table definitions.",
    )
    parser.add_argument(
        "--create-target-schema",
        action="store_true",
        help="Create the target schema if it does not exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview generated DDL without executing it (default).",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually execute DDL against the target schema.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation when --commit is used.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory where the review SQL file is written (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each statement as it is executed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    source_schema = args.source_schema
    target_schema = args.target_schema
    engine_name = args.engine
    table_patterns = args.table_patterns or DEFAULT_TABLE_PATTERNS

    db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    db = GraphDB(config=db_cfg)
    glbcfg = GlobalConfig.from_file()

    # Resolve schema names from config keys if a short key was provided.
    db_schema_names = glbcfg.settings.get("mysql", {}).get("db_schema_names", {})
    source_schema = db_schema_names.get(source_schema, source_schema)
    target_schema = db_schema_names.get(target_schema, target_schema)

    if source_schema == target_schema:
        print(
            "❌ Source and target schema must be different.",
            file=sys.stderr,
        )
        return 1

    # Verify source schema exists.
    if not db.database_exists(engine_name=engine_name, schema_name=source_schema):
        print(f"❌ Source schema does not exist: {source_schema}", file=sys.stderr)
        return 1

    # Handle target schema existence.
    if not db.database_exists(engine_name=engine_name, schema_name=target_schema):
        if args.create_target_schema:
            print(f"⚙️  Target schema does not exist; creating {target_schema}.")
            if args.commit:
                db.create_database(engine_name=engine_name, schema_name=target_schema)
        else:
            print(
                f"❌ Target schema does not exist: {target_schema}. "
                "Use --create-target-schema to create it.",
                file=sys.stderr,
            )
            return 1

    # Discover tables to impose.
    print(f"🔍 Discovering tables in {source_schema} matching {table_patterns} ...")
    source_tables = discover_tables(
        db=db,
        engine_name=engine_name,
        schema_name=source_schema,
        patterns=table_patterns,
        include_views=args.include_views,
    )
    print(f"   Found {len(source_tables)} tables to impose.")

    (
        create_statements,
        alter_statements,
        drop_extra_statements,
        no_change_statements,
    ) = build_impose_statements(
        db=db,
        engine_name=engine_name,
        source_schema=source_schema,
        target_schema=target_schema,
        table_names=source_tables,
        drop_extra=args.drop_extra,
        drop_extra_columns=args.drop_extra_columns,
        drop_extra_indexes=args.drop_extra_indexes,
    )

    # Summary.
    create_count = len(create_statements)
    alter_count = len(alter_statements)
    unchanged_count = len(no_change_statements)
    extra_count = len(drop_extra_statements)

    print(f"\n📋 Imposition plan:")
    print(f"   - Source schema: {source_schema}")
    print(f"   - Target schema: {target_schema}")
    print(f"   - Missing tables to create: {create_count}")
    print(f"   - Existing tables to alter: {alter_count}")
    print(f"   - Tables already matching: {unchanged_count}")
    if args.drop_extra:
        print(f"   - Extra target tables to drop: {extra_count}")

    # Write review file regardless of commit mode.
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    review_path = args.output_dir / f"{source_schema}_to_{target_schema}_{timestamp}.sql"
    write_review_file(
        output_path=review_path,
        create_statements=create_statements,
        alter_statements=alter_statements,
        drop_extra_statements=drop_extra_statements,
        no_change_statements=no_change_statements,
        source_schema=source_schema,
        target_schema=target_schema,
    )
    print(f"\n📝 Review file written: {review_path}")

    # Safety confirmation for commit mode.
    if args.commit:
        destructive = bool(drop_extra_statements)
        if destructive:
            print(
                "\n⚠️  WARNING: --drop-extra will destroy data in extra target tables."
            )
        if destructive or args.drop_extra_columns or args.drop_extra_indexes:
            if not args.yes:
                try:
                    answer = input("   Type 'yes' to proceed: ")
                except (EOFError, KeyboardInterrupt):
                    print("\n❌ Aborted.")
                    return 130
                if answer.strip().lower() != "yes":
                    print("❌ Aborted.")
                    return 1

        execute_statements(
            db=db,
            engine_name=engine_name,
            statements=create_statements + alter_statements + drop_extra_statements,
            commit=True,
            verbose=args.verbose,
        )
        print("\n✅ Schema imposition committed.")
    else:
        print("\n💡 Running in dry-run mode (no changes were made). Use --commit to apply.")
        print("\n-- Generated DDL --")
        for statement in create_statements + alter_statements + drop_extra_statements:
            print(statement)

    return 0


if __name__ == "__main__":
    sys.exit(main())
