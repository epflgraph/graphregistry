# graphregistry/scripts/integrity_checks/schemas/scan_datatypes.py
"""
Compare live MySQL column definitions against the canonical definitions in
scripts/integrity_checks/schemas/datatypes.json and verify that tables use the
expected collation (utf8mb4_bin).

The JSON values are full column definitions, e.g.:
    "row_id": "bigint(20) unsigned NOT NULL AUTO_INCREMENT"
    "object_type": "varchar(32) NOT NULL"

For comparison, integer display widths are normalized away (they are
semantically meaningless and deprecated in MySQL 8). Remediation SQL uses the
control definition verbatim.

EDIT: Normalisation has been deprecated in this version
"""
import argparse
import json
import os
import re
from collections import defaultdict
from pathlib import Path
import rich
from rich.console import Console
from graphdb.core.graphdb import GraphDB
from graphdb.models.sqlquery import print_sql
from graphregistry.common.dbstruct import sql_data_type_mapping

# Console for colored verbose output.
console = Console()

# Datatypes path in database/init/config/system_datatypes.json
CONTROL_PATH = Path(__file__).parent.parent.parent.parent / "database/init/config/system_datatypes.json"

# Index-specific abstract datatypes from config/config_index.json
INDEX_CONFIG_PATH = Path(__file__).parent.parent.parent.parent / "config/config_index.json"

# Special sentinel column that can be suppressed with -nr.
ROW_ID_COLUMN = "row_id"

# Schemas to scan.
SCHEMAS = [
    "graph_airflow",
    "graph_registry",
    "graph_lectures",
    "graph_cache",
    "_1_DEV_graph_airflow",
    "_1_DEV_graph_registry",
    "_1_DEV_graph_lectures",
    "_1_DEV_graph_cache",
    "graphsearch_test",
    "elasticsearch_cache",
]

# Expected charset/collation for tables and text columns.
EXPECTED_CHARSET = "utf8mb4"
EXPECTED_COLLATION = "utf8mb4_bin"

# Schemas to skip in collation checks (e.g. read-only or external indexes).
SKIP_COLLATION_SCHEMAS = {
    # No schemas are currently skipped; all index tables must use utf8mb4_bin.
}

# Public Method: Split a full column definition into (type, attributes)
def split_definition(definition: str) -> tuple[str, str]:
    """
    Split a full column definition into (type, attributes).

    Attributes include NULL/NOT NULL, DEFAULT, AUTO_INCREMENT, ON UPDATE, etc.
    """
    tokens = definition.strip().split()
    attribute_keywords = {
        "not", "null", "default", "auto_increment",
        "character", "collate", "comment", "on",
    }

    # Prepare type_tokens for the following steps.
    type_tokens = []
    attr_tokens = []
    for token in tokens:
        lower = token.lower()
        if lower in attribute_keywords and not attr_tokens:
            attr_tokens.append(token)
        elif attr_tokens:
            attr_tokens.append(token)
        else:
            type_tokens.append(token)

    # Return the computed result.
    return " ".join(type_tokens), " ".join(attr_tokens)

# Public Method: Ensure the control definition for row_id includes AUTO_INCREMENT
def assert_row_id_auto_increment(control: dict[str, str]) -> None:
    """Ensure the control definition for row_id includes AUTO_INCREMENT."""
    if "row_id" not in control:
        raise ValueError("datatypes.json must define a 'row_id' column")
    if "auto_increment" not in control["row_id"].lower():
        raise ValueError(
            "Assertion failed: 'row_id' in datatypes.json must be defined as AUTO_INCREMENT"
        )

# Public Method: load control
def load_control() -> dict[str, str]:
    with CONTROL_PATH.open("r", encoding="utf-8") as f:
        control = json.load(f)
    if not isinstance(control, dict):
        raise ValueError("datatypes.json must be a flat object")
    assert_row_id_auto_increment(control)
    return control

# Public Method: Load and convert config_index.json abstract datatypes into SQL definitions.
def load_index_control() -> dict[str, str]:
    """Load config_index.json data-types and map them to SQL column types."""
    with INDEX_CONFIG_PATH.open("r", encoding="utf-8") as f:
        index_config = json.load(f)
    abstract_types = index_config.get("data-types", {})
    if not isinstance(abstract_types, dict):
        raise ValueError("config_index.json 'data-types' must be a flat object")

    # Declare the control data structure.
    control: dict[str, str] = {}
    for field_name, abstract_type in abstract_types.items():
        if abstract_type not in sql_data_type_mapping:
            raise ValueError(
                f"Unknown abstract datatype '{abstract_type}' for field '{field_name}'"
            )
        control[field_name] = sql_data_type_mapping[abstract_type]
    return control

# Public Method: Decide whether a table should also be checked against index datatypes.
def table_uses_index_datatypes(schema_name: str, table_name: str) -> bool:
    """Index-specific datatypes apply to graphsearch_test, elasticsearch_cache,
    and graph_cache IndexBuildup_* tables.
    """
    if schema_name in {"graphsearch_test", "elasticsearch_cache"}:
        return True
    if schema_name == "graph_cache" and table_name.startswith("IndexBuildup_"):
        return True
    return False

# Public Method: Return column metadata from INFORMATION_SCHEMA
def fetch_column_metadata(db, engine_name, schema_name, table_name):
    """Return column metadata from INFORMATION_SCHEMA.COLUMNS."""
    query = f"""
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
    """
    metadata = {}
    for row in db.execute_query(engine_name=engine_name, query=query):
        metadata[row[0]] = {
            "column_type"    : row[1],
            "is_nullable"    : row[2],
            "column_default" : row[3],
            "extra"          : row[4] or "",
        }
    return metadata

# Public Method: Return the table-level collation from INFORMATION_SCHEMA.TABLES.
def fetch_table_collation(db, engine_name, schema_name, table_name):
    """Return the table-level collation from INFORMATION_SCHEMA.TABLES."""
    query = f"""
        SELECT TABLE_COLLATION
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
    """
    rows = list(db.execute_query(engine_name=engine_name, query=query))
    return rows[0][0] if rows else None

# Public Method: Build a full definition string from column metadata
def build_actual_definition(metadata: dict) -> str:
    """Build a full definition string from column metadata."""
    parts = [metadata["column_type"]]

    # Handle the conditional case.
    if metadata["is_nullable"] == "NO":
        parts.append("NOT NULL")
    else:
        parts.append("NULL")

    # Prepare default for the following steps.
    default = metadata["column_default"]
    if default is not None:
        if isinstance(default, str) and default.upper() == "NULL":
            parts.append("DEFAULT NULL")
        elif isinstance(default, str) and default.upper() not in ("CURRENT_TIMESTAMP",):
            parts.append(f"DEFAULT '{default}'")
        else:
            parts.append(f"DEFAULT {default}")

    # Prepare extra for the following steps.
    extra = metadata["extra"].strip()
    if extra:
        parts.append(extra.upper())

    # Return the computed result.
    return " ".join(parts)

# Public Method: Fix control definitions so MySQL accepts them
def clean_definition_for_sql(definition: str) -> str:
    """
    Fix control definitions so MySQL accepts them.

    schema_definitions.json often returns CURRENT_TIMESTAMP quoted as
    'current_timestamp()'; MySQL rejects DEFAULT 'current_timestamp()'.
    """
    definition = re.sub(
        r"DEFAULT\s+'current_timestamp\(\)'",
        "DEFAULT CURRENT_TIMESTAMP",
        definition,
        flags=re.IGNORECASE,
    )
    definition = re.sub(
        r"ON UPDATE\s+'current_timestamp\(\)'",
        "ON UPDATE CURRENT_TIMESTAMP",
        definition,
        flags=re.IGNORECASE,
    )
    return definition

# Public Method: Build a MODIFY COLUMN clause using the cleaned control definition
def build_modify_clause(column_name: str, control_definition: str) -> str:
    """Build a MODIFY COLUMN clause using the cleaned control definition."""
    return f"`{column_name}` {clean_definition_for_sql(control_definition)}"

# Public Method: Print a colorful per-table comparison of actual vs expected column types.
def print_verbose_report(
    verbose_log: dict[tuple[str, str], list[dict]],
    errors_only: bool = False,
) -> None:
    """Print a per-table, per-column comparison of actual vs expected definitions.

    When errors_only is True, only mismatched columns are shown.
    """
    if not verbose_log:
        return

    # Iterate over the collection.
    for (schema, table), columns in sorted(verbose_log.items()):
        if errors_only:
            columns = [c for c in columns if not c["match"]]
            if not columns:
                continue

        # Continue with the next step.
        console.print(f"\n📋 [bold]{schema}.{table}[/bold]")
        for col in columns:
            column_name = col["column"]
            actual = col["actual"]
            expected = col["expected"]
            if col["match"]:
                console.print(f"  [green]✅ {column_name}[/green]")
                console.print(f"     actual:   {actual}")
                console.print(f"     expected: {expected}")
            else:
                console.print(f"  [red]❌ {column_name}[/red]")
                console.print(f"     [red]actual:   {actual}[/red]")
                console.print(f"     [green]expected: {expected}[/green]")

# Public Method: Print a histogram of actual vs expected datatype mismatches.
def print_mismatch_histogram(type_mismatches: list[dict]) -> None:
    """Print a histogram of how often each actual/expected datatype pair occurs."""
    if not type_mismatches:
        return

    # Declare the counts data structure.
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for m in type_mismatches:
        counts[(m["actual"], m["expected"])] += 1

    # Sort by frequency descending, then by actual/expected text for stability.
    sorted_pairs = sorted(counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))

    # Continue with the next step.
    console.print("\n📊 [bold]Mismatch histogram[/bold] (actual → expected)")
    for (actual, expected), count in sorted_pairs:
        console.print(
            f"  [red]{count:>3}[/red] × "
            f"[red]{actual}[/red]  →  [green]{expected}[/green]"
        )

# Public Method: main
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compare live MySQL columns against system_datatypes.json and "
            "config_index.json data-types, check utf8mb4_bin collation, "
            "and generate or execute remediation SQL."
        )
    )
    parser.add_argument(
        "-nr",
        "--no-row-id-only",
        action = "store_true",
        help   = "Do not print ALTER TABLE statements that only change the row_id column.",
    )
    parser.add_argument(
        "-x",
        "--execute",
        action = "store_true",
        help   = "Execute the generated ALTER TABLE statements. DDL is auto-committed.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action = "store_true",
        help   = "Print a per-table, per-column comparison of actual vs expected definitions.",
    )
    parser.add_argument(
        "-e",
        "--errors-only",
        action = "store_true",
        help   = "With --verbose, show only columns whose actual type does not match the config.",
    )
    args = parser.parse_args()

    # --errors-only is meaningless without the comparison report, so imply --verbose.
    if args.errors_only:
        args.verbose = True

    # Prepare db for the following steps.
    db = GraphDB()
    engine_name = "xaas_coresrv"

    # Prepare control for the following steps.
    control = load_control()
    index_control = load_index_control()

    # Normalized lookups for the two control sources.
    control_normalized = {
        col: defn for col, defn in control.items()
    }
    index_control_normalized = {
        col: defn for col, defn in index_control.items()
    }

    # Prepare type_mismatches for the following steps.
    type_mismatches = []
    table_collation_mismatches = []
    verbose_log: dict[tuple[str, str], list[dict]] = defaultdict(list)

    # Iterate over the collection.
    for schema_name in SCHEMAS:
        tables = [
            t
            for t in db.get_tables_in_schema(
                engine_name=engine_name, schema_name=schema_name
            )
            if not t.startswith("_")
        ]

        # Iterate over the collection.
        for table_name in tables:

            # Handle the conditional case.
            if os.path.exists('abort'):
                print('Script aborted by request.')
                exit()

            # if '_AS' in table_name or '_GBC' in table_name or 'score' in table_name.lower():
            #     continue

            # Index tables also inherit the abstract datatypes from config_index.json.
            # System datatypes take precedence when a field exists in both sources.
            if table_uses_index_datatypes(schema_name, table_name):
                table_control = {**index_control, **control}
                table_control_normalized = {**index_control_normalized, **control_normalized}
            else:
                table_control = control
                table_control_normalized = control_normalized

            # Prepare metadata for the following steps.
            metadata = fetch_column_metadata(
                db, engine_name, schema_name, table_name
            )

            # Detect tables that do not use the case- and accent-sensitive
            # utf8mb4_bin collation, which can cause incorrect lookups or duplicate keys.
            if schema_name not in SKIP_COLLATION_SCHEMAS:
                table_collation = fetch_table_collation(
                    db, engine_name, schema_name, table_name
                )
                if table_collation and table_collation.lower() != EXPECTED_COLLATION:
                    table_collation_mismatches.append(
                        {
                            "schema"   : schema_name,
                            "table"    : table_name,
                            "actual"   : table_collation,
                            "expected" : EXPECTED_COLLATION,
                        }
                    )

            # Iterate over the collection.
            for column_name, col_meta in metadata.items():
                # Only evaluate columns defined in the table-specific control.
                if column_name not in table_control:
                    continue

                # Prepare actual_definition for the following steps.
                actual_definition = build_actual_definition(col_meta)
                actual_normalized = actual_definition
                expected_definition = table_control[column_name]
                expected_normalized = table_control_normalized[column_name]
                is_match = actual_normalized == expected_normalized

                # Handle the conditional case.
                if args.verbose:
                    verbose_log[(schema_name, table_name)].append(
                        {
                            "column"   : column_name,
                            "actual"   : actual_definition,
                            "expected" : expected_definition,
                            "match"    : is_match,
                        }
                    )

                # Handle the conditional case.
                if not is_match:
                    type_mismatches.append(
                        {
                            "schema"   : schema_name,
                            "table"    : table_name,
                            "column"   : column_name,
                            "actual"   : actual_definition,
                            "expected" : expected_definition,
                        }
                    )

    # Public Method: Build a single ALTER TABLE per table combining type and collation fixes.
    def build_combined_statements(
        type_mismatches: list[dict],
        table_collation_mismatches: list[dict],
        skip_row_id_only: bool,
    ) -> list[str]:
        """Build a single ALTER TABLE per table combining type and collation fixes."""
        type_grouped = defaultdict(list)
        for m in type_mismatches:
            type_grouped[(m["schema"], m["table"])].append(m)

        # Prepare collation_set for the following steps.
        collation_set = {(m["schema"], m["table"]) for m in table_collation_mismatches}

        # Prepare statements for the following steps.
        statements = []
        for schema, table in sorted(set(type_grouped.keys()) | collation_set):
            modifications = []

            # Handle the conditional case.
            if (schema, table) in collation_set:
                modifications.append(
                    f"    CONVERT TO CHARACTER SET {EXPECTED_CHARSET} COLLATE {EXPECTED_COLLATION}"
                )

            # Prepare columns for the following steps.
            columns = type_grouped[(schema, table)]
            if skip_row_id_only and {m["column"] for m in columns} == {ROW_ID_COLUMN}:
                if not modifications:
                    continue

            # Iterate over the collection.
            for m in columns:
                modifications.append(
                    "    MODIFY COLUMN "
                    + build_modify_clause(m["column"], m["expected"])
                )

            # Skip tables that have no modifications to apply.
            if not modifications:
                continue

            # Compose a single ALTER TABLE statement for this schema.table.
            stmt = (
                f"ALTER TABLE `{schema}`.`{table}`\n"
                + ",\n".join(modifications)
                + ";"
            )
            statements.append(stmt)
        return statements

    # --------------------------------------------------
    # Report results.
    # --------------------------------------------------

    # Print the per-table comparison when verbose mode is enabled.
    if args.verbose:
        print_verbose_report(verbose_log, errors_only=args.errors_only)

    # Print remediation SQL when any mismatch was detected.
        if args.no_row_id_only and type_mismatches:
            print("-- -nr enabled: hiding ALTER TABLE statements that only change row_id.\n")
        if args.execute:
            print("-- EXECUTING generated ALTER TABLE statements. DDL is auto-committed.\n")
        else:
            print("-- Review before running. Definitions are taken verbatim from system_datatypes.json")
            print("-- and config_index.json data-types.")
            print("-- Expected table collation: CHARACTER SET utf8mb4 COLLATE utf8mb4_bin.\n")

        # Build and optionally execute remediation ALTER TABLE statements.
        statements = build_combined_statements(
            type_mismatches,
            table_collation_mismatches,
            skip_row_id_only=args.no_row_id_only,
        )
        total = len(statements)
        for idx, stmt in enumerate(statements, start=1):
            print(f"-- [{idx}/{total}]")
            print_sql(stmt, title="Remediation")
            if args.execute:
                try:
                    db.execute_query(engine_name=engine_name, query=stmt)
                    print("  -> OK")
                except Exception as exc:
                    print(f"  -> FAILED: {exc}")
            print()
    else:
        print("All defined columns match the canonical definitions and table collation is utf8mb4_bin.")

    # Always print the mismatch histogram when there are datatype mismatches.
    print_mismatch_histogram(type_mismatches)

# Run the scan when this script is executed directly.
if __name__ == "__main__":
    main()
