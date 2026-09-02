#!/usr/bin/env python3
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
"""
import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
import rich, os
from graphdb.core.graphdb import GraphDB
from graphdb.models.sqlquery import print_sql


CONTROL_PATH = Path(__file__).parent / "datatypes.json"

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
    "graphsearch_test",
}


def normalize_type(data_type: str) -> str:
    """
    Normalize the type portion of a column definition for comparison.

    - Strip display width from integer types (bigint(20), int(10), etc.)
    - Normalize tinyint(N) to tinyint(1)
    - Preserve unsigned, enum values, character sets, etc.
    """
    data_type = data_type.lower().strip()

    # Convert square brackets to parentheses.
    data_type = re.sub(r"\[(\d+)\]", r"(\1)", data_type)

    # tinyint(4) -> tinyint(1)
    data_type = re.sub(r"tinyint\(\d+\)", "tinyint(1)", data_type)

    # bigint(20) unsigned -> bigint unsigned
    data_type = re.sub(r"bigint\(\d+\) unsigned", "bigint unsigned", data_type)
    # bigint(20) -> bigint
    data_type = re.sub(r"bigint\(\d+\)", "bigint", data_type)

    # int(10) unsigned -> int unsigned, int(11) -> int
    data_type = re.sub(r"int\(\d+\) unsigned", "int unsigned", data_type)
    data_type = re.sub(r"int\(\d+\)", "int", data_type)

    # smallint(5) unsigned -> smallint unsigned, etc.
    data_type = re.sub(r"smallint\(\d+\) unsigned", "smallint unsigned", data_type)
    data_type = re.sub(r"smallint\(\d+\)", "smallint", data_type)

    # mediumint
    data_type = re.sub(r"mediumint\(\d+\) unsigned", "mediumint unsigned", data_type)
    data_type = re.sub(r"mediumint\(\d+\)", "mediumint", data_type)

    # Collapse multiple spaces.
    data_type = re.sub(r"\s+", " ", data_type)

    return data_type


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

    return " ".join(type_tokens), " ".join(attr_tokens)


def normalize_default(default_clause: str) -> str:
    """Normalize a DEFAULT clause to avoid false positives."""
    default_clause = default_clause.strip()
    lower = default_clause.lower()

    # 'current_timestamp()' and CURRENT_TIMESTAMP are equivalent.
    if lower in ("'current_timestamp()'", "current_timestamp()", "current_timestamp"):
        return "DEFAULT CURRENT_TIMESTAMP"

    # Unquote numeric defaults ('0' -> 0, '1' -> 1).
    if re.fullmatch(r"'\d+'", default_clause):
        return f"DEFAULT {default_clause[1:-1]}"

    return f"DEFAULT {default_clause}"


def normalize_definition(definition: str) -> str:
    """Normalize a full column definition for comparison."""
    type_part, attr_part = split_definition(definition)
    normalized_type = normalize_type(type_part)
    # Normalize attribute order: NOT NULL / NULL, DEFAULT, AUTO_INCREMENT, rest.
    attr_lower = attr_part.lower()
    parts = []
    if "not null" in attr_lower:
        parts.append("NOT NULL")
    elif "null" in attr_lower:
        parts.append("NULL")
    else:
        # MySQL default is nullable when neither NULL nor NOT NULL is specified.
        parts.append("NULL")
    if "default" in attr_lower:
        match = re.search(
            r"default\s+(.+?)(?=(?:\s+(?:auto_increment|on|comment))|$)", attr_lower
        )
        if match:
            parts.append(normalize_default(match.group(1).strip()))
    if "auto_increment" in attr_lower:
        parts.append("AUTO_INCREMENT")
    return f"{normalized_type} {' '.join(parts)}".strip()


def assert_row_id_auto_increment(control: dict[str, str]) -> None:
    """Ensure the control definition for row_id includes AUTO_INCREMENT."""
    if "row_id" not in control:
        raise ValueError("datatypes.json must define a 'row_id' column")
    if "auto_increment" not in control["row_id"].lower():
        raise ValueError(
            "Assertion failed: 'row_id' in datatypes.json must be defined as AUTO_INCREMENT"
        )


def load_control() -> dict[str, str]:
    with CONTROL_PATH.open("r", encoding="utf-8") as f:
        control = json.load(f)
    if not isinstance(control, dict):
        raise ValueError("datatypes.json must be a flat object")
    assert_row_id_auto_increment(control)
    return control


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
            "column_type": row[1],
            "is_nullable": row[2],
            "column_default": row[3],
            "extra": row[4] or "",
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


def build_actual_definition(metadata: dict) -> str:
    """Build a full definition string from column metadata."""
    parts = [metadata["column_type"]]

    if metadata["is_nullable"] == "NO":
        parts.append("NOT NULL")
    else:
        parts.append("NULL")

    default = metadata["column_default"]
    if default is not None:
        if isinstance(default, str) and default.upper() == "NULL":
            parts.append("DEFAULT NULL")
        elif isinstance(default, str) and default.upper() not in ("CURRENT_TIMESTAMP",):
            parts.append(f"DEFAULT '{default}'")
        else:
            parts.append(f"DEFAULT {default}")

    extra = metadata["extra"].strip()
    if extra:
        parts.append(extra.upper())

    return " ".join(parts)


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


def build_modify_clause(column_name: str, control_definition: str) -> str:
    """Build a MODIFY COLUMN clause using the cleaned control definition."""
    return f"`{column_name}` {clean_definition_for_sql(control_definition)}"


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compare live MySQL columns against datatypes.json, check "
            "utf8mb4_bin collation, and generate or execute remediation SQL."
        )
    )
    parser.add_argument(
        "-nr",
        "--no-row-id-only",
        action="store_true",
        help="Do not print ALTER TABLE statements that only change the row_id column.",
    )
    parser.add_argument(
        "-x",
        "--execute",
        action="store_true",
        help="Execute the generated ALTER TABLE statements. DDL is auto-committed.",
    )
    args = parser.parse_args()

    db = GraphDB()
    engine_name = "xaas_coresrv"

    control = load_control()
    control_normalized = {
        col: normalize_definition(defn) for col, defn in control.items()
    }

    type_mismatches = []
    table_collation_mismatches = []

    for schema_name in SCHEMAS:
        tables = [
            t
            for t in db.get_tables_in_schema(
                engine_name=engine_name, schema_name=schema_name
            )
            if not t.startswith("_")
        ]

        for table_name in tables:

            if os.path.exists('abort'):
                print('Script aborted by request.')
                exit()

            if '_AS' in table_name or '_GBC' in table_name or 'score' in table_name.lower():
                continue

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

            for column_name, col_meta in metadata.items():
                # Only evaluate columns defined in the control JSON.
                if column_name not in control:
                    continue

                actual_definition = build_actual_definition(col_meta)
                actual_normalized = normalize_definition(actual_definition)
                expected_normalized = control_normalized[column_name]

                if actual_normalized != expected_normalized:
                    type_mismatches.append(
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "column": column_name,
                            "actual": actual_definition,
                            "expected": control[column_name],
                        }
                    )

    # --------------------------------------------------
    # Generate remediation SQL.
    # --------------------------------------------------
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

        collation_set = {(m["schema"], m["table"]) for m in table_collation_mismatches}

        statements = []
        for schema, table in sorted(set(type_grouped.keys()) | collation_set):
            modifications = []

            if (schema, table) in collation_set:
                modifications.append(
                    f"    CONVERT TO CHARACTER SET {EXPECTED_CHARSET} COLLATE {EXPECTED_COLLATION}"
                )

            columns = type_grouped[(schema, table)]
            if skip_row_id_only and {m["column"] for m in columns} == {ROW_ID_COLUMN}:
                if not modifications:
                    continue

            for m in columns:
                modifications.append(
                    "    MODIFY COLUMN "
                    + build_modify_clause(m["column"], control[m["column"]])
                )

            if not modifications:
                continue

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

    if type_mismatches or table_collation_mismatches:
        if args.no_row_id_only and type_mismatches:
            print("-- -nr enabled: hiding ALTER TABLE statements that only change row_id.\n")
        if args.execute:
            print("-- EXECUTING generated ALTER TABLE statements. DDL is auto-committed.\n")
        else:
            print("-- Review before running. Definitions are taken verbatim from datatypes.json.")
            print("-- Expected table collation: CHARACTER SET utf8mb4 COLLATE utf8mb4_bin.\n")

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


if __name__ == "__main__":
    main()
