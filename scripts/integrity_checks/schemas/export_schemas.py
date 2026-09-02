# graphregistry/scripts/integrity_checks/schemas/export_schemas.py
"""Export live MySQL schemas to database/init/schemas/*.sql files.

Reads the logical schema names from config/config_global.yaml, queries each
database via GraphDB, and writes one SQL file per schema containing the
CREATE TABLE statements for all non-underscore-prefixed tables.

Output is normalised: backticks are removed and AUTO_INCREMENT is reset to 1.
"""
import argparse
import re
from pathlib import Path
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig

# Public Method: Normalise a raw SHOW CREATE TABLE statement for export.
def normalise_create_statement(statement: str) -> str:
    """Remove backticks, reset AUTO_INCREMENT to 1, and add IF NOT EXISTS."""
    statement = re.sub(r"^CREATE TABLE\b", "CREATE TABLE IF NOT EXISTS", statement)
    statement = statement.replace("`", "")
    statement = re.sub(r"AUTO_INCREMENT=\d+", "AUTO_INCREMENT=1", statement)
    return statement

# Logical GraphDB schema keys mapped to their output file name suffixes.
# The logical key is what GlobalConfig exposes in mysql_schema_names;
# the suffix is used for the output file schema_<suffix>.sql.
SCHEMA_EXPORTS = [
    ("registry", "registry"),
    ("lectures", "lectures"),
    ("airflow", "airflow"),
    ("graph_cache", "graph_cache_test"),
]

# Public Method: Resolve the repository root from this script's location.
def repo_root() -> Path:
    """Return the GraphRegistry repository root directory."""
    return Path(__file__).resolve().parents[3]

# Public Method: Export a single schema to an SQL file.
def export_schema(db: GraphDB, engine_name: str, schema_name: str, output_path: Path) -> int:
    """Write the CREATE TABLE statements for one schema to output_path."""
    tables = [
        t
        for t in db.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name)
        if not t.startswith("_") and not t.startswith("IndexBuildup_")
    ]

    # Collect CREATE TABLE statements for every table in the schema.
    statements = []
    for table_name in sorted(tables):
        query = f"SHOW CREATE TABLE `{schema_name}`.`{table_name}`"
        rows = list(db.execute_query(engine_name=engine_name, query=query))
        if rows:
            statements.append(normalise_create_statement(rows[0][1]) + ";")

    # Ensure the output directory exists and write the schema SQL file.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n\n".join(statements) + "\n", encoding="utf-8")
    return len(statements)

# Public Method: Main entry point for the schema export script.
def main() -> None:
    """Export the configured schemas to SQL files."""
    parser = argparse.ArgumentParser(
        description="Export live MySQL schemas to database/init/schemas/*.sql files."
    )
    parser.add_argument(
        "--engine",
        default = "xaas_coresrv",
        help    = "GraphDB engine name to use for the export.",
    )
    args = parser.parse_args()

    # Load schema names from config_global.yaml via the GlobalConfig loader.
    glb_cfg = GlobalConfig()
    schema_names = glb_cfg.mysql_schema_names[args.engine]

    # Resolve the directory where schema SQL files are written.
    output_dir = repo_root() / "database" / "init" / "schemas"

    # Export each configured logical schema to its own SQL file.
    for logical_key, file_suffix in SCHEMA_EXPORTS:
        schema_name = schema_names[logical_key]
        output_path = output_dir / f"schema_{file_suffix}.sql"
        table_count = export_schema(
            db          = GraphDB(),
            engine_name = args.engine,
            schema_name = schema_name,
            output_path = output_path,
        )
        print(f"Exported {table_count:>3} tables from {schema_name} to {output_path}")

# Run the export when executed as a script.
if __name__ == "__main__":
    main()
