# apply_data_patch.py
"""Apply generated index patch files to the target engine."""

import argparse
import re
from pathlib import Path

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH
from graphregistry.common.dbstruct import DynamicSQL
from graphregistry.adapters.persistence.mysql.repositories.arp_indexdeploy import (
    MySQLIndexDeploy,
)
from graphregistry.domain.repositories.rpo_indexdeploy import IndexTableSpec


LINK_TABLE_RE = re.compile(
    r"^Index_D_(?P<doc_type>[^_]+)_L_(?P<link_type>[^_]+)_T_(?P<link_subtype>[^_]+?)(?P<suffix>_Search|_COPY)?$"
)


def discover_specs(db: GraphDB, glbcfg: GlobalConfig) -> list[IndexTableSpec]:
    """Discover doc/link/page-profile tables exactly like create_data_patch.py."""
    source_engine = "xaas_coresrv"
    source_schema = glbcfg.mysql_schema_names[source_engine]["graphsearch"]
    source_cache_schema = glbcfg.mysql_schema_names[source_engine]["graph_cache"]

    existing_graphsearch_tables = {
        t
        for t in db.get_tables_in_schema(
            engine_name=source_engine,
            schema_name=source_schema,
            use_regex=[r"^Index_D_.+$", r"^Data_N_Object_T_PageProfile$"],
        )
        if not t.startswith("_")
    }
    existing_cache_tables = set(
        db.get_tables_in_schema(
            engine_name=source_engine,
            schema_name=source_cache_schema,
            use_regex=[r"^Data_N_Object_T_PageProfile$"],
        )
    )

    dynsql = DynamicSQL(db=db)
    specs: list[IndexTableSpec] = []

    for doc_type in dynsql.doc_types:
        table_name = f"Index_D_{doc_type}"
        if table_name in existing_graphsearch_tables:
            specs.append(IndexTableSpec(table_type="doc", doc_type=doc_type))

    for table_name in sorted(existing_graphsearch_tables):
        match = LINK_TABLE_RE.match(table_name)
        if not match:
            continue
        specs.append(
            IndexTableSpec(
                table_type="doclink",
                doc_type=match.group("doc_type"),
                link_type=match.group("link_type"),
                link_subtype=match.group("link_subtype"),
                special_suffix=match.group("suffix") or "",
            )
        )

    if "Data_N_Object_T_PageProfile" in existing_cache_tables:
        specs.append(IndexTableSpec(table_type="page_profile"))

    return specs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply generated index patch files to the target engine/schema."
    )
    parser.add_argument(
        "patch_dir",
        type=Path,
        help="Path to the patch directory, e.g. data/index_patches/2026-07-31_15-16/patch",
    )
    parser.add_argument(
        "--schema_name",
        required=True,
        help="Target schema / database name, e.g. graphsearch_prod_mirror",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the first 256 characters of each patch file instead of executing it.",
    )
    args = parser.parse_args()

    if not args.patch_dir.is_dir():
        raise SystemExit(f"Patch directory does not exist: {args.patch_dir}")

    db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    db = GraphDB(config=db_cfg)
    glbcfg = GlobalConfig.from_file()

    specs = discover_specs(db, glbcfg)

    deploy = MySQLIndexDeploy(db=db, glbcfg=glbcfg)
    deploy.apply_patch_files(
        patch_dir=args.patch_dir,
        target_engine="xaas_coresrv",
        target_schema=args.schema_name,
        table_specs=specs,
        dry_run=args.dry_run,
    )

    print(f"\nPatch applied from: {args.patch_dir} to schema {args.schema_name}")


if __name__ == "__main__":
    main()
