import argparse
import re

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH
from graphregistry.common.dbstruct import DynamicSQL
from graphregistry.adapters.persistence.mysql.repositories.arp_indexdeploy import MySQLIndexDeploy
from graphregistry.domain.repositories.rpo_indexdeploy import IndexTableSpec


# Matches Index_D_<doc>_L_<link>_T_<subtype> with optional _Search / _COPY suffix.
LINK_TABLE_RE = re.compile(
    r"^Index_D_(?P<doc_type>[^_]+)_L_(?P<link_type>[^_]+)_T_(?P<link_subtype>[^_]+?)(?P<suffix>_Search|_COPY)?$"
)


def main():
    parser = argparse.ArgumentParser(
        description="Generate GraphSearch index patch/rollback SQL files."
    )
    parser.add_argument(
        "--table-name",
        "--table_name",
        dest="table_name",
        default=None,
        help="Generate patch files for one specific table only, e.g. Index_D_Lecture",
    )
    parser.add_argument(
        "--default-batch-size",
        "--default_batch_size",
        "--batch-size",
        "--batch_size",
        dest="default_batch_size",
        type=int,
        default=100,
        help="Default number of rows per SQL statement (default: 100). "
             "Overridden per operation by --replace-batch-size, "
             "--delete-batch-size, and --insert-batch-size.",
    )
    parser.add_argument(
        "--replace-batch-size",
        "--replace_batch_size",
        dest="replace_batch_size",
        type=int,
        default=None,
        help="Rows per REPLACE statement (falls back to --batch-size).",
    )
    parser.add_argument(
        "--delete-batch-size",
        "--delete_batch_size",
        dest="delete_batch_size",
        type=int,
        default=None,
        help="Rows per DELETE ... IN statement (falls back to --batch-size).",
    )
    parser.add_argument(
        "--insert-batch-size",
        "--insert_batch_size",
        dest="insert_batch_size",
        type=int,
        default=None,
        help="Rows per INSERT statement (falls back to --batch-size).",
    )
    parser.add_argument(
        "--skip-count",
        "--skip_count",
        dest="skip_count",
        action="store_true",
        help="Skip the slow COUNT(*) pre-checks and stream rows directly. "
             "No patch_max_rows protection when enabled.",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Print every SQL query used to build the patch via print_sql().",
    )
    parser.add_argument(
        "--use-unhex",
        "--use_unhex",
        dest="use_unhex",
        action="store_true",
        help="Encode string/bytes values as UNHEX(...) literals instead of readable quoted strings.",
    )
    args = parser.parse_args()

    # Load configs
    db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    db = GraphDB(config=db_cfg)
    glbcfg = GlobalConfig.from_file()

    source_engine = "xaas_coresrv"
    source_schema = glbcfg.mysql_schema_names[source_engine]["graphsearch"]
    source_cache_schema = glbcfg.mysql_schema_names[source_engine]["graph_cache"]

    # Discover all relevant tables in the source schemas
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

    # Doc tables: Index_D_<DocType>
    doc_types_synced = []
    for doc_type in dynsql.doc_types:
        table_name = f"Index_D_{doc_type}"
        if table_name in existing_graphsearch_tables:
            specs.append(IndexTableSpec(table_type="doc", doc_type=doc_type))
            doc_types_synced.append(doc_type)

    # Link tables: Index_D_<DocType>_L_<LinkType>_T_<SEM|ORG>[_Search|_COPY]
    link_tables_synced = []
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
        link_tables_synced.append(table_name)

    # Page profile table lives in graph_cache
    if "Data_N_Object_T_PageProfile" in existing_cache_tables:
        specs.append(IndexTableSpec(table_type="page_profile"))

    print(f"\nFound {len(doc_types_synced)} doc tables to sync:")
    for doc_type in doc_types_synced:
        print(f"  - {doc_type}")

    print(f"\nFound {len(link_tables_synced)} link tables to sync:")
    for table_name in link_tables_synced:
        print(f"  - {table_name}")

    print(
        f"\nPage profile table will be synced: "
        f"{'yes' if 'Data_N_Object_T_PageProfile' in existing_cache_tables else 'no'}"
    )

    # Instantiate adapter and generate patch files
    deploy = MySQLIndexDeploy(
        db=db,
        glbcfg=glbcfg,
        debug=args.debug,
        use_unhex=args.use_unhex,
    )

    if args.table_name:
        specs = [s for s in specs if deploy._table_name(s) == args.table_name]
        if not specs:
            raise SystemExit(f"No matching table found: {args.table_name}")
        print(f"\nFiltering to single table: {args.table_name}")

    replace_batch_size = args.replace_batch_size or args.default_batch_size
    delete_batch_size = args.delete_batch_size or args.default_batch_size
    insert_batch_size = args.insert_batch_size or args.default_batch_size

    patch_dir = deploy.generate_patch_files(
        source_engine=source_engine,
        target_engine=source_engine,  # prod mirror lives on the same server
        table_specs=specs,
        schema_overrides={
            "source_graphsearch": "graphsearch_test",
            "source_graph_cache": "graph_cache_test",
            "target_graphsearch": "graphsearch_prod_mirror",
            "target_graph_cache": "graph_cache_prod_mirror",
        },
        replace_batch_size=replace_batch_size,
        delete_batch_size=delete_batch_size,
        insert_batch_size=insert_batch_size,
        skip_count=args.skip_count,
    )

    print(f"Patch files written to: {patch_dir}")


if __name__ == "__main__":
    main()
