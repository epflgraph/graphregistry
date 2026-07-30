from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH
from graphregistry.common.dbstruct import DynamicSQL
from graphregistry.adapters.persistence.mysql.repositories.arp_indexdeploy import MySQLIndexDeploy
from graphregistry.domain.repositories.rpo_indexdeploy import IndexTableSpec


def main():
    # Load configs
    db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    db = GraphDB(config=db_cfg)
    glbcfg = GlobalConfig.from_file()

    # Discover which doc index tables actually exist in the source schema
    source_engine = "xaas_coresrv"
    source_schema = glbcfg.mysql_schema_names[source_engine]["graphsearch"]
    existing_tables = {
        t
        for t in db.get_tables_in_schema(
            engine_name=source_engine,
            schema_name=source_schema,
            use_regex=[r"^Index_D_[^_]+$"],
        )
        if not t.startswith("_")
    }

    dynsql = DynamicSQL(db=db)
    specs = []
    for doc_type in dynsql.doc_types:
        table_name = f"Index_D_{doc_type}"
        if table_name in existing_tables:
            specs.append(IndexTableSpec(table_type="doc", doc_type=doc_type))

    print(f"Found {len(specs)} doc tables to sync: {[s.doc_type for s in specs]}")

    # Instantiate adapter and generate patch files
    deploy = MySQLIndexDeploy(db=db, glbcfg=glbcfg)
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
    )

    print(f"Patch files written to: {patch_dir}")


if __name__ == "__main__":
    main()
