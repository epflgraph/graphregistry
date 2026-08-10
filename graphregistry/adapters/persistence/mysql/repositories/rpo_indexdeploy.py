# graphregistry/adapters/persistence/mysql/repositories/arp_indexdeploy.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, get_args
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import NodeKey, Node, NodeList
from graphregistry.domain.types import ActionSet
from graphregistry.application.ports.repositories.prt_node import NodeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.adapters.persistence.mysql.mappers.map_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories.schemas import PAGE_PROFILE_COLUMNS
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.types import ObjectType
from graphregistry.domain.models.entities.types import ConceptMapType
import rich
from graphdb.models.sqlquery import SQLQuery, print_sql

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLIndexDeploy(NodeRepository):

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, db: GraphDB, schema_resolver: SchemaResolver) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.msg = GraphLogger()

    #----------------------------------------#
    #
    #----------------------------------------#

    # Method: ...
    def page_profile_insert_ids(self) -> list[tuple[str, str]]:

        # Get required schema names using the schema resolver
        engine_name, schema_graphsearch_test = self.schema_resolver.for_graphsearch_test()
        _, schema_graphsearch_prod_mirror = self.schema_resolver.for_graphsearch_prod_mirror()
        print(engine_name)
        return

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['patching']['fetch']['page_profile_insert_ids'],
            graphsearch_test = schema_graphsearch_test,
            graphsearch_prod_mirror = schema_graphsearch_prod_mirror
        )
        print_sql(sql_query)

        # # Resolve placeholders in template query
        # sql_query = resolve_sql_query(
        #     file_path = sql_queries_paths['patching']['fetch']['page_profile_delete_ids'],
        #     graphsearch_test = schema_graphsearch_test,
        #     graphsearch_prod_mirror = schema_graphsearch_prod_mirror
        # )
        # print_sql(sql_query)


        #     -- Fetch ids of rows to INSERT
        # SELECT t.object_type, t.object_id
        #     FROM        [[graphsearch_test]].Data_N_Object_T_PageProfile t
        # LEFT JOIN [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
        #     USING (object_type, object_id)
        #     WHERE p.object_id IS NULL;


        # Execute SQL query
        node_list = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Return node list
        return cast(list[tuple[str, str]], node_list)

if __name__ == "__main__":
    from graphregistry.common.config import GlobalConfig
    from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
    from graphdb.core.graphdb import GraphDB

    # Initialize global configuration
    glbcfg = GlobalConfig()

    # Initialize schema resolver with engine name and global configuration
    schema_resolver = DefaultSchemaResolver(engine_name="mysql", glbcfg=glbcfg)

    # Initialize database connection (GraphDB)
    db = GraphDB()

    # Create an instance of MySQLIndexDeploy with the database and schema resolver
    index_deploy = MySQLIndexDeploy(db=db, schema_resolver=schema_resolver)

    # Call the page_profile_insert_ids method to fetch IDs for insertion
    insert_ids = index_deploy.page_profile_insert_ids()

    # Print the fetched IDs
    rich.print(insert_ids)
