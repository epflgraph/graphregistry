# graphregistry/adapters/persistence/mysql/repositories/rpo_pageprofile.py
from __future__ import annotations
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_pageprofile import PageProfileRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

#==================#
# Class Definition #
#==================#
class MySQLPageProfileRepository(PageProfileRepository):
    """MySQL adapter for the PageProfileRepository port.

    The SQL statement and query id are extracted verbatim from the legacy
    GraphRegistry.IndexDB.PageProfile class (query id Mp7U7rMW). The patched
    column list is discovered from the live graph_cache table, as the legacy
    constructor discovers it; the graphsearch target table is created from
    the graph_cache template when missing, since the graphsearch schema has
    no static CREATE TABLE for the page profile.
    """

    # The page-profile table carries the same name in both schemas.
    _TABLE_NAME = "Data_N_Object_T_PageProfile"
    _KEY_COLUMN_NAMES = ['object_type', 'object_id']

    # Public Method: Initialize the repository with an injected database
    # client and schema resolver; no constructor-time schema introspection
    # or DDL, unlike the legacy PageProfile class.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.verbose = verbose

    # Public Method: Patch the page-profile projection from the flagged
    # graph_cache rows into the graphsearch schema.
    def patch(self, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        target_table_path = f"{search_schema}.{self._TABLE_NAME}"

        # Without actions there is nothing to do, as the legacy command warns.
        if len(actions) == 0:
            sysmsg.warning("No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
            sysmsg.info("🚜 📝 Nothing to do.")
            return PropagationStats(target=target_table_path, rows_flagged=0)
        if 'eval' in actions and 'commit' not in actions:
            sysmsg.warning("Executing in evaluation mode only.")

        # Discover the patched columns from the live graph_cache table,
        # excluding the keys and the control columns; the graphsearch target
        # uses record_deleted rather than deleted.
        columns = self.db.get_column_names(engine_name=engine_name, schema_name=cache_schema, table_name=self._TABLE_NAME)
        upd_column_names = [c for c in columns if c not in self._KEY_COLUMN_NAMES + ['row_id', 'to_process', 'deleted']]

        # Ensure the target table exists; the graphsearch schema has no
        # static CREATE TABLE for the page profile, so the target is created
        # from the graph_cache template when missing.
        self._ensure_target_table(engine_name, cache_schema, search_schema)

        # Copy the flagged rows whose types are fields-active, driven by the
        # fields-changed flags and the typeflags.
        sql_query = f"""
             SELECT {', '.join([f'p.{k}' for k in self._KEY_COLUMN_NAMES])}{', ' if len(upd_column_names) > 0 else ''}{', '.join(upd_column_names)}
               FROM {cache_schema}.{self._TABLE_NAME} p
         INNER JOIN {airflow_schema}.Operations_N_Object_T_FieldsChanged fc
                 USING (object_type, object_id)
         INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags tf
                 USING (object_type)
                 WHERE tf.flag_type  = 'fields'
                   AND  p.to_process = 1
                   AND fc.to_process = 1
                   AND tf.to_process = 1
                   AND p.deleted = 0
        """
        if 'print' in actions:
            print_sql(sql_query, title='Mp7U7rMW')
        if 'commit' in actions:
            sysmsg.trace("⚙️  Processing page profile ...")

        # Upsert the flagged rows; the safe-insert helper handles the eval
        # and print actions, so the stats carry no separate count.
        self.db.execute_query_as_safe_inserts(
            engine_name       = engine_name,
            schema_name       = search_schema,
            table_name        = self._TABLE_NAME,
            query             = sql_query,
            key_column_names  = self._KEY_COLUMN_NAMES,
            upd_column_names  = upd_column_names,
            eval_column_names = ['object_type'],
            actions           = actions,
            verbose           = self.verbose,
            query_id          = 'Mp7U7rMW',
        )
        sysmsg.success("🚜 ✅ Done patching page profile table.")
        return PropagationStats(target=target_table_path, rows_flagged=0)

    # Internal Method: Ensure the graphsearch page-profile table exists,
    # creating it from the graph_cache template when missing, as the legacy
    # constructor does.
    def _ensure_target_table(self, engine_name: str, cache_schema: str, search_schema: str) -> None:

        # Ensure the target database exists first.
        if not self.db.database_exists(engine_name=engine_name, schema_name=search_schema):
            sysmsg.warning(f"Target database '{search_schema}' does not exist. Creating database ...")
            self.db.create_database(engine_name=engine_name, schema_name=search_schema)
            if not self.db.database_exists(engine_name=engine_name, schema_name=search_schema):
                sysmsg.critical(f"❌ Failed to create database '{search_schema}'.")
                raise RuntimeError(f"Failed to create database '{search_schema}'.")
            sysmsg.trace("☑️ Database created successfully.")

        # Create the target from the cache template when missing.
        if not self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=self._TABLE_NAME):
            sysmsg.warning(
                f"Target table '{search_schema}.{self._TABLE_NAME}' does not exist. "
                f"Creating table from graph_cache template ..."
            )
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = f"CREATE TABLE IF NOT EXISTS {search_schema}.{self._TABLE_NAME} "
                              f"LIKE {cache_schema}.{self._TABLE_NAME}",
                verbose     = False,
                query_id    = 'pageprofile-create-target',
            )
            if not self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=self._TABLE_NAME):
                sysmsg.critical(f"❌ Failed to create table '{search_schema}.{self._TABLE_NAME}'.")
                raise RuntimeError(f"Failed to create table '{search_schema}.{self._TABLE_NAME}'.")
            sysmsg.trace("☑️ Target PageProfile table created successfully.")
