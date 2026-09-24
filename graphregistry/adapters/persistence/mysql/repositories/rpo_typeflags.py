# graphregistry/adapters/persistence/mysql/repositories/rpo_typeflags.py
from __future__ import annotations
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_typeflags import TypeFlagsRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.domain.models.pipeline.mdl_typeflags import EdgeTypeFlag, NodeTypeFlag, TypeFlagConfig
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

#==================#
# Class Definition #
#==================#
class MySQLTypeFlagsRepository(TypeFlagsRepository):
    """MySQL adapter for the TypeFlagsRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.Orchestration.TypeFlags class so that behavior is preserved
    during the strangler migration. Table references stay unquoted, exactly as
    the legacy queries build them.

    Two legacy behaviors are preserved on purpose: node flag rows are only
    updated (the sync command creates missing rows), while edge flag rows are
    upserted in both directions because undirected families are stored twice.
    """

    # Airflow tables holding the node and edge type flags.
    _NODE_FLAGS_TABLE = "Operations_N_Object_T_TypeFlags"
    _EDGE_FLAGS_TABLE = "Operations_N_Object_N_Object_T_TypeFlags"

    # Public Method: Initialize the repository with an injected database client
    # and schema resolver; no module-level connections are created.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.verbose = verbose

    #================================================================#
    # Method Group: Read methods                                     #
    #================================================================#

    # Public Method: Load the full activation state of the type flags, mapping
    # the active-only rows of the airflow tables onto the domain model.
    def load(self) -> TypeFlagConfig:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Fetch the per-type fields and scores activation from the node flags
        # table; the WHERE clause keeps only types with at least one active
        # family, mirroring the legacy get_config_json query.
        node_query = f"""
             SELECT t1.object_type, t1.to_process AS process_fields, t2.to_process AS process_scores
               FROM {schema_name}.{self._NODE_FLAGS_TABLE} t1
         INNER JOIN {schema_name}.{self._NODE_FLAGS_TABLE} t2
              USING (object_type)
              WHERE t1.flag_type = 'fields'
                AND t2.flag_type = 'scores'
                AND (t1.to_process = 1 OR t2.to_process = 1)
        """
        node_rows = self.db.execute_query(engine_name=engine_name, query=node_query, query_id='4bcoW1KT')

        # Emit one flag per family and type, keeping the inactive families of
        # partially activated types so the model reflects the table contents.
        node_flags = [
            NodeTypeFlag(object_type=object_type, flag_type=flag_type, to_process=bool(active))
            for object_type, process_fields, process_scores in node_rows
            for flag_type, active in (("fields", process_fields), ("scores", process_scores))
        ]

        # Fetch the active edge families, collapsed to their canonical order by
        # the LEAST/GREATEST selection of the legacy query.
        edge_query = f"""
            SELECT DISTINCT LEAST(from_object_type, to_object_type)    AS from_object_type,
                            GREATEST(from_object_type, to_object_type) AS to_object_type
               FROM {schema_name}.{self._EDGE_FLAGS_TABLE}
              WHERE to_process = 1
        """
        edge_rows = self.db.execute_query(engine_name=engine_name, query=edge_query, query_id='9K34TTeQ')

        # Map each canonical (from, to) row onto an activated edge flag; the
        # model canonicalises on construction, a no-op for the sorted rows.
        edge_flags = [
            EdgeTypeFlag(from_object_type=row[0], to_object_type=row[1], to_process=True)
            for row in edge_rows
        ]
        return TypeFlagConfig(nodes=node_flags, edges=edge_flags)

    #================================================================#
    # Method Group: Write methods                                    #
    #================================================================#

    # Public Method: Save the full activation state, replacing the previous
    # configuration: reset every flag, then activate the flags of the model.
    def save(self, config: TypeFlagConfig, actions: ActionSet = ('commit',)) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        do_commit = 'commit' in actions

        # The legacy config command always resets before activating, which
        # makes save a full replacement rather than a merge.
        if do_commit:
            self.reset(actions=actions)
        else:
            n_nodes = sum(1 for flag in config.nodes if flag.to_process)
            n_edges = sum(1 for flag in config.edges if flag.to_process)
            sysmsg.info(f"TypeFlags save [eval]: would reset all flags, activate {n_nodes} node flags and {n_edges} edge families.")

        # Activate the node flags whose family is switched on; inactive model
        # flags are skipped, exactly as the legacy config loop does.
        for flag in config.nodes:
            if not flag.to_process:
                continue
            if do_commit:
                self.db.set_cells(
                    engine_name = engine_name,
                    schema_name = schema_name,
                    table_name  = self._NODE_FLAGS_TABLE,
                    set         = [('to_process', 1)],
                    where       = [
                        ('object_type', flag.object_type),
                        ('flag_type',   flag.flag_type),
                    ],
                    verbose = self.verbose,
                )

        # Activate the edge families. Both directions are upserted because
        # undirected families are stored as two rows and set_cells cannot
        # insert missing rows.
        for flag in config.edges:
            if not flag.to_process:
                continue
            edge_query = f"""
                INSERT INTO {schema_name}.{self._EDGE_FLAGS_TABLE}
                            (from_object_type, to_object_type, to_process)
                     VALUES ('{flag.from_object_type}', '{flag.to_object_type}', 1),
                            ('{flag.to_object_type}', '{flag.from_object_type}', 1)
                ON DUPLICATE KEY UPDATE to_process = 1;
            """
            if 'print' in actions:
                print_sql(edge_query, title='typeflags-edge-upsert')
            if do_commit:
                self.db.execute_query_in_shell(
                    engine_name = engine_name,
                    query       = edge_query,
                    verbose     = self.verbose,
                    query_id    = 'typeflags-edge-upsert',
                )

    # Public Method: Deactivate every node and edge type flag, using the
    # legacy reset queries and query id.
    def reset(self, actions: ActionSet = ('commit',)) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Both flag tables are reset with the same statement shape, keeping
        # the legacy query id for tracking.
        for table_name in (self._NODE_FLAGS_TABLE, self._EDGE_FLAGS_TABLE):
            reset_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET to_process = 0
                 WHERE to_process = 1
            """
            if 'print' in actions:
                print_sql(reset_query, title='AUzikHX5')
            if 'commit' in actions:
                self.db.execute_query_in_shell(
                    engine_name = engine_name,
                    query       = reset_query,
                    verbose     = self.verbose,
                    query_id    = 'AUzikHX5',
                )
