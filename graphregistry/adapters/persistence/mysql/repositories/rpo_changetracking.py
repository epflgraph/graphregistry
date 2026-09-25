# graphregistry/adapters/persistence/mysql/repositories/rpo_changetracking.py
from __future__ import annotations
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Iterable
from loguru import logger as sysmsg
from tabulate import tabulate
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_changetracking import ChangeTrackingRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.auxfcn import print_colour
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.pipeline.mdl_changetracking import (
    EdgeChangeRecord,
    ObjectChangeRecord,
    ObjectScoreExpiryRecord,
)
from graphregistry.domain.models.pipeline.mdl_policies import ExpirationPolicy, ProcessingScope
from graphregistry.domain.models.pipeline.mdl_stats import EdgeRefreshStats, NodeRefreshStats, PropagationStats, TrackingStatus
from graphregistry.domain.models.values.mdl_cachestate import CacheState
from graphregistry.domain.models.values.mdl_checksums import Checksum, ChecksumPair
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Languages of the multilingual page-profile columns, in checksum order.
_PAGE_PROFILE_LANGUAGES = ("en", "fr", "de", "it")

# Internal Function: Render one refresh evaluation table in the legacy
# print_dataframe format, skipped when there are no rows.
def _print_refresh_table(rows: list[list], headers: list[str], title: str) -> None:
    if not rows:
        return
    print('')
    print_colour(title, colour='white', background='black', style='bold')
    print(tabulate(rows, headers=headers, tablefmt='fancy_grid', showindex=False))
    print('')

# Multilingual page-profile field groups whose per-language value decomposes
# into generation flags, correction flags, translation origin, and value.
_PAGE_PROFILE_LANGUAGE_FIELDS = ("name", "description_short", "description_medium", "description_long")

# Full column list of the page-profile checksum, in the exact legacy order:
# numeric ids, short code, subtypes, the four decomposed language fields,
# external keys and urls, and the visibility flag.
PAGE_PROFILE_CHECKSUM_COLUMNS: tuple[str, ...] = (
    tuple(f"numeric_id_{lang}" for lang in _PAGE_PROFILE_LANGUAGES)
    + ("short_code",)
    + tuple(f"subtype_{lang}" for lang in _PAGE_PROFILE_LANGUAGES)
    + tuple(
        f"{field}_{lang}_{part}"
        for field in _PAGE_PROFILE_LANGUAGE_FIELDS
        for lang in _PAGE_PROFILE_LANGUAGES
        for part in ("is_auto_generated", "is_auto_corrected", "is_auto_translated", "translated_from", "value")
    )
    + tuple(f"external_key_{lang}" for lang in _PAGE_PROFILE_LANGUAGES)
    + tuple(f"external_url_{lang}" for lang in _PAGE_PROFILE_LANGUAGES)
    + ("is_visible",)
)

# Sentinel used by the SQL checksum queries in place of NULL columns.
_NULL_SENTINEL = "__null__"

#==================#
# Class Definition #
#==================#
class MySQLChangeTrackingRepository(ChangeTrackingRepository):
    """MySQL adapter for the ChangeTrackingRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.Orchestration FieldsChanged and ScoresExpired classes and
    their Orchestration wrappers, so behavior is preserved during the
    strangler migration. The global configuration is injected because the
    data-schema enumeration and the schema-to-object-type mapping it carries
    are still config-driven; moving them behind the resolver port is a later
    cleanup.
    """

    # Airflow tables of the two tracking families.
    _NODE_TABLE     = "Operations_N_Object_T_FieldsChanged"
    _EDGE_TABLE     = "Operations_N_Object_N_Object_T_FieldsChanged"
    _SCORES_TABLE   = "Operations_N_Object_T_ScoresExpired"

    # Public Method: Initialize the repository with an injected database client,
    # schema resolver, and global configuration; no module-level connections.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, global_config: GlobalConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.global_config = global_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Scope condition builders                         #
    #================================================================#

    # Internal Method: Render a list of object types as a SQL IN-list, using the
    # repr formatting of the legacy where-condition generators.
    @staticmethod
    def _sql_type_list(types: Iterable[str]) -> str:
        return ", ".join(repr(t) for t in types)

    # Internal Method: Build the node-table condition for the fields family from
    # the scope; an empty type list yields FALSE so the table is skipped safely.
    def _node_fields_condition(self, scope: ProcessingScope) -> str:
        if len(scope.node_fields_types) == 0:
            return "FALSE"
        return f"""object_type IN ({self._sql_type_list(scope.node_fields_types)}) AND deleted = 0"""

    # Internal Method: Build the node-table condition for the scores family.
    def _node_scores_condition(self, scope: ProcessingScope) -> str:
        if len(scope.node_scores_types) == 0:
            return "FALSE"
        return f"""object_type IN ({self._sql_type_list(scope.node_scores_types)}) AND deleted = 0"""

    # Internal Method: Build the edge-table condition from the scope's canonical
    # pairs, matching both stored directions of the undirected families.
    def _edge_fields_condition(self, scope: ProcessingScope) -> str:
        if len(scope.edge_type_pairs) == 0:
            return "FALSE"
        pairs = ", ".join(repr(pair.canonical_order.as_tuple) for pair in scope.edge_type_pairs)
        return f"""( (from_object_type, to_object_type) IN ({pairs}) OR (to_object_type, from_object_type) IN ({pairs}) ) AND deleted = 0"""

    # Internal Method: Restrict a condition to a subset of object types, as the
    # legacy expire-condition generator does for its optional type filter.
    def _restrict_to_object_types(self, condition: str, object_types: list[str] | None, edges: bool = False) -> str:
        if object_types is None:
            return condition
        types_list = self._sql_type_list(object_types)
        if edges:
            return f"""({condition} AND (from_object_type IN ({types_list}) OR to_object_type IN ({types_list})))"""
        return f"""({condition} AND object_type IN ({types_list}))"""

    # Internal Method: Run a write query under the action semantics: print the
    # statement when requested and execute it only in commit mode.
    def _execute_write(self, engine_name: str, query: str, query_id: str, actions: ActionSet) -> None:
        if 'print' in actions:
            print_sql(query, title=query_id)
        if 'commit' in actions:
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = query,
                verbose     = self.verbose or 'print' in actions,
                query_id    = query_id,
            )

    #================================================================#
    # Method Group: Sync                                             #
    #================================================================#

    # Public Method: Insert tracking rows for registry objects and edges that
    # lack them, mirroring the legacy sync command over both families.
    def sync_new_records(self, include_lectures: bool = False, include_ontology: bool = False, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Build the registry data schemas to sync, in the legacy order:
        # lectures first when included, registry always, ontology last.
        schemas_to_sync = []
        if include_lectures:
            schemas_to_sync.append(self.global_config.schema_lectures)
        schemas_to_sync.append(self.global_config.schema_registry)
        if include_ontology:
            schemas_to_sync.append(self.global_config.schema_ontology)

        # Sync both tracking families per schema and collect per-target counts.
        stats = []
        sysmsg.info("♻️  📝 Synching new objects added to the registry with 'FieldsChanged' airflow tables.")
        for data_schema in schemas_to_sync:
            stats.extend(self._sync_fields_changed(engine_name, schema_name, data_schema, actions))
        sysmsg.success("♻️  ✅ Done synching new objects between registry and 'FieldsChanged' airflow tables.")
        sysmsg.info("♻️  📝 Synching new objects added to the registry with 'ScoresExpired' airflow tables.")
        for data_schema in schemas_to_sync:
            stats.extend(self._sync_scores_expired(engine_name, schema_name, data_schema, actions))
        sysmsg.success("♻️  ✅ Done synching new objects between registry and 'ScoresExpired' airflow tables.")
        return stats

    # Internal Method: Sync the FieldsChanged tables for one data schema.
    def _sync_fields_changed(self, engine_name: str, airflow_schema: str, data_schema: str, actions: ActionSet) -> list[PropagationStats]:
        stats = []
        sysmsg.trace(f"⚙️  Processing nodes on schema '{data_schema}' ...")

        # Count new object nodes to sync; the counts feed the returned stats.
        count_query = f"""
                  SELECT cp.object_type, COUNT(*) AS n
                    FROM {data_schema}.Nodes_N_Object cp
               LEFT JOIN {airflow_schema}.{self._NODE_TABLE} fc
                    USING (object_type, object_id)
                   WHERE fc.object_id IS NULL
                     AND (fc.deleted = 0 OR fc.object_id IS NULL)
                     AND cp.object_type NOT IN ('Slide', 'Transcript')
                     AND cp.record_deleted = 0
                GROUP BY cp.object_type
        """
        counts = self.db.execute_query(engine_name=engine_name, query=count_query, query_id='DY3x5PC8')
        sysmsg.trace(f"Done. New objects synched: {counts if counts else []}")
        stats.append(PropagationStats(
            target       = f"{data_schema}.Nodes_N_Object -> {self._NODE_TABLE}",
            rows_flagged = sum(row[1] for row in counts) if counts else 0,
        ))

        # Insert the missing node tracking rows with the legacy defaults.
        insert_query = f"""
             INSERT INTO {airflow_schema}.{self._NODE_TABLE}
                        (object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process, deleted)
                  SELECT cp.object_type, cp.object_id, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, 1 AS to_process, 0 AS deleted
                    FROM {data_schema}.Nodes_N_Object cp
               LEFT JOIN {airflow_schema}.{self._NODE_TABLE} fc
                    USING (object_type, object_id)
                   WHERE fc.object_id IS NULL
                     AND (fc.deleted = 0 OR fc.object_id IS NULL)
                     AND cp.object_type NOT IN ('Slide', 'Transcript')
                     AND cp.record_deleted = 0
             ON DUPLICATE KEY UPDATE to_process = VALUES(to_process),
                                     deleted    = VALUES(deleted);
        """
        self._execute_write(engine_name, insert_query, '2PbejfUm', actions)

        # Backfill node typeflag rows so later activations can update them.
        sysmsg.trace(f"⚙️  Updating type flags for new objects on schema '{data_schema}' ...")
        typeflags_query = f"""
                    INSERT INTO {airflow_schema}.Operations_N_Object_T_TypeFlags
                               (object_type, flag_type, to_process)
                    SELECT DISTINCT object_type, 'fields' AS flag_type, 0 AS to_process
                               FROM {airflow_schema}.{self._NODE_TABLE}
                              WHERE deleted = 0
        ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_T_TypeFlags.to_process;
        """
        self._execute_write(engine_name, typeflags_query, 'x5BdjGfN', actions)

        # Count new object-to-object edges to sync.
        sysmsg.trace(f"⚙️  Processing edges on schema '{data_schema}' ...")
        edge_count_query = f"""
                  SELECT cp.from_object_type, cp.to_object_type, COUNT(*) AS n
                    FROM {data_schema}.Edges_N_Object_N_Object_T_ChildToParent cp
               LEFT JOIN {airflow_schema}.{self._EDGE_TABLE} fc
                    USING (from_object_type, from_object_id, to_object_type, to_object_id)
                   WHERE fc.from_object_id IS NULL
                     AND (fc.deleted = 0 OR fc.from_object_id IS NULL)
                     AND cp.from_object_type NOT IN ('Slide', 'Transcript')
                     AND cp.to_object_type   NOT IN ('Slide', 'Transcript')
                     AND NOT (cp.from_object_type = 'Concept' AND cp.to_object_type = 'Concept')
                     AND cp.record_deleted = 0
                GROUP BY cp.from_object_type, cp.to_object_type
        """
        edge_counts = self.db.execute_query(engine_name=engine_name, query=edge_count_query, query_id='Gk7dDRC0')
        sysmsg.trace(f"Done. New object tuples synched: {edge_counts if edge_counts else []}")
        stats.append(PropagationStats(
            target       = f"{data_schema}.Edges_N_Object_N_Object_T_ChildToParent -> {self._EDGE_TABLE}",
            rows_flagged = sum(row[2] for row in edge_counts) if edge_counts else 0,
        ))

        # Insert the missing edge tracking rows, excluding Concept-to-Concept.
        edge_insert_query = f"""
             INSERT INTO {airflow_schema}.{self._EDGE_TABLE}
                        (from_object_type, from_object_id, to_object_type, to_object_id, context, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process, deleted)
                  SELECT cp.from_object_type, cp.from_object_id, cp.to_object_type, cp.to_object_id, cp.context, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, 1 AS to_process, 0 AS deleted
                    FROM {data_schema}.Edges_N_Object_N_Object_T_ChildToParent cp
               LEFT JOIN {airflow_schema}.{self._EDGE_TABLE} fc
                    USING (from_object_type, from_object_id, to_object_type, to_object_id)
                   WHERE fc.from_object_id IS NULL
                     AND (fc.deleted = 0 OR fc.from_object_id IS NULL)
                     AND cp.from_object_type NOT IN ('Slide', 'Transcript')
                     AND cp.to_object_type   NOT IN ('Slide', 'Transcript')
                     AND NOT (cp.from_object_type = 'Concept' AND cp.to_object_type = 'Concept')
                     AND cp.record_deleted = 0
             ON DUPLICATE KEY UPDATE to_process = VALUES(to_process),
                                     deleted    = VALUES(deleted);
        """
        self._execute_write(engine_name, edge_insert_query, 's1gXyPYb', actions)

        # Backfill edge typeflag rows for the new edge families.
        sysmsg.trace(f"⚙️  Updating type flags for new edges on schema '{data_schema}' ...")
        edge_typeflags_query = f"""
                    INSERT INTO {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags
                               (from_object_type, to_object_type, to_process)
                    SELECT DISTINCT from_object_type, to_object_type, 0 AS to_process
                               FROM {airflow_schema}.{self._EDGE_TABLE}
                              WHERE deleted = 0
        ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_N_Object_T_TypeFlags.to_process;
        """
        self._execute_write(engine_name, edge_typeflags_query, 'dEE3eDPD', actions)
        return stats

    # Internal Method: Sync the ScoresExpired table for one data schema.
    def _sync_scores_expired(self, engine_name: str, airflow_schema: str, data_schema: str, actions: ActionSet) -> list[PropagationStats]:
        sysmsg.trace(f"⚙️  Processing nodes on schema '{data_schema}' ...")

        # Count new object nodes to sync for score expiry tracking.
        count_query = f"""
                  SELECT n.object_type, COUNT(*) AS n
                    FROM {data_schema}.Nodes_N_Object n
               LEFT JOIN {airflow_schema}.{self._SCORES_TABLE} o
                    USING (object_type, object_id)
                   WHERE o.object_id IS NULL
                     AND (o.deleted = 0 OR o.object_id IS NULL)
                     AND n.object_type != 'Transcript'
                     AND n.object_type != 'Slide'
                     AND n.record_deleted = 0
                GROUP BY n.object_type
        """
        counts = self.db.execute_query(engine_name=engine_name, query=count_query, query_id='7RNfE1fF')

        # Insert the missing score-expiry tracking rows.
        insert_query = f"""
             INSERT INTO {airflow_schema}.{self._SCORES_TABLE}
                        (object_type, object_id, last_date_cached, has_expired, to_process, deleted)
                  SELECT n.object_type, n.object_id, NULL AS last_date_cached, NULL AS has_expired, 1 AS to_process, 0 AS deleted
                    FROM {data_schema}.Nodes_N_Object n
               LEFT JOIN {airflow_schema}.{self._SCORES_TABLE} o
                    USING (object_type, object_id)
                   WHERE o.object_id IS NULL
                     AND (o.deleted = 0 OR o.object_id IS NULL)
                     AND n.object_type NOT IN ('Slide', 'Transcript')
                     AND n.record_deleted = 0
             ON DUPLICATE KEY UPDATE to_process = VALUES(to_process),
                                     deleted    = VALUES(deleted);
        """
        self._execute_write(engine_name, insert_query, '5mhz4Uwr', actions)

        # Backfill the scores typeflag rows for the new object types.
        typeflags_query = f"""
                    INSERT INTO {airflow_schema}.Operations_N_Object_T_TypeFlags
                               (object_type, flag_type, to_process)
                    SELECT DISTINCT object_type, 'scores' AS flag_type, 0 AS to_process
                               FROM {airflow_schema}.{self._SCORES_TABLE}
                              WHERE deleted = 0
        ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_T_TypeFlags.to_process;
        """
        self._execute_write(engine_name, typeflags_query, 'n2TKRWNV', actions)
        return [PropagationStats(
            target       = f"{data_schema}.Nodes_N_Object -> {self._SCORES_TABLE}",
            rows_flagged = sum(row[1] for row in counts) if counts else 0,
        )]

    #================================================================#
    # Method Group: Checksum update                                  #
    #================================================================#

    # Public Method: Recompute the current checksums of tracked objects and
    # edges from the registry sources, mirroring the legacy
    # update_checksums_v2 command.
    def update_current_checksums(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        sysmsg.info("🧩 📝 Update object checksums based on typeflag activation.")

        # Serialize the edge families into their endpoint types for the
        # schema-skip checks, as the legacy command does.
        obj_types = scope.node_fields_types
        obj2obj_types = {t for pair in scope.edge_type_pairs for t in pair.as_tuple}

        #---------------#
        # Node checksums #
        #---------------#

        # Loop over the registry and lectures schemas for general object rows.
        for schema_name in (self.global_config.schema_registry, self.global_config.schema_lectures):

            # Skip schemas whose object types are all outside the scope.
            if len(set(obj_types) & set(self.global_config.schema_to_object_types[schema_name])) == 0:
                sysmsg.trace(f"➡️ Skipping calculation: Object > General registry > {schema_name}")
                continue
            sysmsg.trace(f"⚙️ Processing checksums: Object > General registry > {schema_name} ...")
            sql_query = f"""
                          SELECT object_type, object_id,
                                 MD5(CONCAT(MD5(COALESCE(object_type, "__null__")), MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(object_title, "__null__")), MD5(COALESCE(text_source, "__null__")), MD5(COALESCE(raw_text, "__null__")))) AS checksum_val
                              FROM {schema_name}.Nodes_N_Object o
                        INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                             USING (object_type)
                              WHERE object_type NOT IN ('Slide', 'Transcript')
                                AND o.record_deleted = 0
                                AND t.flag_type = 'fields'
                                AND t.to_process = 1
              """
            self._upsert_checksums(engine_name, cache_schema, sql_query, 'Operations_N_Object_T_ChecksumsObject',
                                   ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, 'VBk3hp3Z')

        # Ontology tables follow dedicated checksum queries for Concept and
        # Category rows, whose columns differ from the general object table.
        if len(set(obj_types) & set(self.global_config.schema_to_object_types[self.global_config.schema_ontology])) == 0:
            sysmsg.trace(f"➡️ Skipping calculation: Object > General registry > {self.global_config.schema_ontology}")
        else:
            sysmsg.trace(f"⚙️ Processing checksums: Object > General registry > {self.global_config.schema_ontology} ...")
            concept_query = f"""
                          SELECT object_type, object_id,
                                 MD5(CONCAT(MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(name, "__null__")), MD5(COALESCE(is_ontology_category, "__null__")), MD5(COALESCE(is_ontology_concept, "__null__")), MD5(COALESCE(is_ontology_neighbour, "__null__")), MD5(COALESCE(is_noise, "__null__")), MD5(COALESCE(is_unused, "__null__")))) AS checksum_val
                            FROM {self.global_config.schema_ontology}.Nodes_N_Concept o
                      INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                            WHERE t.flag_type = 'fields'
                              AND t.to_process = 1
              """
            self._upsert_checksums(engine_name, cache_schema, concept_query, 'Operations_N_Object_T_ChecksumsObject',
                                   ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, 'CmNTYc97')
            category_query = f"""
                          SELECT object_type, object_id,
                                 MD5(CONCAT(MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(name, "__null__")), MD5(COALESCE(depth, "__null__")), MD5(COALESCE(reference_page_id, "__null__")), MD5(COALESCE(reference_page_key, "__null__")), MD5(COALESCE(reference_page_url, "__null__")))) AS checksum_val
                            FROM {self.global_config.schema_ontology}.Nodes_N_Category o
                      INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                            WHERE t.flag_type = 'fields'
                              AND t.to_process = 1
              """
            self._upsert_checksums(engine_name, cache_schema, category_query, 'Operations_N_Object_T_ChecksumsObject',
                                   ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, 'XUiwHdd6')

        # Page profile checksums cover registry, lectures, and ontology schemas.
        for schema_name in (self.global_config.schema_registry, self.global_config.schema_lectures, self.global_config.schema_ontology):

            # Skip schemas whose object types are all outside the scope.
            if len(set(obj_types) & set(self.global_config.schema_to_object_types[schema_name])) == 0:
                sysmsg.trace(f"➡️ Skipping calculation: Object > Page profile > {schema_name}")
                continue
            sysmsg.trace(f"⚙️ Processing checksums: Object > Page profile > {schema_name} ...")
            sql_query = f"""
                          SELECT object_type, object_id,
                                 {self._page_profile_checksum_expression()} AS checksum_val
                              FROM {schema_name}.Data_N_Object_T_PageProfile p
                        INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                             USING (object_type)
                              WHERE object_type NOT IN ('Slide', 'Transcript')
                                AND p.record_deleted = 0
                                AND t.flag_type = 'fields'
                                AND t.to_process = 1
              """
            self._upsert_checksums(engine_name, cache_schema, sql_query, 'Operations_N_Object_T_ChecksumsPageProfile',
                                   ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, '5nVWX6nk')

        # Custom fields checksums aggregate the per-field digests per object.
        for schema_name in (self.global_config.schema_registry, self.global_config.schema_lectures, self.global_config.schema_ontology):

            # Skip schemas whose object types are all outside the scope.
            if len(set(obj_types) & set(self.global_config.schema_to_object_types[schema_name])) == 0:
                sysmsg.trace(f"➡️ Skipping calculation: Object > Custom fields > {schema_name}")
                continue
            sysmsg.trace(f"⚙️ Processing checksums: Object > Custom fields > {schema_name} ...")
            sql_query = f"""
                          SELECT object_type, object_id,
                                 MD5(GROUP_CONCAT(MD5(CONCAT(
                                    MD5(COALESCE(field_language, "__null__")), MD5(COALESCE(field_name, "__null__")), MD5(COALESCE(field_value, "__null__"))
                                 )) ORDER BY field_language, field_name, field_value)) AS checksum_val
                            FROM {schema_name}.Data_N_Object_T_CustomFields c
                      INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                            WHERE object_type NOT IN ('Slide', 'Transcript')
                              AND c.record_deleted = 0
                              AND t.flag_type = 'fields'
                              AND t.to_process = 1
                          GROUP BY object_type, object_id
              """
            self._upsert_checksums(engine_name, cache_schema, sql_query, 'Operations_N_Object_T_ChecksumsCustomFields',
                                   ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, 'oTWu6bBL')

        # Final object checksums combine the three partial checksums.
        sysmsg.trace("⚙️ Processing checksums: Object > Final checksums ...")
        final_query = f"""
                      SELECT object_type, o.object_id,
                             MD5(CONCAT(COALESCE(o.checksum_val, "__null__"), COALESCE(p.checksum_val, "__null__"), COALESCE(c.checksum_val, "__null__"))) AS checksum_val
                        FROM {cache_schema}.Operations_N_Object_T_ChecksumsObject o
                   LEFT JOIN {cache_schema}.Operations_N_Object_T_ChecksumsPageProfile p
                        USING (object_type, object_id)
                   LEFT JOIN {cache_schema}.Operations_N_Object_T_ChecksumsCustomFields c
                        USING (object_type, object_id)
                  INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                        USING (object_type)
                        WHERE t.flag_type = 'fields'
                          AND t.to_process = 1
              """
        self._upsert_checksums(engine_name, cache_schema, final_query, 'Operations_N_Object_T_Checksums',
                               ['object_type', 'object_id'], ['checksum_val'], ['object_type'], actions, 'y0yFAafh')

        # Apply the final checksums to the airflow table, commit mode only.
        sysmsg.trace("⚙️ Processing checksums: Object > Applying to Airflow ...")
        if 'commit' in actions:
            apply_query = f"""
                          UPDATE {airflow_schema}.{self._NODE_TABLE} f
                      INNER JOIN {cache_schema}.Operations_N_Object_T_Checksums c
                           USING (object_type, object_id)
                      INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                              SET f.checksum_current = c.checksum_val
                            WHERE t.flag_type = 'fields'
                              AND t.to_process = 1
                              AND f.deleted = 0
                """
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = apply_query,
                verbose     = self.verbose,
                query_id    = 'j65waWD2',
            )

        #----------------#
        # Edge checksums #
        #----------------#
        sysmsg.trace("☑️ Done processing checksums for Object.")

        # Loop over all data schemas for the edge families.
        for schema_name in (self.global_config.schema_registry, self.global_config.schema_lectures, self.global_config.schema_ontology):

            # Skip schemas whose edge endpoint types are all outside the scope.
            if len(set(obj2obj_types) & set(self.global_config.schema_to_object_types[schema_name])) == 0:
                sysmsg.trace(f"➡️ Skipping calculation: Object-to-Object > General registry > {schema_name}")
                continue
            sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > General registry > {schema_name} ...")
            sql_query = f"""
                          SELECT from_object_type, from_object_id, to_object_type, to_object_id, context,
                                 MD5(CONCAT(
                                    MD5(COALESCE(from_object_type, "__null__")), MD5(COALESCE(from_object_id, "__null__")),
                                    MD5(COALESCE(  to_object_type, "__null__")), MD5(COALESCE(  to_object_id, "__null__")),
                                    MD5(COALESCE(`context`, "__null__"))
                                 )) AS checksum_val
                            FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent e
                      INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                            WHERE e.record_deleted = 0
                              AND t.to_process = 1
              """
            self._upsert_checksums(engine_name, cache_schema, sql_query, 'Operations_N_Object_N_Object_T_ChecksumsObject',
                                   ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                                   ['checksum_val'], ['from_object_type', 'to_object_type'], actions, 'LnzeNnx1')

        # Edge custom fields checksums aggregate the per-field digests per edge.
        for schema_name in (self.global_config.schema_registry, self.global_config.schema_lectures, self.global_config.schema_ontology):

            # Skip schemas whose edge endpoint types are all outside the scope.
            if len(set(obj2obj_types) & set(self.global_config.schema_to_object_types[schema_name])) == 0:
                sysmsg.trace(f"➡️ Skipping calculation: Object-to-Object > Custom fields > {schema_name}")
                continue
            sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > Custom fields > {schema_name} ...")
            sql_query = f"""
                          SELECT from_object_type, from_object_id, to_object_type, to_object_id, context,
                                 MD5(GROUP_CONCAT(MD5(CONCAT(
                                    MD5(COALESCE(field_language, "__null__")), MD5(COALESCE(field_name, "__null__")), MD5(COALESCE(field_value, "__null__"))
                                 )) ORDER BY field_language, field_name, field_value)) AS checksum_val
                            FROM {schema_name}.Data_N_Object_N_Object_T_CustomFields c
                      INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                            WHERE c.record_deleted = 0
                             AND to_process = 1
                          GROUP BY from_object_type, from_object_id, to_object_type, to_object_id, context
              """
            self._upsert_checksums(engine_name, cache_schema, sql_query, 'Operations_N_Object_N_Object_T_ChecksumsCustomFields',
                                   ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                                   ['checksum_val'], ['from_object_type', 'to_object_type'], actions, 'WZ4gEw01')

        # Final edge checksums combine the two partial checksums.
        sysmsg.trace("⚙️ Processing checksums: Object-to-Object > Final checksums ...")
        final_edge_query = f"""
                      SELECT o.from_object_type, o.from_object_id, o.to_object_type, o.to_object_id, o.context,
                             MD5(CONCAT(COALESCE(o.checksum_val, "__null__"), COALESCE(c.checksum_val, "__null__"))) AS checksum_val
                        FROM {cache_schema}.Operations_N_Object_N_Object_T_ChecksumsObject o
                   LEFT JOIN {cache_schema}.Operations_N_Object_N_Object_T_ChecksumsCustomFields c
                        USING (from_object_type, from_object_id, to_object_type, to_object_id, context)
                  INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags t
                        USING (from_object_type, to_object_type)
                        WHERE to_process = 1
            """
        self._upsert_checksums(engine_name, cache_schema, final_edge_query, 'Operations_N_Object_N_Object_T_Checksums',
                               ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                               ['checksum_val'], ['from_object_type', 'to_object_type'], actions, 'JJQ2pj3y')

        # Apply the final edge checksums to the airflow table, commit mode only.
        sysmsg.trace("⚙️ Processing checksums: Object-to-Object > Applying to Airflow ...")
        if 'commit' in actions:
            apply_edge_query = f"""
                          UPDATE {airflow_schema}.{self._EDGE_TABLE} f
                      INNER JOIN {cache_schema}.Operations_N_Object_N_Object_T_Checksums c
                           USING (from_object_type, from_object_id, to_object_type, to_object_id, context)
                      INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                              SET f.checksum_current = c.checksum_val
                            WHERE t.to_process = 1
                              AND f.deleted = 0
                """
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = apply_edge_query,
                verbose     = self.verbose,
                query_id    = 'Fpas6ysH',
            )
        sysmsg.trace("☑️ Done processing checksums for Object-to-Object.")
        sysmsg.success("🧩 ✅ Done updating object checksums.")

    # Internal Method: Upsert one checksum query through the GraphDB
    # safe-insert helper, forwarding the action set for eval/print handling.
    def _upsert_checksums(self, engine_name: str, cache_schema: str, query: str, table_name: str,
                          key_column_names: list[str], upd_column_names: list[str], eval_column_names: list[str],
                          actions: ActionSet, query_id: str) -> None:
        self.db.execute_query_as_safe_inserts(
            engine_name       = engine_name,
            schema_name       = cache_schema,
            table_name        = table_name,
            query             = query,
            key_column_names  = key_column_names,
            upd_column_names  = upd_column_names,
            eval_column_names = eval_column_names,
            actions           = actions,
            verbose           = self.verbose,
            query_id          = query_id,
        )

    # Internal Method: Build the page-profile checksum expression from the
    # column list, reproducing the legacy hand-written MD5/COALESCE chain
    # byte for byte.
    @staticmethod
    def _page_profile_checksum_expression() -> str:
        parts = ", ".join(
            f'MD5(COALESCE({column}, "{_NULL_SENTINEL}"))'
            for column in PAGE_PROFILE_CHECKSUM_COLUMNS
        )
        return f"MD5(CONCAT({parts}))"

    #================================================================#
    # Method Group: Refresh                                          #
    #================================================================#

    # Public Method: Derive has_changed from the stored checksums and schedule
    # the records that drifted, expired, or were never cached, mirroring the
    # legacy refresh command over both families.
    def refresh_flags(self, scope: ProcessingScope, limit_per_type: int | None = None, actions: ActionSet = ('commit',)) -> list[NodeRefreshStats | EdgeRefreshStats]:

        # The legacy refresh applies a default per-type limit of 100 rows.
        limit_per_type = limit_per_type if limit_per_type is not None else 100
        sysmsg.info("🧩 🏁 📝 Refresh checksums and set 'to_process' flags to 1 in 'FieldsChanged' airflow tables.")
        sysmsg.trace(f"Input parameters: limit_per_type={limit_per_type} (rows).")
        stats = []
        stats.extend(self._refresh_fields_changed(scope, limit_per_type, actions))
        stats.extend(self._refresh_scores_expired(scope, limit_per_type, actions))
        return stats

    # Internal Method: Refresh the FieldsChanged family and return its stats.
    def _refresh_fields_changed(self, scope: ProcessingScope, limit_per_type: int, actions: ActionSet) -> list[NodeRefreshStats | EdgeRefreshStats]:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        node_condition = self._node_fields_condition(scope)
        edge_condition = self._edge_fields_condition(scope)
        stats = []

        sysmsg.trace("Set 'to_process' flags to 1.")

        # Derive has_changed by comparing current and previous checksums.
        # The node records join the node typeflags without a flag_type
        # filter, matching types active in either family. The edge records
        # join the edge typeflags by their endpoint pair — the legacy query
        # joined the NODE typeflags with the edge columns, which can never
        # resolve; fixed here as a documented deviation (E2E finding,
        # 2026-09-23).
        node_changed_query = f"""
              UPDATE {schema_name}.{self._NODE_TABLE} f
          INNER JOIN {schema_name}.Operations_N_Object_T_TypeFlags t
               USING (object_type)
                  SET has_changed = (f.checksum_current != f.checksum_previous)
                WHERE t.to_process = 1
                  AND f.deleted = 0
        """
        self._execute_write(engine_name, node_changed_query, 'iGojjBW7', actions)

        # The edge derivation joins the edge typeflags table.
        edge_changed_query = f"""
              UPDATE {schema_name}.{self._EDGE_TABLE} f
          INNER JOIN {schema_name}.Operations_N_Object_N_Object_T_TypeFlags t
               USING (from_object_type, to_object_type)
                  SET has_changed = (f.checksum_current != f.checksum_previous)
                WHERE t.to_process = 1
                  AND f.deleted = 0
        """
        self._execute_write(engine_name, edge_changed_query, 'Hy3LQ6tJ', actions)

        # Reset then set the to_process flags on both tables.
        for table_name in (self._NODE_TABLE, self._EDGE_TABLE):
            sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")
            condition = node_condition if table_name == self._NODE_TABLE else edge_condition
            if condition == "FALSE":
                sysmsg.trace(f"Nothing to do for table '{table_name}'. Check the processing scope.")
                continue
            reset_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET to_process = 0
                 WHERE to_process = 1
                   AND {condition}
            """
            self._execute_write(engine_name, reset_query, 'MsnEuv05', actions)

            # Table key definitions for the ranked selection below.
            table_type_key = "object_type" if table_name == self._NODE_TABLE else "from_object_type, to_object_type, context"
            table_full_key = "object_type, object_id" if table_name == self._NODE_TABLE else "from_object_type, from_object_id, to_object_type, to_object_id, context"
            select_query = f"""
                      UPDATE {schema_name}.{table_name} t2u
                  INNER JOIN (SELECT {table_full_key}
                                FROM (SELECT {table_full_key}, ROW_NUMBER() OVER (PARTITION BY {table_type_key}) AS row_to_process
                                        FROM {schema_name}.{table_name}
                                       WHERE (has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL)
                                         AND ({condition})
                                      ) tA
                                WHERE row_to_process <= {limit_per_type}
                              ) tB
                        USING ({table_full_key})
                          SET t2u.to_process = 1
            """
            self._execute_write(engine_name, select_query, 'ye472zFQ', actions)

        # Collect the per-group statistics of both tables.
        sysmsg.trace("Fetch stats on what to process.")
        for table_name in (self._NODE_TABLE, self._EDGE_TABLE):
            is_node_table = table_name == self._NODE_TABLE
            group_key = "object_type" if is_node_table else "from_object_type, to_object_type"
            stats_query = f"""
                SELECT {group_key},
                       SUM(    ISNULL(last_date_cached)                                    ) AS new_or_never_cached,
                       SUM(NOT ISNULL(last_date_cached) AND     has_changed                ) AS checksum_changed,
                       SUM(NOT ISNULL(last_date_cached) AND NOT has_changed AND has_expired) AS cache_expired,
                       SUM(to_process)                                                       AS to_process
                  FROM {schema_name}.{table_name}
                 WHERE deleted = 0
              GROUP BY {group_key}
                HAVING new_or_never_cached + checksum_changed + cache_expired > 0
            """
            rows = self.db.execute_query(engine_name=engine_name, query=stats_query, query_id='4QF4Lh4y')

            # Render the per-group evaluation table and its total in the
            # legacy print_dataframe format.
            key_width = 1 if is_node_table else 2
            headers = (['object_type'] if is_node_table else ['from_object_type', 'to_object_type']) \
                + ['new_or_never_cached', 'checksum_changed', 'cache_expired', 'to_process']
            table_rows = [
                [value if idx < key_width else int(value) for idx, value in enumerate(row)]
                for row in rows
            ]
            _print_refresh_table(
                rows    = table_rows,
                headers = headers,
                title   = f'\n🔍 Evaluation results for table: "{table_name}"',
            )
            total_row = [sum(row[idx] for row in table_rows) for idx in range(key_width, len(headers))]
            _print_refresh_table(
                rows    = [total_row],
                headers = ['TOTAL', 'new_or_never_cached', 'checksum_changed', 'cache_expired', 'to_process'],
                title   = f'\n🔍 Evaluation results for table: "{table_name}"',
            )
            for row in rows:
                if is_node_table:
                    stats.append(NodeRefreshStats(
                        object_type         = row[0],
                        new_or_never_cached = row[1],
                        checksum_changed    = row[2],
                        cache_expired       = row[3],
                        to_process          = row[4],
                    ))
                else:
                    stats.append(EdgeRefreshStats(
                        edge_type_pair      = EdgeTypePair(from_object_type=row[0], to_object_type=row[1]),
                        new_or_never_cached = row[2],
                        checksum_changed    = row[3],
                        cache_expired       = row[4],
                        to_process          = row[5],
                    ))
        sysmsg.success("🧩 🏁 ✅ Done refreshing checksums and setting 'to_process' flags in 'FieldsChanged' airflow tables.\n")
        return stats

    # Internal Method: Refresh the ScoresExpired family and return its stats.
    def _refresh_scores_expired(self, scope: ProcessingScope, limit_per_type: int, actions: ActionSet) -> list[NodeRefreshStats]:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        sysmsg.info("🏁 📝 Set 'to_process' flags to 1 in 'ScoresExpired' airflow tables.")
        sysmsg.trace("⚙️  Processing 'Operations_N_Object_T_ScoresExpired' table ...")
        condition = self._node_scores_condition(scope)

        # Skip the family entirely when no scores types are active.
        if condition == "FALSE":
            sysmsg.trace("Nothing to do for table 'Operations_N_Object_T_ScoresExpired'. Check the processing scope.")
            return []

        # Reset then set the to_process flags on the scores table.
        reset_query = f"""
            UPDATE {schema_name}.{self._SCORES_TABLE}
               SET to_process = 0
             WHERE to_process = 1
               AND {condition}
        """
        self._execute_write(engine_name, reset_query, 'q4L84LJy', actions)
        select_query = f"""
                  UPDATE {schema_name}.{self._SCORES_TABLE} t2u
              INNER JOIN (SELECT object_type, object_id
                            FROM (SELECT object_type, object_id, ROW_NUMBER() OVER (PARTITION BY object_type) AS row_to_process
                                    FROM {schema_name}.{self._SCORES_TABLE}
                                   WHERE (has_expired = 1 OR last_date_cached IS NULL)
                                     AND ({condition})
                                  ) tA
                            WHERE row_to_process <= {limit_per_type}
                          ) tB
                   USING (object_type, object_id)
                      SET t2u.to_process = 1
        """
        self._execute_write(engine_name, select_query, 'tBKyps8J', actions)

        # Collect the per-type statistics of the scores table.
        stats_query = f"""
            SELECT object_type,
                   SUM(    ISNULL(last_date_cached)                ) AS new_or_never_cached,
                   SUM(NOT ISNULL(last_date_cached) AND has_expired) AS cache_expired
              FROM {schema_name}.{self._SCORES_TABLE}
             WHERE deleted = 0
          GROUP BY object_type
            HAVING new_or_never_cached + cache_expired > 0
        """
        rows = self.db.execute_query(engine_name=engine_name, query=stats_query, query_id='JH9iFxCF')

        # Render the per-type evaluation table and its total in the legacy
        # print_dataframe format; the scores family carries two stat columns.
        table_rows = [[row[0], int(row[1]), int(row[2])] for row in rows]
        _print_refresh_table(
            rows    = table_rows,
            headers = ['object_type', 'new_or_never_cached', 'cache_expired'],
            title   = f'\n🔍 Evaluation results for table: "{self._SCORES_TABLE}"',
        )
        total_row = [sum(row[1] for row in table_rows), sum(row[2] for row in table_rows)]
        _print_refresh_table(
            rows    = [total_row],
            headers = ['TOTAL', 'new_or_never_cached', 'cache_expired'],
            title   = f'\n🔍 Evaluation results for table: "{self._SCORES_TABLE}"',
        )
        sysmsg.success("🏁 ✅ Done setting 'to_process' flags in 'ScoresExpired' airflow tables.\n")
        return [
            NodeRefreshStats(
                object_type         = row[0],
                new_or_never_cached = row[1],
                cache_expired       = row[2],
            )
            for row in rows
        ]

    #================================================================#
    # Method Group: Expiration                                       #
    #================================================================#

    # Public Method: Set the staleness flag on tracking records according to
    # the expiration policy and processing scope, mirroring the legacy
    # Orchestration.expire command over both families.
    def apply_expiration(self, policy: ExpirationPolicy, scope: ProcessingScope, count_only: bool = False, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        stats = []
        if policy.include_fields:
            stats.extend(self._expire_fields_changed(policy, scope, count_only, actions))
        if policy.include_scores:
            stats.extend(self._expire_scores_expired(policy, scope, count_only, actions))
        return stats

    # Internal Method: Expire the FieldsChanged family per policy and scope.
    def _expire_fields_changed(self, policy: ExpirationPolicy, scope: ProcessingScope, count_only: bool, actions: ActionSet) -> list[PropagationStats]:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Build the scoped conditions, restricted to the policy's type subset.
        node_condition = self._restrict_to_object_types(self._node_fields_condition(scope), policy.object_types)
        edge_condition = self._restrict_to_object_types(self._edge_fields_condition(scope), policy.object_types, edges=True)

        # The legacy command uses a large sentinel for an unlimited per-type
        # budget; the policy's None maps onto it.
        limit_per_type = policy.limit_per_type if policy.limit_per_type is not None else 9999999999

        # Optional date filter; older_than of zero days disables it.
        date_filter = ""
        if policy.older_than.total_seconds() > 0:
            older_than_days = int(policy.older_than.total_seconds() // 86400)
            date_filter = f"AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than_days} DAY"

        # Include flags gate which tables of the family are expired.
        include_node_table = policy.include_nodes and node_condition != "FALSE"
        include_edge_table = policy.include_edges and edge_condition != "FALSE"
        if not include_node_table and not include_edge_table:
            sysmsg.warning("Nothing to do for the FieldsChanged family. Check the policy or processing scope.")
            return []

        # Collect the per-group expiration counts when counting only.
        stats = []
        for table_name, condition in ((self._NODE_TABLE, node_condition), (self._EDGE_TABLE, edge_condition)):
            if (table_name == self._NODE_TABLE and not include_node_table) or (table_name == self._EDGE_TABLE and not include_edge_table):
                continue
            group_key = "object_type" if table_name == self._NODE_TABLE else "from_object_type, to_object_type"

            # Reset all expiration flags before selecting new expired rows.
            reset_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET has_expired = 0
                 WHERE has_expired = 1
                   AND {condition}
            """
            self._execute_write(engine_name, reset_query, 'MY52N1XY', actions)

            # Rank the candidate rows per group and expire the top rows.
            if not count_only:
                expire_query = f"""
                      UPDATE {schema_name}.{table_name} t
                        JOIN (SELECT row_id
                                FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY {group_key} ORDER BY row_id) AS rn
                                        FROM {schema_name}.{table_name}
                                       WHERE ({condition})
                                         {date_filter}
                                     ) ranked
                               WHERE rn <= {limit_per_type}
                             ) ranked_rows
                         ON t.row_id = ranked_rows.row_id
                        SET t.has_expired = 1
                      WHERE {condition}
                """
                self._execute_write(engine_name, expire_query, '10PxduJu', actions)
            else:
                # Count the rows that would be expired, per group.
                count_query = f"""
                    SELECT {group_key}, COUNT(*) AS rows_to_be_set
                      FROM {schema_name}.{table_name} t
                      JOIN (SELECT row_id
                              FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY {group_key} ORDER BY row_id) AS rn
                                      FROM {schema_name}.{table_name}
                                     WHERE ({condition})
                                       {date_filter}
                                   ) ranked
                             WHERE rn <= {limit_per_type}
                           ) ranked_rows
                          ON ranked_rows.row_id = t.row_id
                      WHERE {condition}
                   GROUP BY {group_key}
                """
                rows = self.db.execute_query(engine_name=engine_name, query=count_query, query_id='d5GKbPVP')
                for row in rows:
                    stats.append(PropagationStats(target=f"{table_name}:{row[0]}", rows_flagged=row[-1]))
        return stats

    # Internal Method: Expire the ScoresExpired family per policy and scope.
    def _expire_scores_expired(self, policy: ExpirationPolicy, scope: ProcessingScope, count_only: bool, actions: ActionSet) -> list[PropagationStats]:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Build the scoped node condition, restricted to the policy's subset.
        condition = self._restrict_to_object_types(self._node_scores_condition(scope), policy.object_types)
        if not policy.include_nodes or condition == "FALSE":
            sysmsg.warning("Nothing to do for the ScoresExpired family. Check the policy or processing scope.")
            return []

        # The legacy command uses a large sentinel for an unlimited budget.
        limit_per_type = policy.limit_per_type if policy.limit_per_type is not None else 9999999999

        # Optional date filter; older_than of zero days disables it.
        date_filter = ""
        if policy.older_than.total_seconds() > 0:
            older_than_days = int(policy.older_than.total_seconds() // 86400)
            date_filter = f"AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than_days} DAY"

        # Reset all expiration flags before selecting new expired rows.
        reset_query = f"""
            UPDATE {schema_name}.{self._SCORES_TABLE}
               SET has_expired = 0
             WHERE has_expired = 1
               AND {condition}
        """
        self._execute_write(engine_name, reset_query, 'Zsz9iF13', actions)

        # Rank the candidate rows per type and expire the top rows.
        if not count_only:
            expire_query = f"""
                UPDATE {schema_name}.{self._SCORES_TABLE} t
                  JOIN (SELECT row_id
                          FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY row_id) AS rn
                                  FROM {schema_name}.{self._SCORES_TABLE}
                                 WHERE ({condition})
                                   {date_filter}
                               ) ranked
                         WHERE rn <= {limit_per_type}
                       ) ranked_rows
                   ON t.row_id = ranked_rows.row_id
                  SET t.has_expired = 1
                WHERE {condition}
            """
            self._execute_write(engine_name, expire_query, '6nKcLVme', actions)
            return []

        # Count the rows that would be expired, per type.
        count_query = f"""
            SELECT object_type, COUNT(*) AS rows_to_be_set
              FROM {schema_name}.{self._SCORES_TABLE} t
              JOIN (SELECT row_id
                      FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY row_id) AS rn
                              FROM {schema_name}.{self._SCORES_TABLE}
                             WHERE ({condition})
                               {date_filter}
                           ) ranked
                     WHERE rn <= {limit_per_type}
                   ) ranked_rows
                  ON ranked_rows.row_id = t.row_id
              WHERE {condition}
           GROUP BY object_type
        """
        rows = self.db.execute_query(engine_name=engine_name, query=count_query, query_id='46PNmQvh')
        return [PropagationStats(target=f"{self._SCORES_TABLE}:{row[0]}", rows_flagged=row[1]) for row in rows]

    #================================================================#
    # Method Group: Rollover and cache dates                         #
    #================================================================#

    # Public Method: Close the drift window by adopting the current checksums
    # as the previous ones, mirroring the legacy rollover command.
    def rollover_checksums(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        sysmsg.info("⬅️  📝 Rollover checksums (make previous checksum equal to current) in 'FieldsChanged' airflow tables.")
        node_condition = self._node_fields_condition(scope)
        edge_condition = self._edge_fields_condition(scope)

        # Loop over the FieldsChanged tables, evaluating or committing per action.
        for table_name in (self._NODE_TABLE, self._EDGE_TABLE):
            condition = node_condition if table_name == self._NODE_TABLE else edge_condition
            if condition == "FALSE":
                sysmsg.trace(f"Nothing to do for table '{table_name}'. Check the processing scope.")
                continue
            group_key = "object_type" if table_name == self._NODE_TABLE else "from_object_type, to_object_type"
            eval_query = f"""
                SELECT {group_key}, COUNT(*) AS n_to_rollover
                  FROM {schema_name}.{table_name}
                 WHERE (   COALESCE(checksum_previous, '__null__') != COALESCE(checksum_current, '__null__')
                        OR has_changed > 0.5
                        OR (has_changed IS NULL AND checksum_current IS NOT NULL)
                       )
                   AND {condition}
                   AND to_process = 1
              GROUP BY {group_key}
            """
            if 'eval' in actions:
                self.db.execute_query(engine_name=engine_name, query=eval_query, query_id='D3YbxeVt')
            commit_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET checksum_previous = checksum_current, has_changed = 0
                 WHERE (   COALESCE(checksum_previous, '__null__') != COALESCE(checksum_current, '__null__')
                        OR has_changed > 0.5
                        OR (has_changed IS NULL AND checksum_current IS NOT NULL)
                       )
                   AND {condition}
                   AND to_process = 1
            """
            self._execute_write(engine_name, commit_query, 'ht5AZcsE', actions)
        sysmsg.success("⬅️  ✅ Done rolling over checksums.")

    # Public Method: Stamp the cache date of the processed records, mirroring
    # the legacy update_dates command over both families.
    def update_cache_dates(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        self._update_dates_fields_changed(scope, actions)
        self._update_dates_scores_expired(scope, actions)

    # Internal Method: Stamp the cache dates of the FieldsChanged tables.
    def _update_dates_fields_changed(self, scope: ProcessingScope, actions: ActionSet) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        sysmsg.info("⬅️  📝 Update last_date_cached values in 'FieldsChanged' airflow tables.")
        node_condition = self._node_fields_condition(scope)
        edge_condition = self._edge_fields_condition(scope)

        # Loop over the FieldsChanged tables with their scoped conditions.
        for table_name in (self._NODE_TABLE, self._EDGE_TABLE):
            condition = node_condition if table_name == self._NODE_TABLE else edge_condition
            if condition == "FALSE":
                sysmsg.trace(f"Nothing to do for table '{table_name}'. Check the processing scope.")
                continue
            group_key = "object_type" if table_name == self._NODE_TABLE else "from_object_type, to_object_type"

            # Evaluation query counting the rows whose date would change.
            eval_query = f"""
                SELECT {group_key}, COUNT(*) AS n_to_update
                  FROM {schema_name}.{table_name}
                 WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                   AND {condition}
                   AND to_process = 1
              GROUP BY {group_key}
            """
            if 'eval' in actions:
                self.db.execute_query(engine_name=engine_name, query=eval_query, query_id='kpAX4Cft')

            # Commit query stamping today's date and clearing the expiry flag.
            commit_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET last_date_cached = DATE(NOW()), has_expired = 0
                 WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                   AND {condition}
                   AND to_process = 1
            """
            self._execute_write(engine_name, commit_query, 'Q2dracb0', actions)
        sysmsg.success("⬅️  ✅ Done updating last_date_cached values in 'FieldsChanged' airflow tables.")

    # Internal Method: Stamp the cache dates of the ScoresExpired table.
    def _update_dates_scores_expired(self, scope: ProcessingScope, actions: ActionSet) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        sysmsg.info("⬅️  📝 Update last_date_cached values in 'ScoresExpired' airflow tables.")
        condition = self._node_scores_condition(scope)

        # Skip the family entirely when no scores types are active.
        if condition == "FALSE":
            sysmsg.trace("Nothing to do for table 'Operations_N_Object_T_ScoresExpired'. Check the processing scope.")
            return

        # Evaluation query counting the rows whose date would change.
        eval_query = f"""
            SELECT object_type, COUNT(*) AS n_to_update
              FROM {schema_name}.{self._SCORES_TABLE}
             WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
               AND {condition}
               AND to_process = 1
          GROUP BY object_type
        """
        if 'eval' in actions:
            self.db.execute_query(engine_name=engine_name, query=eval_query, query_id='y9GdvZ4W')

        # Commit query stamping today's date and clearing the expiry flag. The
        # legacy shell call carries no query id; the omission is preserved.
        commit_query = f"""
            UPDATE {schema_name}.{self._SCORES_TABLE}
               SET last_date_cached = DATE(NOW()), has_expired = 0
             WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
               AND {condition}
               AND to_process = 1
        """
        if 'print' in actions:
            print_sql(commit_query, title='ERWG42')
        if 'commit' in actions:
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = commit_query,
                verbose     = self.verbose or 'print' in actions,
            )
        sysmsg.success("⬅️  ✅ Done updating last_date_cached values in 'ScoresExpired' airflow tables.")

    #================================================================#
    # Method Group: Resets                                           #
    #================================================================#

    # Public Method: Clear the to_process flags of the tracked records in
    # scope, mirroring the conditional resets of both legacy families.
    def reset_flags(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # FieldsChanged tables reset under the fields-family conditions.
        for table_name, condition in (
            (self._NODE_TABLE, self._node_fields_condition(scope)),
            (self._EDGE_TABLE, self._edge_fields_condition(scope)),
        ):
            if condition == "FALSE":
                sysmsg.trace(f"Nothing to do for table '{table_name}'. Check the processing scope.")
                continue
            reset_query = f"""
                UPDATE {schema_name}.{table_name}
                   SET to_process = 0
                 WHERE to_process = 1
                   AND {condition}
            """
            self._execute_write(engine_name, reset_query, 'RWCE1vkr', actions)

        # ScoresExpired table resets under the scores-family condition.
        scores_condition = self._node_scores_condition(scope)
        if scores_condition == "FALSE":
            sysmsg.trace("Nothing to do for table 'Operations_N_Object_T_ScoresExpired'. Check the processing scope.")
            return
        scores_reset_query = f"""
            UPDATE {schema_name}.{self._SCORES_TABLE}
               SET to_process = 0
             WHERE to_process = 1
               AND {scores_condition}
        """
        self._execute_write(engine_name, scores_reset_query, 'AhJepYi8', actions)

    # Public Method: Clear the processing, drift, and staleness flags of all
    # tracking records unconditionally, mirroring the airflow option of the
    # legacy Orchestration.reset command.
    def clear_all_flags(self, clear_has_expired: bool = True, actions: ActionSet = ('commit',)) -> None:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # The legacy reset banners, reflecting whether has_expired is cleared.
        if clear_has_expired:
            sysmsg.info("🧹 📝 Reset 'to_process', 'has_changed' and 'has_expired' flags in graph_airflow tables.")
        else:
            sysmsg.info("🧹 📝 Reset 'to_process' and 'has_changed' flags in graph_airflow tables.")
        print('\nThe following tables will be affected:')
        for table_name in (self._EDGE_TABLE, self._NODE_TABLE, self._SCORES_TABLE):
            print(f" - {schema_name}.{table_name}")
        print('')
        sysmsg.trace(f"Processing '{schema_name}' fields and scores tables ...")

        # The legacy reset order: edge table, node table, scores table.
        for table_name in (self._EDGE_TABLE, self._NODE_TABLE, self._SCORES_TABLE):

            # Build the SET clause for the flags present on this table. When
            # clear_has_expired is False the expiry flag is preserved so a
            # previous expire command remains in effect during planning.
            set_parts = ["to_process = 0"]
            where_parts = ["to_process = 1"]
            if self.db.has_column(engine_name=engine_name, schema_name=schema_name, table_name=table_name, column_name='has_changed'):
                set_parts.append("has_changed = 0")
                where_parts.append("has_changed = 1")
            if clear_has_expired and self.db.has_column(engine_name=engine_name, schema_name=schema_name, table_name=table_name, column_name='has_expired'):
                set_parts.append("has_expired = 0")
                where_parts.append("has_expired = 1")
            reset_query = f"UPDATE {schema_name}.{table_name} SET {', '.join(set_parts)} WHERE {' OR '.join(where_parts)};"
            self._execute_write(engine_name, reset_query, '5LEjczg5', actions)
        if clear_has_expired:
            sysmsg.success(f"🧹 ✅ Done resetting 'to_process', 'has_changed' and 'has_expired' flags in '{schema_name}' tables.")
        else:
            sysmsg.success(f"🧹 ✅ Done resetting 'to_process' and 'has_changed' flags in '{schema_name}' tables.")

    #================================================================#
    # Method Group: Record reads                                     #
    #================================================================#

    # Public Method: Retrieve the change-tracking record of a single node.
    def get_node_record(self, key: NodeKey) -> ObjectChangeRecord | None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        rows = self.db.get_cells(
            engine_name = engine_name,
            schema_name = schema_name,
            table_name  = self._NODE_TABLE,
            select      = ['object_type', 'object_id', 'checksum_current', 'checksum_previous', 'has_changed', 'last_date_cached', 'has_expired', 'to_process', 'deleted'],
            where       = [
                ('object_type', key.object_type),
                ('object_id',   key.object_id),
            ],
        )
        if not rows:
            return None
        return self._row_to_object_record(rows[0])

    # Public Method: Retrieve the change-tracking record of a single edge.
    def get_edge_record(self, key: EdgeKey) -> EdgeChangeRecord | None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        rows = self.db.get_cells(
            engine_name = engine_name,
            schema_name = schema_name,
            table_name  = self._EDGE_TABLE,
            select      = ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context', 'checksum_current', 'checksum_previous', 'has_changed', 'last_date_cached', 'has_expired', 'to_process', 'deleted'],
            where       = [
                ('from_object_type', key.from_object_type),
                ('from_object_id',   key.from_object_id),
                ('to_object_type',   key.to_object_type),
                ('to_object_id',     key.to_object_id),
                ('context',          key.context),
            ],
        )
        if not rows:
            return None
        row = rows[0]
        return EdgeChangeRecord(
            key        = EdgeKey(from_object_type=row[0], from_object_id=row[1], to_object_type=row[2], to_object_id=row[3], context=row[4]),
            checksums  = self._row_to_checksum_pair(row[5], row[6]),
            state      = self._row_to_cache_state(row[8], row[9], row[10]),
            deleted    = bool(row[11]),
        )

    # Public Method: Retrieve the score-expiry record of a single node.
    def get_score_expiry_record(self, key: NodeKey) -> ObjectScoreExpiryRecord | None:
        engine_name, schema_name = self.schema_resolver.for_airflow()
        rows = self.db.get_cells(
            engine_name = engine_name,
            schema_name = schema_name,
            table_name  = self._SCORES_TABLE,
            select      = ['object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process', 'deleted'],
            where       = [
                ('object_type', key.object_type),
                ('object_id',   key.object_id),
            ],
        )
        if not rows:
            return None
        row = rows[0]
        return ObjectScoreExpiryRecord(
            key     = NodeKey(object_type=row[0], object_id=row[1]),
            state   = self._row_to_cache_state(row[2], row[3], row[4]),
            deleted = bool(row[5]),
        )

    #================================================================#
    # Method Group: Row mapping helpers                              #
    #================================================================#

    # Internal Method: Map one FieldsChanged node row onto the domain record.
    def _row_to_object_record(self, row: tuple) -> ObjectChangeRecord:
        return ObjectChangeRecord(
            key       = NodeKey(object_type=row[0], object_id=row[1]),
            checksums = self._row_to_checksum_pair(row[2], row[3]),
            state     = self._row_to_cache_state(row[5], row[6], row[7]),
            deleted   = bool(row[8]),
        )

    # Internal Method: Map checksum columns onto the checksum pair, treating
    # empty strings as absent values.
    @staticmethod
    def _row_to_checksum_pair(current: Any, previous: Any) -> ChecksumPair:
        return ChecksumPair(
            current  = Checksum(value=current) if current else None,
            previous = Checksum(value=previous) if previous else None,
        )

    # Internal Method: Map cache-state columns onto the CacheState value object.
    @staticmethod
    def _row_to_cache_state(last_date_cached: Any, has_expired: Any, to_process: Any) -> CacheState:
        cached_on = None
        if isinstance(last_date_cached, datetime):
            cached_on = last_date_cached.date()
        elif isinstance(last_date_cached, date):
            cached_on = last_date_cached
        return CacheState(
            last_date_cached = cached_on,
            has_expired      = bool(has_expired),
            to_process       = bool(to_process),
        )

    #================================================================#
    # Method Group: Status overview                                  #
    #================================================================#

    # Public Method: Retrieve the processing-status overview of the tracking
    # tables: the pending change records per node type and edge family, and
    # the pending score expiries per type.
    def get_status(self) -> TrackingStatus:
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Pending node change records per object type.
        node_counts = self.db.execute_query(
            engine_name = engine_name,
            query       = f"""
                SELECT object_type, COUNT(*) AS n_to_process
                  FROM {schema_name}.{self._NODE_TABLE}
                 WHERE to_process = 1
              GROUP BY object_type
            """,
            query_id    = 'TDgw7fYz',
        )

        # Pending edge change records per edge family.
        edge_counts = self.db.execute_query(
            engine_name = engine_name,
            query       = f"""
                SELECT from_object_type, to_object_type, COUNT(*) AS n_to_process
                  FROM {schema_name}.{self._EDGE_TABLE}
                 WHERE to_process = 1
              GROUP BY from_object_type, to_object_type
            """,
            query_id    = 'EqpDtL34',
        )

        # Pending score expiries per object type.
        scores_counts = self.db.execute_query(
            engine_name = engine_name,
            query       = f"""
                SELECT object_type, COUNT(*) AS n_to_process
                  FROM {schema_name}.{self._SCORES_TABLE}
                 WHERE to_process = 1
              GROUP BY object_type
            """,
            query_id    = 'ts8NQExF',
        )
        return TrackingStatus(
            node_fields_counts = [tuple(row) for row in node_counts],
            edge_fields_counts = [tuple(row) for row in edge_counts],
            scores_counts      = [tuple(row) for row in scores_counts],
        )
