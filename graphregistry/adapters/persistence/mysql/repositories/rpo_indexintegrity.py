# graphregistry/adapters/persistence/mysql/repositories/rpo_indexintegrity.py
from __future__ import annotations
from array import array
import os
import pickle
import re
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from tqdm import tqdm
from graphregistry.application.ports.repositories.prt_indexintegrity import IndexIntegrityRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Width of the progress bar and the overflow rank cap of the semantic tables.
PBWIDTH = 92
SEM_ROW_RANK_THRESHOLD = 32

# Pickle cache of the node mapping and edge list between runs, and the
# temporary SQL file used to write the largest component in chunks.
PICKLE_DIR = '/tmp/graphregistry_delete_loose_ends'
LARGEST_COMPONENT_SQL_PATH = '/tmp/sql_query_upd_largest_component.sql'

#==================#
# Class Definition #
#==================#
class MySQLIndexIntegrityRepository(IndexIntegrityRepository):
    """MySQL adapter for the IndexIntegrityRepository port.

    The SQL statements, query ids, the union-find computation, and the
    pickle caching are extracted verbatim from the legacy
    GraphRegistry.IndexDB.delete_loose_ends command. The five steps run in
    the legacy order; the final verification logs critical messages for any
    remaining orphans without aborting, as the legacy does.
    """

    # Public Method: Initialize the repository with an injected database
    # client, schema resolver, and global configuration.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, global_config: GlobalConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.global_config = global_config
        self.verbose = verbose

    # Internal Method: Escape a SQL string literal by doubling single quotes.
    @staticmethod
    def _sql_string(value) -> str:
        return str(value).replace("'", "''")

    # Public Method: Prune the loose ends from the index tables.
    def delete_loose_ends(self, refresh_graph: bool = True, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        _, es_cache_schema = self.schema_resolver.for_es_cache()
        cache_table_path = f"{cache_schema}.Operations_N_Object_T_LargestConnectedGraph"
        stats = PropagationStats(target="index_tables", rows_flagged=0)

        # Step 0: remove the orphaned overflow rows from the semantic tables.
        stats.rows_flagged += self._delete_overflow_sem_rows(engine_name, search_schema, actions)

        # Step 1: clean the deleted nodes from the page profile; the profile
        # seeds the graph analysis, so it must not contain deleted nodes.
        page_profile_rows_deleted = self._clean_page_profile(engine_name, search_schema, actions)
        stats.rows_flagged += page_profile_rows_deleted

        # Only invalidate the graph caches when rows were actually deleted,
        # so a no-op commit does not force a full rebuild.
        if page_profile_rows_deleted > 0:
            sysmsg.trace(f"Page profile modified ({page_profile_rows_deleted:,} rows); invalidating graph caches.")
            for pickle_path in self._pickle_paths():
                if os.path.exists(pickle_path):
                    os.remove(pickle_path)
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = f"TRUNCATE TABLE {cache_table_path};",
                verbose     = 'print' in actions,
                query_id    = 'lccinv',
            )

        # Step 2: calculate the largest connected component over the index
        # links, reusing the cached component when allowed.
        self._ensure_largest_component(engine_name, cache_schema, search_schema, cache_table_path, refresh_graph, actions)

        # Postcondition: the component must hold nodes whenever the graph
        # analysis had nodes to analyse. An empty component with a non-empty
        # page profile means the computation died mid-run, and every later
        # cleanup would no-op against garbage (E2E finding, 2026-09-25).
        page_profile_rows = self.db.execute_query(
            engine_name = engine_name,
            schema_name = search_schema,
            query       = f"SELECT COUNT(*) FROM {search_schema}.Data_N_Object_T_PageProfile;",
            query_id    = 'lccchkpp',
        )
        component_rows = self.db.execute_query(
            engine_name = engine_name,
            query       = f"SELECT COUNT(*) FROM {cache_table_path};",
            query_id    = 'lccchk',
        )
        page_profile_count = page_profile_rows[0][0] if page_profile_rows else 0
        component_count = component_rows[0][0] if component_rows else 0
        if page_profile_count > 0 and component_count == 0:
            raise RuntimeError(
                f"Largest connected component is empty while the page profile holds "
                f"{page_profile_count} nodes; the graph analysis failed to persist. "
                "Check the failure logged above."
            )

        # Step 4: clean up the index tables of both schemas against the
        # largest component as the valid-node reference.
        for schema_name in (search_schema, es_cache_schema):
            stats.rows_flagged += self._cleanup_schema_tables(engine_name, cache_schema, schema_name, actions)

        # Step 5: delete doc-link edges whose endpoints are missing from the
        # corresponding doc index tables.
        for schema_name in (search_schema, es_cache_schema):
            stats.rows_flagged += self._delete_dangling_doclink_refs(engine_name, schema_name, actions)

        # Final verification: report any remaining orphans.
        self._verify_integrity(engine_name, search_schema, es_cache_schema)
        return stats

    #================================================================#
    # Method Group: Step 0 - overflow ranks                          #
    #================================================================#

    # Internal Method: Remove the overflow and placeholder rank rows from
    # the semantic tables, capped at the patch threshold.
    def _delete_overflow_sem_rows(self, engine_name: str, search_schema: str, actions: ActionSet) -> int:
        rows_deleted = 0
        index_tables = self.db.get_tables_in_schema(
            engine_name = engine_name,
            schema_name = search_schema,
            use_regex   = [r'^Index_D_[^_]*_L_[^_]*_T_SEM+$'],
        )
        index_tables = [t for t in index_tables if not t.startswith('_')]
        for table_name in index_tables:
            sql_query_eval = f"""
               SELECT doc_type, link_type, COUNT(*) AS n_to_delete
                 FROM {search_schema}.{table_name}
                WHERE row_rank > {SEM_ROW_RANK_THRESHOLD}
                   OR row_rank = 99
             GROUP BY doc_type, link_type
            """
            if 'eval' in actions or 'commit' in actions:
                out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='rr99cnt')
                n_rows = sum(row[2] for row in out) if out else 0
                rows_deleted += n_rows
                if n_rows > 0 and 'commit' in actions:
                    sql_query_delete = f"DELETE FROM {search_schema}.{table_name} WHERE row_rank > {SEM_ROW_RANK_THRESHOLD} OR row_rank = 99"
                    self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_delete, verbose='print' in actions, query_id='rr99del')
        return rows_deleted

    #================================================================#
    # Method Group: Step 1 - page profile cleaning                   #
    #================================================================#

    # Internal Method: Clean the deleted nodes from the page profile
    # against a source-of-truth temp table of valid nodes.
    def _clean_page_profile(self, engine_name: str, search_schema: str, actions: ActionSet) -> int:
        _, cache_schema = self.schema_resolver.for_graph_cache()
        valid_nodes_source_table = f"{cache_schema}._tmp_valid_nodes_source_of_truth"
        self.db.execute_query_in_shell(
            engine_name = engine_name,
            query       = f"""
                DROP TABLE IF EXISTS {valid_nodes_source_table};
                CREATE TABLE {valid_nodes_source_table} (
                    object_type VARCHAR(255) NOT NULL,
                    object_id   VARCHAR(255) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                INSERT INTO {valid_nodes_source_table} (object_type, object_id)
                    SELECT object_type, object_id FROM {self.global_config.schema_registry}.Nodes_N_Object WHERE record_deleted = 0
                    UNION ALL
                    SELECT object_type, object_id FROM {self.global_config.schema_lectures}.Nodes_N_Object WHERE record_deleted = 0
                    UNION ALL
                    SELECT object_type, object_id FROM {self.global_config.schema_ontology}.Nodes_N_Category
                    UNION ALL
                    SELECT object_type, object_id FROM {self.global_config.schema_ontology}.Nodes_N_Concept;
            """,
            verbose     = 'print' in actions,
            query_id    = 'vnsrctbl',
        )

        # The deleted filter applies only when the profile carries the column.
        page_profile_table = f"{search_schema}.Data_N_Object_T_PageProfile"
        page_profile_columns = self.db.get_column_names(engine_name=engine_name, schema_name=search_schema, table_name='Data_N_Object_T_PageProfile')
        has_deleted_col = 'deleted' in page_profile_columns
        sql_query_eval = f"""
            SELECT t.object_type, COUNT(*) AS n_to_delete
              FROM {page_profile_table} t
         LEFT JOIN {valid_nodes_source_table} n
                ON n.object_type = t.object_type
               AND n.object_id   = t.object_id
              WHERE n.object_id IS NULL
                    {'AND t.deleted = 0' if has_deleted_col else ''}
          GROUP BY t.object_type
        """
        sql_query_commit = f"""
            DELETE FROM {page_profile_table}
             WHERE NOT EXISTS (
                   SELECT 1 FROM {valid_nodes_source_table} n
                    WHERE n.object_type = {page_profile_table}.object_type
                      AND n.object_id   = {page_profile_table}.object_id
               )
                    {'AND deleted = 0' if has_deleted_col else ''}
        """

        # The count always runs so the commit can skip when nothing deletes.
        out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='ppdleval', verbose='print' in actions)
        rows_to_delete = sum(row[1] for row in out) if out else 0
        if 'commit' in actions and rows_to_delete > 0:
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, query_id='ppdlcommit', verbose='print' in actions)
        self.db.execute_query_in_shell(
            engine_name = engine_name,
            query       = f"DROP TABLE IF EXISTS {valid_nodes_source_table};",
            verbose     = 'print' in actions,
            query_id    = 'vnsrcdrp',
        )
        return rows_to_delete

    #================================================================#
    # Method Group: Step 2 - largest connected component             #
    #================================================================#

    # Internal Method: Return the pickle cache paths of the node mapping
    # and edge list.
    @staticmethod
    def _pickle_paths() -> tuple[str, str]:
        os.makedirs(PICKLE_DIR, exist_ok=True)
        return (
            os.path.join(PICKLE_DIR, 'node_uid_to_row_id.pkl'),
            os.path.join(PICKLE_DIR, 'graph_edges.pkl'),
        )

    # Internal Method: Check whether the cached largest-component table
    # exists and is non-empty.
    def _graph_cache_exists(self, engine_name: str, cache_schema: str, cache_table_path: str) -> bool:
        try:
            _, cache_schema = self.schema_resolver.for_graph_cache()
            tables = self.db.get_tables_in_schema(
                engine_name = engine_name,
                schema_name = cache_schema,
                use_regex   = [r'^Operations_N_Object_T_LargestConnectedGraph$'],
            )
            if not tables:
                return False
            result = self.db.execute_query(engine_name=engine_name, query=f"SELECT 1 FROM {cache_table_path} LIMIT 1", query_id='cache_chk')
            return bool(result)
        except Exception:
            return False

    # Internal Method: Write the largest-component nodes to the cache table
    # in chunked inserts through a temporary SQL file.
    def _write_largest_component(self, engine_name: str, cache_table_path: str, nodes: list[tuple[str, str]], chunk_size: int = 10000) -> None:
        lines = [f"TRUNCATE TABLE {cache_table_path};"]
        for i in range(0, len(nodes), chunk_size):
            chunk = nodes[i:i + chunk_size]
            values = ", ".join(
                f"('{self._sql_string(object_type)}', '{self._sql_string(object_id)}')"
                for object_type, object_id in chunk
            )
            lines.append(f"INSERT INTO {cache_table_path} (object_type, object_id) VALUES {values};")
        with open(LARGEST_COMPONENT_SQL_PATH, 'w') as f:
            f.write("\n".join(lines))
        self.db.execute_query_from_file(engine_name=engine_name, file_path=LARGEST_COMPONENT_SQL_PATH, verbose=self.verbose)

    # Internal Method: Ensure the largest connected component is available
    # in the cache table, computing it with union-find when the cache is
    # missing, empty, or invalidated.
    def _ensure_largest_component(self, engine_name: str, cache_schema: str, search_schema: str, cache_table_path: str, refresh_graph: bool, actions: ActionSet) -> None:
        if not refresh_graph and self._graph_cache_exists(engine_name, cache_schema, cache_table_path):
            sysmsg.trace("SQL graph cache exists and is non-empty; using it.")
            return
        if refresh_graph:
            sysmsg.trace("refresh_graph is True; rebuilding SQL graph cache.")

        # Load or rebuild the node mapping and edge list from the pickle
        # caches of the temporary directory.
        nodes_pickle, edges_pickle = self._pickle_paths()
        if refresh_graph or not os.path.exists(nodes_pickle) or not os.path.exists(edges_pickle):

            # Build the node uid -> row_id mapping from the page profile.
            node_uids = self.db.execute_query(
                engine_name = engine_name,
                schema_name = search_schema,
                query       = f"SELECT CONCAT(object_type, '|', object_id) AS uid, row_id FROM {search_schema}.Data_N_Object_T_PageProfile;",
                query_id    = 'ndmap',
            )
            node_uid_to_row_id = {uid: row_id for uid, row_id in node_uids}
            with open(nodes_pickle, 'wb') as f:
                pickle.dump(node_uid_to_row_id, f)

            # Fetch the undirected edges from all index doc-link tables,
            # mapped onto the page-profile row ids.
            list_of_edge_tables = self.db.get_tables_in_schema(
                engine_name = engine_name,
                schema_name = search_schema,
                use_regex   = [r'^Index_D_[^_]*_L_[^_]*'],
            )
            list_of_edge_tables = [t for t in list_of_edge_tables if not t.startswith('_')]
            from_ids = array('Q')
            to_ids = array('Q')
            for edge_table_name in tqdm(list_of_edge_tables, desc='Fetch edges', unit='table'):
                edge_uids = self.db.execute_query(
                    engine_name = engine_name,
                    schema_name = search_schema,
                    query       = f"""
                        SELECT LEAST(CONCAT(doc_type, '|', doc_id), CONCAT(link_type, '|', link_id)) AS from_uid,
                               GREATEST(CONCAT(doc_type, '|', doc_id), CONCAT(link_type, '|', link_id)) AS to_uid
                          FROM {search_schema}.{edge_table_name}
                         WHERE row_rank < 99;
                    """,
                    query_id    = 'edtbl',
                )
                for from_uid, to_uid in edge_uids:
                    if from_uid in node_uid_to_row_id and to_uid in node_uid_to_row_id:
                        from_ids.append(node_uid_to_row_id[from_uid])
                        to_ids.append(node_uid_to_row_id[to_uid])
            with open(edges_pickle, 'wb') as f:
                pickle.dump((from_ids, to_ids), f)
        else:
            with open(nodes_pickle, 'rb') as f:
                node_uid_to_row_id = pickle.load(f)
            with open(edges_pickle, 'rb') as f:
                from_ids, to_ids = pickle.load(f)

        # An empty page profile yields an empty node mapping; there is no
        # graph to analyse, so the component stays empty (E2E finding,
        # fresh-schema run with nothing flagged upstream).
        if not node_uid_to_row_id:
            sysmsg.warning("Page profile is empty; skipping largest-component computation.")
            return

        # Union-Find on the edge list: path halving and union by rank.
        max_row_id = max(node_uid_to_row_id.values())
        parent = array('Q', range(max_row_id + 1))
        rank = array('B', [0]) * len(parent)

        # Internal Function: Find the root of a node with path halving.
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        # Internal Function: Union two components by rank.
        def union(x, y):
            x = find(x)
            y = find(y)
            if x == y:
                return
            if rank[x] < rank[y]:
                parent[x] = y
            elif rank[x] > rank[y]:
                parent[y] = x
            else:
                parent[y] = x
                rank[x] += 1

        # Union every edge of the cached list.
        for u, v in zip(from_ids, to_ids):
            union(u, v)

        # Count the component sizes and find the largest component.
        component_sizes = {}
        for node_id in node_uid_to_row_id.values():
            root = find(node_id)
            component_sizes[root] = component_sizes.get(root, 0) + 1
        largest_root, largest_size = max(component_sizes.items(), key=lambda x: x[1])
        print(f"\n[🐬 GraphSearch DB] Found {len(component_sizes)} connected components.")
        print(f"\n[🐬 GraphSearch DB] Largest connected component size: {largest_size}")

        # Build the node tuples of the largest component and write them.
        row_id_to_uid = {row_id: uid for uid, row_id in node_uid_to_row_id.items()}
        largest_component_nodes = [
            tuple(row_id_to_uid[node_id].split('|'))
            for node_id in node_uid_to_row_id.values()
            if find(node_id) == largest_root
        ]
        self._write_largest_component(engine_name, cache_table_path, largest_component_nodes)

    #================================================================#
    # Method Group: Step 4 - index table cleanup                     #
    #================================================================#

    # Internal Method: Clean up the index tables of one schema against the
    # largest connected component, classifying each table by its columns.
    def _cleanup_schema_tables(self, engine_name: str, cache_schema: str, schema_name: str, actions: ActionSet) -> int:
        allowed_nodes_table = 'Operations_N_Object_T_LargestConnectedGraph'
        list_of_tables = self.db.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name, use_regex=False)
        list_of_tables = [t for t in list_of_tables if not t.startswith('_') and "ProcessingTokens" not in t and "Checksums" not in t]
        schema_rows_to_delete = 0
        for table_name in list_of_tables:
            list_of_columns = self.db.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

            # The soft-delete filters apply only when the table carries the column.
            has_deleted_col = 'deleted' in list_of_columns
            deleted_filter_eval = "AND t.deleted = 0" if has_deleted_col else ""
            deleted_filter_commit = "AND deleted = 0" if has_deleted_col else ""

            # Classify the table by its key columns and build the evaluation
            # and commit queries per family; a row of a pair table is a loose
            # end when EITHER endpoint is missing, so the predicate is OR.
            if len({'from_object_type', 'from_object_id', 'to_object_type', 'to_object_id'} & set(list_of_columns)) == 4:
                from_prefix, to_prefix = 'from_object', 'to_object'
                sql_query_eval = f"""
                          SELECT t.from_object_type, t.to_object_type, COUNT(*) AS n_to_delete
                             FROM {schema_name}.{table_name} t
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n_from
                               ON n_from.object_type = t.{from_prefix}_type
                              AND n_from.object_id   = t.{from_prefix}_id
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n_to
                               ON n_to.object_type = t.{to_prefix}_type
                              AND n_to.object_id   = t.{to_prefix}_id
                            WHERE (n_from.object_id IS NULL OR n_to.object_id IS NULL)
                                  {deleted_filter_eval}
                         GROUP BY t.from_object_type, t.to_object_type
                    """
                sql_query_commit = f"""
                        DELETE FROM {schema_name}.{table_name}
                         WHERE (NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n_from
                                WHERE n_from.object_type = {schema_name}.{table_name}.{from_prefix}_type
                                  AND n_from.object_id   = {schema_name}.{table_name}.{from_prefix}_id
                         )
                            OR NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n_to
                                WHERE n_to.object_type = {schema_name}.{table_name}.{to_prefix}_type
                                  AND n_to.object_id   = {schema_name}.{table_name}.{to_prefix}_id
                         ))
                              {deleted_filter_commit}
                    """
            elif len({'doc_type', 'doc_id', 'link_type', 'link_id'} & set(list_of_columns)) == 4:
                from_prefix, to_prefix = 'doc', 'link'
                sql_query_eval = f"""
                          SELECT t.doc_type, t.link_type, COUNT(*) AS n_to_delete
                             FROM {schema_name}.{table_name} t
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n_from
                               ON n_from.object_type = t.{from_prefix}_type
                              AND n_from.object_id   = t.{from_prefix}_id
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n_to
                               ON n_to.object_type = t.{to_prefix}_type
                              AND n_to.object_id   = t.{to_prefix}_id
                            WHERE (n_from.object_id IS NULL OR n_to.object_id IS NULL)
                                  {deleted_filter_eval}
                         GROUP BY t.doc_type, t.link_type
                    """
                sql_query_commit = f"""
                        DELETE FROM {schema_name}.{table_name}
                         WHERE (NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n_from
                                WHERE n_from.object_type = {schema_name}.{table_name}.{from_prefix}_type
                                  AND n_from.object_id   = {schema_name}.{table_name}.{from_prefix}_id
                         )
                            OR NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n_to
                                WHERE n_to.object_type = {schema_name}.{table_name}.{to_prefix}_type
                                  AND n_to.object_id   = {schema_name}.{table_name}.{to_prefix}_id
                         ))
                              {deleted_filter_commit}
                    """
            elif len({'object_type', 'object_id'} & set(list_of_columns)) == 2:
                col_prefix = 'object'
                sql_query_eval = f"""
                          SELECT t.object_type, COUNT(*) AS n_to_delete
                             FROM {schema_name}.{table_name} t
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n
                               ON t.{col_prefix}_type = n.object_type
                              AND t.{col_prefix}_id   = n.object_id
                            WHERE n.object_id IS NULL
                                  {deleted_filter_eval}
                         GROUP BY t.object_type
                    """
                sql_query_commit = f"""
                        DELETE FROM {schema_name}.{table_name}
                         WHERE NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n
                                WHERE n.object_type = {schema_name}.{table_name}.{col_prefix}_type
                                  AND n.object_id   = {schema_name}.{table_name}.{col_prefix}_id
                         )
                              {deleted_filter_commit}
                    """
            elif len({'doc_type', 'doc_id'} & set(list_of_columns)) == 2:
                col_prefix = 'doc'
                sql_query_eval = f"""
                          SELECT t.doc_type, COUNT(*) AS n_to_delete
                             FROM {schema_name}.{table_name} t
                        LEFT JOIN {cache_schema}.{allowed_nodes_table} n
                               ON t.{col_prefix}_type = n.object_type
                              AND t.{col_prefix}_id   = n.object_id
                            WHERE n.object_id IS NULL
                                  {deleted_filter_eval}
                         GROUP BY t.doc_type
                    """
                sql_query_commit = f"""
                        DELETE FROM {schema_name}.{table_name}
                         WHERE NOT EXISTS (
                               SELECT 1 FROM {cache_schema}.{allowed_nodes_table} n
                                WHERE n.object_type = {schema_name}.{table_name}.{col_prefix}_type
                                  AND n.object_id   = {schema_name}.{table_name}.{col_prefix}_id
                         )
                              {deleted_filter_commit}
                    """
            else:
                continue

            # The evaluation counts the loose rows so the commit only runs
            # on tables with rows to remove.
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='DFSHG4tf', verbose='print' in actions)
            table_rows_to_delete = sum(row[-1] for row in out) if out else 0
            schema_rows_to_delete += table_rows_to_delete
            if 'commit' in actions and table_rows_to_delete > 0:
                self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, query_id='s5DfH2Lk', verbose='print' in actions)
        return schema_rows_to_delete

    #================================================================#
    # Method Group: Step 5 - dangling doc-link references            #
    #================================================================#

    # Internal Method: Delete doc-link edges whose endpoints are missing
    # from the corresponding doc index tables of the schema.
    def _delete_dangling_doclink_refs(self, engine_name: str, schema_name: str, actions: ActionSet) -> int:
        doclink_tables = self.db.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name, use_regex=[r'^Index_D_[^_]+_L_[^_]+'])
        doclink_tables = [t for t in doclink_tables if not t.startswith('_')]
        index_tables = set(self.db.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name, use_regex=[r'^Index_D_[^_]*$']))
        schema_rows_to_delete = 0
        for table_name in doclink_tables:
            match = re.search(r'Index_D_([^_]+)_L_([^_]+)', table_name)
            if not match:
                continue
            doc_type, link_type = match.groups()
            doc_index_table = f"Index_D_{doc_type}"
            link_index_table = f"Index_D_{link_type}"

            # Skip pairs whose doc index tables do not exist in the schema.
            if doc_index_table not in index_tables or link_index_table not in index_tables:
                continue
            sql_query_eval = f"""
                SELECT COUNT(*) AS n_to_delete
                  FROM {schema_name}.{table_name} t
             LEFT JOIN {schema_name}.{doc_index_table} d
                    ON d.doc_type = t.doc_type
                   AND d.doc_id   = t.doc_id
             LEFT JOIN {schema_name}.{link_index_table} l
                    ON l.doc_type = t.link_type
                   AND l.doc_id   = t.link_id
                  WHERE d.doc_id IS NULL OR l.doc_id IS NULL
            """
            sql_query_commit = f"""
                DELETE FROM {schema_name}.{table_name}
                 WHERE NOT EXISTS (
                        SELECT 1 FROM {schema_name}.{doc_index_table} d
                         WHERE d.doc_type = {schema_name}.{table_name}.doc_type
                           AND d.doc_id   = {schema_name}.{table_name}.doc_id
                    )
                    OR NOT EXISTS (
                        SELECT 1 FROM {schema_name}.{link_index_table} l
                         WHERE l.doc_type = {schema_name}.{table_name}.link_type
                           AND l.doc_id   = {schema_name}.{table_name}.link_id
                    )
            """

            # The count always runs so the commit can skip no-op deletes.
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='dicidxeval')
            n_rows = out[0][0] if out else 0
            schema_rows_to_delete += n_rows
            if 'commit' in actions and n_rows > 0:
                self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, query_id='dicidxcommit', verbose='print' in actions)
        return schema_rows_to_delete

    #================================================================#
    # Method Group: Final verification                               #
    #================================================================#

    # Internal Method: Verify that no orphaned nodes or edges remain in the
    # index tables, logging critical messages for any findings.
    def _verify_integrity(self, engine_name: str, search_schema: str, es_cache_schema: str) -> None:
        list_of_node_tables = self.db.get_tables_in_schema(engine_name=engine_name, schema_name=search_schema, use_regex=[r'^Index_D_[^_]*$'])
        for schema_name in (search_schema, es_cache_schema):
            list_of_edge_tables = self.db.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name, use_regex=[r'^Index_D_[^_]*_L_[^_]*'])
            for table_name in list_of_node_tables:
                if not self.db.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
                    continue
                n_orphaned_nodes = self.db.execute_query(
                    engine_name = engine_name,
                    schema_name = schema_name,
                    query       = f"""
                          SELECT COUNT(*)
                            FROM {schema_name}.{table_name} t
                       LEFT JOIN {search_schema}.Data_N_Object_T_PageProfile p
                              ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)
                           WHERE p.object_id IS NULL
                    """)[0][0]
                if n_orphaned_nodes > 0:
                    sysmsg.critical(f'Table "{table_name}" has {n_orphaned_nodes} orphaned node(s) with no page profile.')
            for table_name in list_of_edge_tables:
                if not self.db.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
                    continue

                # Orphaned edges: either endpoint without a page profile.
                n_orphaned_edges = sum(
                    self.db.execute_query(
                        engine_name = engine_name,
                        schema_name = schema_name,
                        query       = f"""
                              SELECT COUNT(*)
                                FROM {schema_name}.{table_name} t
                           LEFT JOIN {search_schema}.Data_N_Object_T_PageProfile p
                                  ON (t.{doc_or_link}_type, t.{doc_or_link}_id) = (p.object_type, p.object_id)
                               WHERE p.object_id IS NULL
                        """)[0][0]
                    for doc_or_link in ('doc', 'link')
                )
                if n_orphaned_edges > 0:
                    sysmsg.critical(f'Table "{table_name}" has {n_orphaned_edges} orphaned edge(s) with no page profile.')

                # Edges without a corresponding doc index entry, in both
                # directions; the check skips missing doc tables.
                match = re.findall(r'Index_D_([^_]+)_L_([^_]+)', table_name)
                if not match:
                    continue
                doc_type, link_type = match[0]
                n_with_no_doc_index = 0
                for object_type, id_column in ((doc_type, 'doc'), (link_type, 'link')):
                    doc_table = f"Index_D_{object_type}"
                    if not self.db.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=doc_table):
                        sysmsg.trace(f"Doc table '{schema_name}.{doc_table}' does not exist; skipping doc-index check for '{table_name}'.")
                        continue
                    n_with_no_doc_index += self.db.execute_query(
                        engine_name = engine_name,
                        schema_name = schema_name,
                        query       = f"""
                              SELECT COUNT(*)
                                FROM {schema_name}.{table_name} t
                           LEFT JOIN {schema_name}.{doc_table} d
                                  ON (t.{id_column}_type, t.{id_column}_id) = (d.doc_type, d.doc_id)
                               WHERE d.doc_id IS NULL
                        """)[0][0]
                if n_with_no_doc_index > 0:
                    sysmsg.critical(f'Table "{table_name}" has {n_with_no_doc_index} edge(s) with no corresponding doc index entry.')
