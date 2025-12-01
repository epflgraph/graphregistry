
from graphregistry.common.config import IndexConfig
from graphregistry.clients.mysql import GraphDB

idxcfg = IndexConfig()

db = GraphDB()

def cleanup_docs_table_gs(doc_type, actions):
    db.delete_orphaned_rows(
        engine_name = 'test',
        upd_schema  = 'graphsearch_test',
        upd_table   = f'Index_D_{doc_type}',
        upd_key     = ('doc_institution', 'doc_type', 'doc_id'),
        ref_schema  = 'graphsearch_test',
        ref_table   = 'Data_N_Object_T_PageProfile',
        ref_key     = ('institution_id', 'object_type', 'object_id'),
        ref_where   = f"r.object_type = '{doc_type}'",
        actions     = actions
    )

def cleanup_docs_table_es(doc_type, actions):
    db.delete_orphaned_rows(
        engine_name = 'test',
        upd_schema  = 'elasticsearch_cache',
        upd_table   = f'Index_D_{doc_type}',
        upd_key     = ('doc_type', 'doc_id'),
        ref_schema  = 'graphsearch_test',
        ref_table   = 'Data_N_Object_T_PageProfile',
        ref_key     = ('object_type', 'object_id'),
        ref_where   = f"r.object_type = '{doc_type}'",
        actions     = actions
    )

def cleanup_doclinks_table_gs(doc_type, link_type, table_type, actions):
    for upd_key, object_type in [
        [( 'doc_institution',  'doc_type',  'doc_id'),  doc_type],
        [('link_institution', 'link_type', 'link_id'), link_type]
    ]:
        db.delete_orphaned_rows(
            engine_name = 'test',
            upd_schema  = 'graphsearch_test',
            upd_table   = f'Index_D_{doc_type}_L_{link_type}_T_{table_type}',
            upd_key     = upd_key,
            ref_schema  = 'graphsearch_test',
            ref_table   = 'Data_N_Object_T_PageProfile',
            ref_key     = ('institution_id', 'object_type', 'object_id'),
            ref_where   = f"r.object_type = '{object_type}'",
            actions     = actions
        )

def cleanup_doclinks_table_es(doc_type, link_type, actions):
    for upd_key, object_type in [
        [( 'doc_type',  'doc_id'),  doc_type],
        [('link_type', 'link_id'), link_type]
    ]:
        db.delete_orphaned_rows(
            engine_name = 'test',
            upd_schema  = 'elasticsearch_cache',
            upd_table   = f'Index_D_{doc_type}_L_{link_type}',
            upd_key     = upd_key,
            ref_schema  = 'graphsearch_test',
            ref_table   = 'Data_N_Object_T_PageProfile',
            ref_key     = ('object_type', 'object_id'),
            ref_where   = f"r.object_type = '{object_type}'",
            actions     = actions
        )


# -- Execute this query to remove poorly connected concepts ( before running the script ) --
#     DELETE u
#       FROM graphsearch_test.Data_N_Object_T_PageProfile u
#  LEFT JOIN graph_cache.Nodes_N_Object_T_DegreeScores d
#      USING (institution_id, object_type, object_id)
#      WHERE u.object_type = 'Concept'
#        AND (d.avg_norm_log_degree < 0.1 OR d.avg_norm_log_degree IS NULL);

actions = ('print')

list_of_types = idxcfg.settings['doc_types']

for doc_type in list_of_types:
    cleanup_docs_table_gs(doc_type, actions)
    cleanup_docs_table_es(doc_type, actions)
    for link_type in list_of_types:
        for table_type in ['SEM', 'ORG']:
            cleanup_doclinks_table_gs(doc_type, link_type, table_type, actions)
        cleanup_doclinks_table_es(doc_type, link_type, actions)
cleanup_doclinks_table_gs('Lecture', 'Concept', 'ORG_Search', actions)