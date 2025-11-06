#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.clients.mysql import GraphDB
from graphregistry.clients.elasticsearch import GraphES
from graphregistry.core.registry import GraphRegistry
from graphregistry.common.config import GlobalConfig
from loguru import logger as sysmsg
import rich, json

# Initialize global config
glbcfg = GlobalConfig()

# Initialize the GraphDB instance
db = GraphDB()

# Initialize the ElasticSearch instance
es = GraphES()

# Initialize the GraphRegistry instance
gr = GraphRegistry()
print('\n')

#=============================================#
# Step 1: Import data from JSON into Registry #
#=============================================#

# Detect/update concepts? [disable for faster imports]
detect_concepts = True

# Choose import method (object or list)
import_method = 'object'

# Execute step?
if False:

    # Method 1: Process and commit object by object
    if import_method == 'object':

        # Process nodes
        for node_json in sample_set['nodes']:
            node = gr.Node()
            node.set_from_json(doc_json=node_json, detect_concepts=detect_concepts)
            node.commit(actions=('commit'))

        # Process edges
        for edge_json in sample_set['edges']:
            edge = gr.Edge()
            edge.set_from_json(doc_json=edge_json)
            edge.commit(actions=('commit'))

    # Method 2: Process and commit as list of objects
    elif import_method == 'list':

        # Process nodes list
        node_list = gr.NodeList()
        node_list.set_from_json(doc_json_list=sample_set['nodes'], detect_concepts=detect_concepts)
        node_list.commit(actions=('eval', 'commit'))

        # Process edges list
        edge_list = gr.EdgeList()
        edge_list.set_from_json(doc_json_list=sample_set['edges'])
        edge_list.commit(actions=('eval', 'commit'))

#=====================================================#
# Step 3: Sync new data into Airflow and set up flags #
#=====================================================#

# Execute step?
if False:

    # Sync new objects from Registry with Airflow
    # gr.orchestrator.sync()

    # Config type flags to process everything
    gr.orchestrator.typeflags.config(config_json={
        'nodes': [
            ['Category'   , False, False],
            ['Concept'    , False, False],
            ['Course'     , False, False],
            ['Person'     , False, False],
            ['Publication', False, False],
            ['Startup'    , False, False],
            ['Unit'       , False, False],
            ['Widget'     , True , False]
        ],
        'edges': [
            ['Category'   , 'Category', False],
            ['Concept'    , 'Category', False],
            ['Course'     , 'Person'  , False],
            ['Lecture'    , 'Widget'  , True ],
            ['Person'     , 'Unit'    , False],
            ['Publication', 'Person'  , False],
            ['Startup'    , 'Person'  , False],
            ['Unit'       , 'Unit'    , False]
        ]
    })

    # Display orchestration status
    # gr.orchestrator.status()
    gr.orchestrator.typeflags.status()

# Print config JSON
rich.print_json(data=gr.orchestrator.typeflags.get_config_json())

#===================================#
# Step 4: Execute all major actions #
#===================================#

# Execute step?
if False:
    gr.orchestrator.typeflags.status()
    # gr.cachemanager.apply_calculated_field_formulas(verbose=False)
    # gr.cachemanager.materialize_views(actions=('commit'))
    # gr.cachemanager.apply_traversal_and_scoring_formulas(verbose=False)
    # gr.cachemanager.update_scores(score_thr=0.1, actions=('eval', 'commit'))
    gr.indexdb.build(actions=('print', 'eval', 'commit'))
    # gr.indexdb.patch(actions=('eval', 'commit'))
    # gr.db.print_database_stats(engine_name='test', schema_name='graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    # gr.db.print_database_stats(engine_name='test', schema_name='elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])

#======================================#
# Step 5: Generate ElasticSearch index #
#======================================#

# Execute step?
if False:

    # Fetch index parameters from config
    index_date = str(glbcfg.settings['elasticsearch']['index_date'])
    index_file = glbcfg.settings['elasticsearch']['index_file']
    index_name = glbcfg.settings['elasticsearch']['index_names']['graphsearch_test']

    # Execute step?
    if False:

        # Generate local ES cache from MySQL
        gr.indexes.generate_local_cache(index_date=index_date, ignore_warnings=True, replace_existing=True, force_replace=True)

        # Generate ES index file from local cache
        gr.indexes.generate_index_from_local_cache(index_date=index_date, ignore_warnings=True, replace_existing=True, force_replace=True)

    #-------------------------------------------------------------#
    # Two methods to import index file into ElasticSearch engine: #
    #-------------------------------------------------------------#

    # Execute step?
    if False:

        # With index date (index name generated automatically)
        print(f"\nMETHOD 1: Importing index date '{index_date}' into ElasticSearch engine...\n")
        gr.indexes.import_index(engine_name='test', index_date=index_date, replace_existing=True, force_replace=True)

        # With explicit index file and name
        # print(f"\nMETHOD 2: Importing index file '{index_file}' as index name '{index_name}' into ElasticSearch engine...\n")
        # gr.indexes.import_index(engine_name='test', index_file=index_file, index_name=index_name, replace_existing=True, force_replace=True)

    #-------------------------------------------------------------#

    # List indexes and aliases in ElasticSearch engine
    es.index_list(engine_name='test', display_size=True)
    es.alias_list(engine_name='test')

    # Set alias to new generated index
    # es.set_alias(engine_name='test', alias_name='graphsearch_test', index_name='graphsearch_test_2025_11_05')
    # es.alias_list(engine_name='test')

    # Direct link to Kibana
    print("\nList of indexes in Kibana:")
    print("- Local ....... http://localhost:5601/app/enterprise_search/content/search_indices/")
    print("- Test ........ http://127.0.0.1:35601/app/enterprise_search/content/search_indices/\n")
    print("- Prod ........ http://127.0.0.1:45601/app/enterprise_search/content/search_indices/\n")




if False:

    db.copy_database_across_engines(
        source_engine_name = 'test',
        source_schema_name = 'graphsearch_test',
        target_engine_name = 'prod',
        target_schema_name = 'graphsearch_prod_2025_11_05'
    )

if False:

    es.copy_index_across_engines(
        source_engine_name = 'test',
        target_engine_name = 'prod',
        index_name         = 'graphsearch_test_2025_11_05',
        rename_to          = 'graphsearch_prod_2025_11_05'
    )


# es.index_list(engine_name='test', display_size=True)
# es.index_list(engine_name='prod', display_size=True)

if True:

    list_of_tables = db.get_tables_in_schema(
        engine_name = 'test',
        schema_name = 'graphsearch_test'
    )

    start = False

    for table_name in list_of_tables:

        print(f"\nComparing table '{table_name}' between TEST and PROD engines:\n")

        if table_name=='Index_D_Lecture_L_MOOC_T_ORG':
            start = True

        if not start:
            continue

        db.compare_tables_by_random_sampling(
            source_engine_name = 'test',
            source_schema_name = 'graphsearch_test',
            source_table_name  = table_name,
            target_engine_name = 'prod',
            target_schema_name = 'graphsearch_prod_2025_11_05',
            target_table_name  = table_name,
            sample_size = 10000
        )