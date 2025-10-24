#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphRegistry, global_config
from loguru import logger as sysmsg
import rich, json

# Initialize the GraphRegistry instance
gr = GraphRegistry()
# print('\n')
# exit()
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
            ['Publication', True , True ],
            ['Startup'    , False, False],
            ['Unit'       , False, False]
        ],
        'edges': [
            ['Category'   , 'Category', False],
            ['Concept'    , 'Category', False],
            ['Course'     , 'Person'  , False],
            ['Person'     , 'Unit'    , False],
            ['Publication', 'Person'  , True ],
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
if True:
    # gr.cachemanager.apply_calculated_field_formulas(verbose=False)
    # gr.cachemanager.materialize_views(actions=('commit'))
    # gr.cachemanager.apply_traversal_and_scoring_formulas(verbose=False)
    gr.cachemanager.update_scores(score_thr=0.1, actions=('eval', 'commit'))
    # gr.indexdb.build(actions=('print', 'eval', 'commit'))
    # gr.indexdb.patch(actions=('eval', 'commit'))
    # gr.db.print_database_stats(engine_name='test', schema_name='graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    # gr.db.print_database_stats(engine_name='test', schema_name='elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])

#======================================#
# Step 5: Generate ElasticSearch index #
#======================================#

# Execute step?
if False:

    # Fetch index parameters from config
    index_date = str(global_config['elasticsearch']['index_date'])
    index_file = global_config['elasticsearch']['index_file']
    index_name = global_config['elasticsearch']['index_names']['graphsearch_test']

    # Generate local ES cache from MySQL
    gr.indexes.generate_local_cache(index_date=index_date, ignore_warnings=True, replace_existing=True, force_replace=True)

    # Generate ES index file from local cache
    gr.indexes.generate_index_from_local_cache(index_date=index_date, ignore_warnings=True, replace_existing=True, force_replace=True)

    #-------------------------------------------------------------#
    # Two methods to import index file into ElasticSearch engine: #
    #-------------------------------------------------------------#

    # With index date (index name generated automatically)
    print(f"\nMETHOD 1: Importing index date '{index_date}' into ElasticSearch engine...\n")
    gr.indexes.import_index(engine_name='test', index_date=index_date, replace_existing=True, force_replace=True)

    # With explicit index file and name
    # print(f"\nMETHOD 2: Importing index file '{index_file}' as index name '{index_name}' into ElasticSearch engine...\n")
    # gr.indexes.import_index(engine_name='test', index_file=index_file, index_name=index_name, replace_existing=True, force_replace=True)

    #-------------------------------------------------------------#

    # List indexes and aliases in ElasticSearch engine
    gr.indexes.idx.index_list(engine_name='test')
    gr.indexes.idx.alias_list(engine_name='test')

    # Direct link to Kibana
    print(f"\nList of indexes in Kibana:\n - http://localhost:5601/app/enterprise_search/content/search_indices/\n")