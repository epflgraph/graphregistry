#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphRegistry
from loguru import logger as sysmsg
import rich, json

# Initialize the GraphRegistry instance
gr = GraphRegistry()
print('\n')

# gr.indexes.idx.index_list(engine_name='test')
# gr.indexes.idx.drop_index(engine_name='test', index_name='graphsearch_test_2025_09_20')
# gr.indexes.idx.index_list(engine_name='test')
# exit()

# Open JSON sample set
with open('database/init/sample_sets/synthetic_ML_sample_set.json', 'r') as fp:
    sample_set = json.load(fp)

# Print sample set content
if True:
    print('Input JSON sample set:')
    rich.print_json(data=sample_set)

#=============================================#
# Step 1: Import data from JSON into Registry #
#=============================================#

# Detect/update concepts? [disable for faster imports]
detect_concepts = False

# Choose import method (object or list)
import_method = 'object'

# Execute step?
if True:

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
# Step 2: Sync new data into Airflow and set up flags #
#=====================================================#

# Execute step?
if True:
    
    # Sync new objects from Registry with Airflow
    gr.orchestrator.sync()

    # Config type flags to process everything
    gr.orchestrator.typeflags.config(config_json={
        'nodes': [
            ['Category'   , True, True],
            ['Concept'    , True, True],
            ['Course'     , True, True],
            ['Person'     , True, True],
            ['Publication', True, True],
            ['Startup'    , True, True],
            ['Unit'       , True, True]
        ],
        'edges': [
            ['Category'   , 'Category', True, True],
            ['Concept'    , 'Category', True, True],
            ['Course'     , 'Person'  , True, True],
            ['Person'     , 'Unit'    , True, True],
            ['Publication', 'Person'  , True, True],
            ['Startup'    , 'Person'  , True, True],
            ['Unit'       , 'Unit'    , True, True]
        ]
    })

    # Display orchestration status
    gr.orchestrator.status()

#===================================#
# Step 3: Execute all major actions #
#===================================#

# Execute step?
if True:
    gr.cachemanager.apply_calculated_field_formulas(verbose=False)
    gr.cachemanager.materialize_views(actions=('commit'))
    gr.cachemanager.apply_traversal_and_scoring_formulas(verbose=False)
    gr.cachemanager.update_scores(actions=('commit'))
    gr.indexdb.build(actions=('commit'))
    gr.indexdb.patch(actions=('commit'))
    gr.db.print_database_stats(engine_name='test', schema_name='test_graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    gr.db.print_database_stats(engine_name='test', schema_name='test_elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])

#======================================#
# Step 4: Generate ElasticSearch index #
#======================================#

# Choose index date (YYYY-MM-DD)
index_date = '2025-09-20'

# Execute step?
if True:
    gr.indexes.generate_local_cache(index_date=index_date)
    gr.indexes.generate_index_from_local_cache(index_date=index_date)
    gr.indexes.import_index(engine_name='test', index_date=index_date, delete_if_exists=False)
    gr.indexes.idx.info(engine_name='test')
    gr.indexes.idx.index_list(engine_name='test')
    gr.indexes.idx.alias_list(engine_name='test')