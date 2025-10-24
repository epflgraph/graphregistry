#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphRegistry
from api.core.config import GlobalConfig
from loguru import logger as sysmsg
import rich, json

# Initialize global config
glbcfg = GlobalConfig()

# Initialize the GraphRegistry instance
gr = GraphRegistry()
print('\n')

# Open JSON sample set
with open('database/init/sample_sets/synthetic_ML_sample_set.json', 'r') as fp:
    sample_set = json.load(fp)

# Print sample set content
if False:
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

#===============================================#
# Step 2: Generate/update Ontology-Light subset #
#===============================================#

# Execute step?
if False:

    # Fetch light ontology schema name from config
    ontology_light_schema_name = global_config['mysql']['db_schema_names']['ontology_light']

    # Load database/init/formulas/formula.ontology_light.sql from file
    with open('database/init/formulas/formula.ontology_light.sql', 'r') as fp:
        sql_query = fp.read()

    # Replace placeholders in SQL query
    for placeholder, value in global_config['mysql']['db_schema_names'].items():
        sql_query = sql_query.replace(f'[[{placeholder}]]', value)

    # Execute SQL query in shell
    gr.db.execute_query_in_shell(engine_name='test', query=sql_query, verbose=True)

#=====================================================#
# Step 3: Sync new data into Airflow and set up flags #
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
            ['Category'   , 'Category', True],
            ['Concept'    , 'Category', True],
            ['Course'     , 'Person'  , True],
            ['Person'     , 'Unit'    , True],
            ['Publication', 'Person'  , True],
            ['Startup'    , 'Person'  , True],
            ['Unit'       , 'Unit'    , True]
        ]
    })

    # Display orchestration status
    gr.orchestrator.status()

#===================================#
# Step 4: Execute all major actions #
#===================================#

# Execute step?
if True:
    gr.cachemanager.apply_calculated_field_formulas(verbose=False)
    gr.cachemanager.materialize_views(actions=('commit'))
    gr.cachemanager.apply_traversal_and_scoring_formulas(verbose=False)
    gr.cachemanager.update_scores(score_thr=0.01, actions=('commit'))
    gr.indexdb.build(actions=('commit'))
    gr.indexdb.patch(actions=('commit'))
    gr.db.print_database_stats(engine_name='test', schema_name='test_graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    gr.db.print_database_stats(engine_name='test', schema_name='test_elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])

#======================================#
# Step 5: Generate ElasticSearch index #
#======================================#

# Execute step?
if True:

    # Fetch index parameters from config
    index_date = str(glbcfg.settings['elasticsearch']['index_date'])
    index_file = glbcfg.settings['elasticsearch']['index_file']
    index_name = glbcfg.settings['elasticsearch']['index_names']['graphsearch_test']

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