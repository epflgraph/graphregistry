#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphRegistry, global_config
from loguru import logger as sysmsg
import rich, json

# Initialize the GraphRegistry instance
gr = GraphRegistry()
print('\n')

#=====================================================#
# Step 1: Sync new data into Airflow and set up flags #
#=====================================================#

# Execute step?
if False:
    
    # Sync new objects from Registry with Airflow
    # gr.orchestrator.sync()

    # Config type flags to process everything TODO: why is this so slow?
    gr.orchestrator.typeflags.config(config_json={
        'nodes': [
            ['Category'   , False, False],
            ['Concept'    , False, False],
            ['Course'     , False, False],
            ['Person'     , False, False],
            ['Publication', False, False],
            ['Startup'    , False, False],
            ['Unit'       , False, False]
        ],
        'edges': [
            ['Category'   , 'Category', False, False],
            ['Concept'    , 'Category', False, False],
            ['Course'     , 'Person'  , False, False],
            ['Person'     , 'Unit'    , False, False],
            ['Publication', 'Person'  , False, False],
            ['Startup'    , 'Person'  , False, False],
            ['Unit'       , 'Unit'    , False, False]
        ]
    })

    # Display orchestration status
    gr.orchestrator.status()

#===================================#
# Step 2: Execute all major actions #
#===================================#

# Execute step?
if True:
    # gr.cachemanager.apply_calculated_field_formulas(verbose=False)
    # gr.cachemanager.materialize_views(actions=('eval', 'commit'))
    gr.cachemanager.apply_traversal_and_scoring_formulas(verbose=False)
    # gr.cachemanager.update_scores(score_thr=0.01, actions=('commit'))
    # gr.indexdb.build(actions=('eval', 'commit'))
    # gr.indexdb.patch(actions=('eval', 'commit'))
    # gr.db.print_database_stats(engine_name='test', schema_name='test_graphsearch_test'   , re_exclude=[r'.*(MOOC|Lecture|Widget).*'])
    # gr.db.print_database_stats(engine_name='test', schema_name='test_elasticsearch_cache', re_exclude=[r'.*(MOOC|Lecture|Widget).*'])

#======================================#
# Step 3: Generate ElasticSearch index #
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