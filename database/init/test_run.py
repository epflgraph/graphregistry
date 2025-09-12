#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphRegistry
from loguru import logger as sysmsg
import rich, json

# Initialize the GraphRegistry instance
gr = GraphRegistry()

# Open JSON sample set
with open('database/init/sample_sets/synthetic_ML_sample_set.json', 'r') as fp:
    sample_set = json.load(fp)

# Print sample set content
if False:
    rich.print_json(data=sample_set)

#=============================================#
# Step 1: Import data from JSON into Registry #
#=============================================#

# Execute step?
if False:

    # Choose import method (object or list)
    import_method = 'object'

    # Method 1: Process and commit object by object
    if import_method == 'object':

        # Process nodes
        for node_json in sample_set['nodes']:
            node = gr.Node()
            node.set_from_json(doc_json=node_json, detect_concepts=False)
            node.commit(actions=('eval', 'commit'))
        
        # Process edges
        for edge_json in sample_set['edges']:
            edge = gr.Edge()
            edge.set_from_json(doc_json=edge_json)
            edge.commit(actions=('eval', 'commit'))

    # Method 2: Process and commit as list of objects
    elif import_method == 'list':

        # Process nodes list
        node_list = gr.NodeList()
        node_list.set_from_json(doc_json_list=sample_set['nodes'], detect_concepts=True)
        node_list.commit(actions=('eval', 'commit'))

        # Process edges list
        edge_list = gr.EdgeList()
        edge_list.set_from_json(doc_json_list=sample_set['edges'])
        edge_list.commit(actions=('eval', 'commit'))

#=====================================================#
# Step 2: Sync new data into Airflow and set up flags #
#=====================================================#

# Execute step?
if False:
    
    # Sync new objects from Registry with Airflow
    gr.orchestrator.sync()

    # Config type flags to process everything
    gr.orchestrator.typeflags.config(config_json={
        'nodes': [
            ['Course'     , True, True],
            ['Person'     , True, True],
            ['Publication', True, True],
            ['Startup'    , True, True],
            ['Unit'       , True, True]
        ],
        'edges': [
            ['Course'     , 'Person', True, True],
            ['Person'     , 'Unit'  , True, True],
            ['Publication', 'Person', True, True],
            ['Startup'    , 'Person', True, True],
            ['Unit'       , 'Unit'  , True, True]
        ]
    })

    # Display orchestration status
    gr.orchestrator.status()

#=====================================================#
# Step 3: ... #
#=====================================================#

if True:
    # gr.cachemanager.apply_views(actions=('eval', 'commit'))
    # gr.cachemanager.apply_formulas(verbose=True)
    gr.indexdb.build(actions=('eval', 'commit'))
    gr.indexdb.patch(actions=('eval', 'commit'))