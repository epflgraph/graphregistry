#!/usr/bin/env bash
# Sequential CLI pipeline executed by test_full_pipeline.py. Any command that
# exits nonzero must abort the sequence immediately; without this, a crashed
# step (e.g. the prune) sails through to the export and the comparison runs
# against garbage with a zero exit code.
set -euo pipefail

# Step 1: Initialise database tables and sample imports
graphregistry init --import-ontology-sample --import-concepts-sample

# Step 2: Load sample node and edge lists into registry
graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_node_list.json
graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_edge_list.json

# Step 3: Sync new data, configure airflow, and generate execution plan
graphregistry airflow sync --include-ontology
graphregistry airflow config tests/end2end_tests/config/config_airflow.json
graphregistry airflow plan -c -l 10000
graphregistry airflow status

# Step 4: Compute and generate/patch knowledge graph for GraphSearch
graphregistry kgraph compute
graphregistry kgraph patch

# Step 6: Wrap up processing cycle
graphregistry airflow rollover

# Step 7: Prune orphan nodes and loose ends from final graph
graphregistry kgraph prune

# Sanity check: Display number of rows in GraphSearch tables
/home/dockerhost/dev/graphregistry/tests/end2end_tests/helpers/out_tables_stats.sh

# Step 8: Export GraphSearch data from MySQL and import into ElasticSearch as index
graphregistry kgraph index -n graphsearch_e2e_test -r -f

# Test results 1: Export all MySQL tables for comparison with ground truth
rm -rf /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql
mkdir -p /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_registry
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_airflow
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_cache
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_traversals
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graphsearch_test
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_elasticsearch_cache

# Test results 2: Export ElasticSearch index for comparison with ground truth
rm -rf /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json
mkdir -p /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json
graphes export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json --chunk_size 1000 --index_name graphsearch_e2e_test
