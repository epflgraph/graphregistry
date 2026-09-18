
graphregistry init --import-ontology-sample

graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_node_list.json
graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_edge_list.json

graphregistry airflow sync --include-ontology
graphregistry airflow config tests/end2end_tests/config/config_airflow.json

# Loop this:
# ------------------------------------------
graphregistry airflow plan -c -l 10000
graphregistry airflow status

graphregistry ai detect-concepts

graphregistry kgraph compute
graphregistry kgraph patch

graphregistry airflow rollover

# ------------------------------------------

graphregistry kgraph prune
scripts/out_tables_stats_dev.sh

graphregistry kgraph index -n graphsearch_e2e_test -r -f

rm -rf /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql
mkdir -p /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_registry
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_airflow
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_cache
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graph_traversals
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_graphsearch_test
graphdb export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/sql -d --schema_name _1_DEV_elasticsearch_cache

rm -rf /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json
mkdir -p /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json
graphes export --output_folder /home/dockerhost/dev/graphregistry/tests/end2end_tests/data/test_output/json --chunk_size 1000 --index_name graphsearch_e2e_test
