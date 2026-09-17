

graphregistry init --import-ontology-sample

graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_node_list.json
graphregistry data save database/init/sample_sets/graph_registry/json/sample_epfl_edge_list.json

graphregistry airflow sync --include-ontology
graphregistry airflow config config/application/config_airflow.json


# Loop this:
# ------------------------------------------
graphregistry airflow plan -c -l 1000
graphregistry airflow status

graphregistry ai detect-concepts

graphregistry kgraph compute
graphregistry kgraph patch

graphregistry airflow rollover
scripts/out_tables_stats_dev.sh
# ------------------------------------------

graphregistry kgraph prune
