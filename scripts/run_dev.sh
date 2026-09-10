
# Step 0: Initialisation
graphregistry setup init

# Step 1(a): Data ingestion sequence (CLI method)
# graphregistry data save --node_list examples/sample_sets/sample_epfl_node_list.json
# graphregistry data save --edge_list examples/sample_sets/sample_epfl_edge_list.json

# Step 1(b): Data ingestion sequence (API method)
jq '.' examples/sample_sets/sample_epfl_node_list.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/nodes/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'

jq '.' examples/sample_sets/sample_epfl_edge_list.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/edges/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'

# Step 2: Sync new data
graphregistry airflow sync --include_ontology

# Step 3: Reset and config airflow
graphregistry airflow reset --options typeflags,airflow,traversals,cache
graphregistry airflow config --typeflags config/config_airflow.json
graphregistry airflow status

# Step 4: AI/LLM operations
graphregistry ai detect_concepts

# Step 5: Decide what to process
graphregistry airflow update_checksums
# graphregistry airflow expire
graphregistry airflow refresh --limit_per_type 10000
graphregistry airflow status

# Step 6: Knowledge graph generation sequence
graphregistry cache update --formulas reset,fields,views,traversals,scores --actions commit,eval
graphregistry cache update --matrix --actions commit
graphregistry index build --actions commit,eval
graphregistry index patch --actions commit,eval
scripts/out_tables_stats_dev.sh

# Step 7: Wrap up processing cycle
graphregistry airflow rollover --actions commit
graphregistry airflow update_dates --actions commit
graphregistry airflow reset --options airflow,traversals,cache

# Step 8: Clean up loose ends in graph cache, search, an elasticsearch indexes
graphregistry data delete_loose_ends --env xaas_coresrv --actions eval,commit

# # Step 9: ElasticSearch index creation and import
graphregistry index generate --target elasticsearch --index_date 9999-99-99 -r -f
graphregistry es import --env xaas_coresrv --input_folder /home/dockerhost/data/es_exports/9999-99-99/es_fullindex_9999-99-99 --rename_to graphsearch_dev -r -f --chunk_size 1000
