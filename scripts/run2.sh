
# # Step 1: Data ingestion sequence
# graphregistry setup init
# graphregistry data save --node_list examples/sample_sets/sample_epfl_node_list.json
# graphregistry data save --edge_list examples/sample_sets/sample_epfl_edge_list.json
# graphregistry ai detect_concepts

# Step 2: Reset and config airflow
graphregistry airflow reset --options=airflow,traversals,cache
graphregistry airflow config --typeflags config/config_airflow.json
# graphregistry airflow status

# Step 3: Sync new data
# graphregistry airflow sync
# graphregistry airflow update_checksums

# Step 4: Decide what to process
graphregistry airflow expire --older_than=1
graphregistry airflow refresh --limit_per_type=1000
graphregistry airflow status

# Step 5: Knowledge graph generation sequence
graphregistry cache update --formulas=fields,views,traversals,scores --actions=commit
graphregistry cache update --matrix --actions=commit
graphregistry index build --actions=commit,eval
graphregistry index patch --actions=commit,eval,print
# scripts/out_tables_stats.sh

# Step 6: Wrap up processing cycle
graphregistry airflow rollover --actions=commit
graphregistry airflow update_dates --actions=commit
# graphregistry airflow reset --options=traversals,cache

# Step 7: Clean up loose ends in graph cache, search, an elasticsearch indexes
# graphregistry data delete_loose_ends -rg -ul --actions eval,commit

# ♻️ Repeat Steps 4-6 until no more left to process

# # Step 8: ElasticSearch index creation and import
# graphregistry index generate --target=elasticsearch --index_date=2026-08-07 -r -f
# graphregistry es import --env=xaas_coresrv --input_folder=/home/dockerhost/data/es_exports/2026-08-07/es_fullindex_2026-08-07 --rename_to=graphsearch_test_2026_08_07 -r -f --chunk_size 1000

# Assign new index to graphsearch_test alias
# graphes list -a
# graphes index --env xaas_coresrv --index_name graphsearch_test_2026_08_07 --create_alias graphsearch_test

# Copy database to prod
# graphdb copy --from_env xaas_coresrv --to_env xaas_prod --from_schema graphsearch_test --to_schema graphsearch_prod_2026_08_07 --chunk_size 1000000 --compress

# Copy index to prod
# graphes copy --from_env xaas_coresrv --to_env xaas_prod --index_name graphsearch_test_2026_08_07 --rename_to graphsearch_prod_2026_08_07 --chunk_size 1000 -gz

# Assign new index to graphsearch_prod alias
# graphes index --env xaas_prod --index_name graphsearch_prod_2026_08_07 --create_alias graphsearch_prod

