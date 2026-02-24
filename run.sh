
# MySQL data update
( ulimit -v $((4*1024*1024)) ; graphregistry airflow reset --options=typeflags,airflow,cache )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow config --typeflags=@airflow_config.json )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow status )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow update_checksums -v )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow expire --older_than=90 --limit_per_type=10 )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow refresh )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow status )
( ulimit -v $((4*1024*1024)) ; graphregistry cache update --formulas=fields,views,traversals,scores --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry cache update --matrix --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry index build --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry index patch --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow rollover --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow update_dates --actions=eval,commit )
( ulimit -v $((4*1024*1024)) ; graphregistry airflow reset --options=typeflags,airflow )

# ElasticSearch data update
( ulimit -v $((4*1024*1024)) ; graphregistry index generate --target=elasticsearch --index_date=2026-02-19 -ifo -r )
( ulimit -v $((4*1024*1024)) ; graphregistry es import --env=xaas_coresrv --input_folder=/home/dockerhost/data/es_exports/2026-02-19/es_fullindex_2026-02-19 --rename_to=graphsearch_test_2026_02_19 -r )
