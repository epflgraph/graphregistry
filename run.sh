graphregistry airflow reset --options=typeflags,airflow,cache
graphregistry airflow config --typeflags=@airflow_config.json
graphregistry airflow status
graphregistry airflow expire --doc_type=Unit --older_than=90 --limit_per_type=10
graphregistry airflow refresh --doc_type=Unit -r -v
graphregistry airflow status
graphregistry cache update --formulas=fields,views,traversals,scores --actions=eval,commit
graphregistry cache update --matrix --actions=eval,commit
graphregistry cache build --actions=eval,commit
graphregistry cache patch --actions=eval,commit
graphregistry airflow rollover --actions=eval,commit
graphregistry airflow update_dates --actions=eval,commit
graphregistry airflow reset --options=typeflags,airflow
