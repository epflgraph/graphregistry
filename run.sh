graphregistry airflow reset --options=typeflags,airflow,cache
graphregistry airflow config --typeflags=@airflow_config.json
graphregistry airflow status
graphregistry airflow expire --older_than=90 --limit_per_type=10
graphregistry airflow refresh -r -v
graphregistry airflow status
graphregistry cache update --formulas=fields,views,traversals,scores --actions=eval,commit
graphregistry cache update --matrix --actions=eval,commit
graphregistry cache build --actions=eval,commit
graphregistry cache patch --actions=eval,commit

# registry.orchestrator.rollover(verbose=True)

