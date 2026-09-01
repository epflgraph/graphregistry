graphregistry airflow reset        --options airflow,traversals,cache
graphregistry airflow config       --typeflags config/config_airflow.json
graphregistry airflow expire       --types Lecture
graphregistry airflow refresh      --limit_per_type 1000000
graphregistry airflow status
graphregistry airflow propagate    --actions commit
graphregistry index   patch        --actions commit
graphregistry airflow rollover     --actions commit
graphregistry airflow update_dates --actions commit
