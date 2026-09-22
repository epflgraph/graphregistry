
graphregistry airflow sync
graphregistry airflow config config/application/config_airflow.json

# Loop this:
# ------------------------------------------
graphregistry airflow reset --options airflow,traversals,cache
# need to add --checksums-registry --checksums-ontology --checksums-lectures --exclude-ontology
graphregistry airflow expire --types Exercise,Notebook --scores
graphregistry airflow plan -l 1000 -shr
graphregistry airflow status

graphregistry ai detect-concepts

graphregistry kgraph compute
graphregistry kgraph patch

graphregistry airflow rollover

# ------------------------------------------

graphregistry kgraph prune
scripts/out_tables_stats_prod.sh

graphregistry kgraph index -n graphsearch_e2e_test -r -f
