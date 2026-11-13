
graphregistry airflow sync
graphregistry airflow config config/application/config_airflow.json

# ------------------------------------------
graphregistry airflow reset --options airflow,traversals,cache
# need to add --checksums-registry --checksums-ontology --checksums-lectures --exclude-ontology
graphregistry airflow expire --types Person --scores

# Loop this:
# ------------------------------------------
graphregistry airflow plan -l 2000 -shr
graphregistry airflow status

graphregistry ai detect-concepts

graphregistry kgraph compute
graphregistry kgraph patch

graphregistry airflow rollover



# ------------------------------------------

graphregistry kgraph prune
scripts/out_tables_stats_prod.sh

graphregistry kgraph index -n graphsearch_test_2026_10_14 -r -f
