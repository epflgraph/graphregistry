python - <<'PY'
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig

db = GraphDB()
glbcfg = GlobalConfig()

db_env = "coresrv"

schema_name = glbcfg.settings['database']['schema_names']['graphsearch_test']

db.print_database_stats(
    engine_name = db_env,
    schema_name = schema_name
)
PY