python - <<'PY'
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig

db = GraphDB()
glbcfg = GlobalConfig()

db_env = "xaas_coresrv"

schema_name = '_1_DEV_' + glbcfg.settings['mysql']['db_schema_names']['graphsearch_test']

db.print_database_stats(
    engine_name=db_env,
    schema_name=schema_name,
    re_exclude=[r'.*(MOOC|Lecture|Widget|Startup|Specialisation|StudyPlan|Notebook|Exercise).*']
)
PY