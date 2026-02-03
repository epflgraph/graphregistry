from typing import Any, Dict

from graphregistry.cli.cmd_ai import (
    cmd_ai_test,
)
from graphregistry.cli.cmd_airflow import (
    cmd_airflow_sync,
    cmd_airflow_status,
    cmd_airflow_to_process,
    cmd_airflow_config,
    cmd_airflow_reset,
    cmd_airflow_expire,
    cmd_airflow_refresh,
)
from graphregistry.cli.cmd_cache import (
    cmd_cache_update,
    cmd_cache_build,
    cmd_cache_patch,
    cmd_cache_to_process,
    cmd_cache_debug,
)
from graphregistry.cli.cmd_config import (
    cmd_config_index,
)
from graphregistry.cli.cmd_db import (
    cmd_db_test,
    cmd_db_export,
    cmd_db_import,
    cmd_db_copy,
    cmd_db_compare
)
from graphregistry.cli.cmd_index import (
    cmd_index_test,
    cmd_index_info,
    cmd_index_health,
    cmd_index_list,
    cmd_index_copy,
    cmd_index_export,
    cmd_index_import,
)

# Global common arguments
global_common_args = {
    'env' : dict(
        flags = ('--env',),
        kwargs = dict(
            help = "Specify environment (default=test).",
            choices = ('test', 'prod', 'xaas_prod', 'xaas_coresrv'),
            # default = 'test'
        )
    )
}

#===================================================#
# CLI Definitions for all Subcommands and Arguments #
#===================================================#
cli_definitions: Dict[str, Any] = {

    #---------------------#
    # Domain: ai          #
    #---------------------#
    'ai' : dict(
        help = "Test GraphAI service.",
        common_args = dict(),
        commands = {
            'test' : dict(
                help = "Test GraphAI service.",
                func = cmd_ai_test,
                args = [],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Domain: airflow     #
    #---------------------#
    'airflow' : dict(
        help = "Synchronize Registry with Airflow and manage type-flag configurations.",
        common_args = dict(),
        commands = {
            'sync' : dict(
                help = "Sync registry with Airflow",
                func = cmd_airflow_sync,
                args = [],
                common_args = []
            ),
            'status' : dict(
                help = "Get status from Airflow",
                func = cmd_airflow_status,
                args = [],
                common_args = []
            ),
            'to_process' : dict(
                help = "Operations on the 'to process' queue for airflow jobs.",
                func = cmd_airflow_to_process,
                args = [
                    dict(flags=('--count', '-c'), kwargs=dict(action='store_true', help="Show number of items waiting to be processed.")),
                    dict(flags=('--reset', '-r'), kwargs=dict(action='store_true', help="Reset the 'to process' queue / counters."))
                ],
                common_args = []
            ),
            'config' : dict(
                help = "Configure Airflow typeflags for orchestration.",
                func = cmd_airflow_config,
                args = [
                    dict(flags=('--typeflags',), kwargs=dict(required=True, type=str, help="Typeflags configuration as a JSON string, or '@path/to/file.json' to load JSON from a file."))
                ],
                common_args = []
            ),
            'reset' : dict(
                help = "...",
                func = cmd_airflow_reset,
                args = [
                    dict(flags=('--object_type', ), kwargs=dict(required=False, type=str, help="Process only the input object type (default=all).")),
                    dict(flags=('--options',     ), kwargs=dict(required=False, type=str, help="...")),
                    dict(flags=('--verbose', '-v'), kwargs=dict(action='store_true', help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'expire' : dict(
                help = "Set 'has_expired' flag to 1 for objects based on date when they were last cached.",
                func = cmd_airflow_expire,
                args = [
                    dict(flags=('--object_type',   ), kwargs=dict(required=False, type=str, help="Process only the input object type (default=all).")),
                    dict(flags=('--older_than',    ), kwargs=dict(required=False, type=int, help="Set 'has_expired' flag to 1 for objects older than <int> in days (default=90).")),
                    dict(flags=('--limit_per_type',), kwargs=dict(required=False, type=int, help="Limit number of objects to process (default=100).")),
                    dict(flags=('--count',   '-c'  ), kwargs=dict(action='store_true', help="Show number of items that match the input conditions (no execution).")),
                    dict(flags=('--verbose', '-v'  ), kwargs=dict(action='store_true', help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'refresh' : dict(
                help = "...",
                func = cmd_airflow_refresh,
                args = [
                    dict(flags=('--object_type',           ), kwargs=dict(required=False, type=str, help="Process only the input object type (default=all).")),
                    dict(flags=('--refresh_checksums', '-r'), kwargs=dict(action='store_true', help="Show number of items that match the input conditions (no execution).")),
                    dict(flags=('--verbose',           '-v'), kwargs=dict(action='store_true', help="Execute in verbose mode.")),
                ],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Domain: cache       #
    #---------------------#
    'cache' : dict(
        help = "Cache-related operations (pending items, recalculation, etc.).",
        common_args = dict(),
        commands = {
            'update' : dict(
                help = "Operations on the 'to process' queue for cache jobs.",
                func = cmd_cache_update,
                args = [
                    dict(flags=('--formulas',  ), kwargs=dict(required=False, type=str, help="...")),
                    dict(flags=('--actions',   ), kwargs=dict(required=False, type=str, help="...")),
                    dict(flags=('--matrix',    ), kwargs=dict(action='store_true', help="Show number of items waiting to be processed.")),
                    dict(flags=('--count', '-c'), kwargs=dict(action='store_true', help="Show number of items waiting to be processed."))
                ],
                common_args = []
            ),
            'build' : dict(
                help = "...",
                func = cmd_cache_build,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, help="..."))
                ],
                common_args = []
            ),
            'patch' : dict(
                help = "...",
                func = cmd_cache_patch,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, help="..."))
                ],
                common_args = []
            ),
            'to_process' : dict(
                help = "Operations on the 'to process' queue for cache jobs.",
                func = cmd_cache_to_process,
                args = [
                    dict(flags=('--count', '-c'), kwargs=dict(action='store_true', help="Show number of items waiting to be processed.")),
                    dict(flags=('--reset', '-r'), kwargs=dict(action='store_true', help="Reset the 'to process' queue / counters."))
                ],
                common_args = []
            ),
            'debug' : dict(
                help = "...",
                func = cmd_cache_debug,
                args = [],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Domain: config      #
    #---------------------#
    'config' : dict(
        help = "...",
        common_args = {},
        commands = {
            'index' : dict(
                help = "Print out index config.",
                func = cmd_config_index,
                args = [],
                common_args = [],
            )
        }
    ),

    #---------------------#
    # Domain: db          #
    #---------------------#
    'db' : dict(
        help = "MySQL database client.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'test' : dict(
                help = "Test MySQL server(s).",
                func = cmd_db_test,
                args = [],
                common_args = ['env'],
            ),
            'export' : dict(
                help = "Export database from MySQL server into local folder.",
                func = cmd_db_export,
                args = [
                    dict(flags = ('--schema_name'  ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to export.")),
                    dict(flags = ('--output_folder',), kwargs = dict(required=True,  type=str, help="Output folder to save the exported index.")),
                    dict(flags = ('--table_name'   ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (optional).")),
                    dict(flags = ('--filter_by'    ,), kwargs = dict(required=False, type=str, default='TRUE',  help="Filter condition to apply to all tables.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', help="Include table definitions in export.")),
                    dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', help="Include data in export."))
                ],
                common_args = ['env'],
            ),
            'import' : dict(
                help = "Import database from local folder into MySQL server.",
                func = cmd_db_import,
                args = [
                    dict(flags = ('--schema_name' ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to import.")),
                    dict(flags = ('--input_folder',), kwargs = dict(required=True,  type=str, help="Input folder containing the exported index.")),
                    dict(flags = ('--table_name'  ,), kwargs = dict(required=False, type=str, default=None, help="Name of the table to export (optional).")),
                    dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', help="Include table definitions in export.")),
                    dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', help="Include data in export.")),
                    dict(flags = ('--ignore_existing'      , '-i'), kwargs = dict(action='store_true', help="Soft ignore table creation and existing rows."))
                ],
                common_args = ['env'],
            ),
            'copy' : dict(
                help = "Copy database or tables across MySQL servers.",
                func = cmd_db_copy,
                args = [
                    dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, default='test', help="Source environment.")),
                    dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, default='prod', help="Target environment.")),
                    dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to copy from.")),
                    dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to copy to.")),
                    dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (optional).")),
                    dict(flags = ('--chunk_size' ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of rows to copy per batch (default=1000000).")),
                ],
                common_args = [],
            ),
            'compare' : dict(
                help = "Compare database or tables across MySQL servers.",
                func = cmd_db_compare,
                args = [
                    dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, default='test', help="Source environment.")),
                    dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, default='prod', help="Target environment.")),
                    dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to copy from.")),
                    dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to copy to.")),
                    dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None,   help="Name of the table to export (optional).")),
                    dict(flags = ('--exact_row_count', '-e'), kwargs = dict(action='store_true', help="Calculate exact row counts (slower)."))
                ],
                common_args = [],
            ),
        }
    ),

    #---------------------#
    # Domain: index       #
    #---------------------#
    'index' : dict(
        help = "Manage ElasticSearch server and indexes.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'test' : dict(
                help = "Test ElasticSearch server(s).",
                func = cmd_index_test,
                args = [],
                common_args = ['env'],
            ),
            'info' : dict(
                help = "Print server info.",
                func = cmd_index_info,
                args = [],
                common_args = ['env'],
            ),
            'health' : dict(
                help = "Print server health.",
                func = cmd_index_health,
                args = [],
                common_args = ['env'],
            ),
            'list' : dict(
                help = "List indexes.",
                func = cmd_index_list,
                args = [dict(flags = ('--display_size', '-s'), kwargs = dict(action='store_true', help="Display index list with sizes (in GB).")),
                        dict(flags = ('--aliases'     , '-a'), kwargs = dict(action='store_true', help="Display index aliases."))
                ],
                common_args = ['env']
            ),
            'export' : dict(
                help = "Export index from ElasticSearch server into local folder.",
                func = cmd_index_export,
                args = [
                    dict(flags = ('--index_name'   ,), kwargs = dict(required=True,  type=str, help="Name of the index to export.")),
                    dict(flags = ('--output_folder',), kwargs = dict(required=True,  type=str, help="Output folder to save the exported index.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--use_gzip'        , '-gz'), kwargs = dict(action='store_true', help="Compress exported data files using GZIP.")),
                    dict(flags = ('--replace_existing',  '-r'), kwargs = dict(action='store_true', help="Replace existing files in the output folder if they exist.")),
                    dict(flags = ('--force'           ,  '-f'), kwargs = dict(action='store_true', help="Force replace without prompting for confirmation."))
                ],
                common_args = ['env'],
            ),
            'import' : dict(
                help = "Import index from local folder into ElasticSearch server.",
                func = cmd_index_import,
                args = [
                    dict(flags = ('--input_folder' ,), kwargs = dict(required=True,  type=str, help="Input folder containing the exported index.")),
                    dict(flags = ('--rename_to'    ,), kwargs = dict(required=False, type=str, default=None,    help="Rename index to this name on target server.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--replace_existing',  '-r'), kwargs = dict(action='store_true', help="Replace existing files in the output folder if they exist.")),
                    dict(flags = ('--force'           ,  '-f'), kwargs = dict(action='store_true', help="Force replace without prompting for confirmation."))
                ],
                common_args = ['env'],
            ),
            'copy' : dict(
                help = "Copy index across ElasticSearch servers.",
                func = cmd_index_copy,
                args = [
                    dict(flags = ('--index_name'   ,), kwargs = dict(required=False, type=str, default=None,   help="Name of the index to copy.")),
                    dict(flags = ('--from_env'     ,), kwargs = dict(required=False, type=str, default='test', help="Source environment.")),
                    dict(flags = ('--to_env'       ,), kwargs = dict(required=False, type=str, default='prod', help="Target environment.")),
                    dict(flags = ('--rename_to'    ,), kwargs = dict(required=False, type=str, default=None,   help="Rename index to this name on target server.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000,   help="Number of documents to copy per batch (default=1000000).")),
                    dict(flags = ('--alias_pattern',), kwargs = dict(required=False, type=str, default=None,   help="Name of the alias to copy.")),
                    dict(flags = ('--use_gzip'        , '-gz'), kwargs = dict(action='store_true', help="Compress exported data files using GZIP.")),
                    dict(flags = ('--replace_existing',  '-r'), kwargs = dict(action='store_true', help="Replace existing files in the output folder if they exist.")),
                    dict(flags = ('--force'           ,  '-f'), kwargs = dict(action='store_true', help="Force replace without prompting for confirmation."))
                ],
                common_args = [],
            ),
            'backup' : dict(
                help = "Backup one or all indexes from ElasticSearch server into backup folder.",
                func = cmd_index_test,
                args = [
                    dict(flags = ('--index_name'   ,), kwargs = dict(required=True,  type=str, help="Name of the index to export.")),
                    dict(flags = ('--output_folder',), kwargs = dict(required=True,  type=str, help="Output folder to save the exported index.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--use_gzip'        , '-gz'), kwargs = dict(action='store_true', help="Compress exported data files using GZIP.")),
                    dict(flags = ('--replace_existing',  '-r'), kwargs = dict(action='store_true', help="Replace existing files in the output folder if they exist.")),
                    dict(flags = ('--force'           ,  '-f'), kwargs = dict(action='store_true', help="Force replace without prompting for confirmation."))
                ],
                common_args = ['env'],
            ),
        }
    ),
}
