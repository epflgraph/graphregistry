# graphregistry/cli/specs.py
from typing import Any, Dict

# Import all command handler functions
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
from graphregistry.cli.cmd_es import (
    cmd_es_test,
    cmd_es_info,
    cmd_es_health,
    cmd_es_list,
    cmd_es_copy,
    cmd_es_export,
    cmd_es_import,
    cmd_es_index
)
from graphregistry.cli.cmd_ai import (
    cmd_ai_test,
)
from graphregistry.cli.cmd_data import (
    cmd_data_import
)
from graphregistry.cli.cmd_airflow import (
    cmd_airflow_sync,
    cmd_airflow_status,
    cmd_airflow_to_process,
    cmd_airflow_config,
    cmd_airflow_update_checksums,
    cmd_airflow_reset,
    cmd_airflow_expire,
    cmd_airflow_refresh,
    cmd_airflow_rollover,
    cmd_airflow_update_dates
)
from graphregistry.cli.cmd_cache import (
    cmd_cache_update,
    cmd_cache_to_process,
    cmd_cache_debug,
)
from graphregistry.cli.cmd_index import (
    cmd_index_build,
    cmd_index_patch,
    cmd_index_mixed_views,
    cmd_index_generate
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
    # Command: config     #
    #---------------------#
    'config' : dict(
        help = "Inspect and validate Registry configuration files.",
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
    # Command: db         #
    #---------------------#
    'db' : dict(
        help = "Manage MySQL/MariaDB server operations.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'test' : dict(
                help = "Test server connectivity.",
                func = cmd_db_test,
                args = [],
                common_args = ['env'],
            ),
            'export' : dict(
                help = "Export database into local folder.",
                func = cmd_db_export,
                args = [
                    dict(flags = ('--schema_name'  ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to export.")),
                    dict(flags = ('--output_folder',), kwargs = dict(required=True,  type=str, help="Output folder to save the exported data into.")),
                    dict(flags = ('--table_name'   ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (if exporting only one table).")),
                    dict(flags = ('--filter_by'    ,), kwargs = dict(required=False, type=str, default='TRUE',  help="Filter condition to apply to all tables.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', default=False, help="Include table definitions in export.")),
                    dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', default=False, help="Include data in export."))
                ],
                common_args = ['env'],
            ),
            'import' : dict(
                help = "Import database from local folder.",
                func = cmd_db_import,
                args = [
                    dict(flags = ('--schema_name' ,), kwargs = dict(required=True,  type=str, help="Name of the database/schema to import.")),
                    dict(flags = ('--input_folder',), kwargs = dict(required=True,  type=str, help="Input folder containing the data to import.")),
                    dict(flags = ('--table_name'  ,), kwargs = dict(required=False, type=str, default=None, help="Name of the table to import (if importing only one table).")),
                    dict(flags = ('--include_create_tables', '-c'), kwargs = dict(action='store_true', default=False, help="Include table definitions in import.")),
                    dict(flags = ('--include_data'         , '-d'), kwargs = dict(action='store_true', default=False, help="Include data in import.")),
                    dict(flags = ('--ignore_existing'      , '-i'), kwargs = dict(action='store_true', default=False, help="Soft ignore table creation and existing rows."))
                ],
                common_args = ['env'],
            ),
            'copy' : dict(
                help = "Copy database or tables across servers.",
                func = cmd_db_copy,
                args = [
                    dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, default='xaas_coresrv', help="Source environment.")),
                    dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, default='xaas_prod',    help="Target environment.")),
                    dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to copy from.")),
                    dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to copy to.")),
                    dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None,    help="Name of the table to export (optional).")),
                    dict(flags = ('--chunk_size' ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of rows to copy per batch (default=1000000).")),
                ],
                common_args = [],
            ),
            'compare' : dict(
                help = "Compare database or tables across servers.",
                func = cmd_db_compare,
                args = [
                    dict(flags = ('--from_env'   ,), kwargs = dict(required=False, type=str, default='xaas_coresrv', help="Source environment.")),
                    dict(flags = ('--to_env'     ,), kwargs = dict(required=False, type=str, default='xaas_prod',    help="Target environment.")),
                    dict(flags = ('--from_schema',), kwargs = dict(required=True,  type=str, help="Name of the source database/schema to compare.")),
                    dict(flags = ('--to_schema'  ,), kwargs = dict(required=True,  type=str, help="Name of the target database/schema to compare.")),
                    dict(flags = ('--table_name' ,), kwargs = dict(required=False, type=str, default=None, help="Name of the table to compare (if comparing only one table).")),
                    dict(flags = ('--exact_row_count', '-e'), kwargs = dict(action='store_true', default=False, help="Calculate exact row counts (slower)."))
                ],
                common_args = [],
            ),
        }
    ),

    #---------------------#
    # Command: es         #
    #---------------------#
    'es' : dict(
        help = "Manage ElasticSearch server operations.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'test' : dict(
                help = "Test server connectivity.",
                func = cmd_es_test,
                args = [],
                common_args = ['env'],
            ),
            'info' : dict(
                help = "Print server info.",
                func = cmd_es_info,
                args = [],
                common_args = ['env'],
            ),
            'health' : dict(
                help = "Print server health.",
                func = cmd_es_health,
                args = [],
                common_args = ['env'],
            ),
            'list' : dict(
                help = "List indexes on the server.",
                func = cmd_es_list,
                args = [dict(flags = ('--display_size', '-s'), kwargs = dict(action='store_true', help="Display index list with sizes (in GB).")),
                        dict(flags = ('--aliases'     , '-a'), kwargs = dict(action='store_true', help="Display index aliases."))
                ],
                common_args = ['env']
            ),
            'export' : dict(
                help = "Export index into local folder.",
                func = cmd_es_export,
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
                help = "Import index from local folder.",
                func = cmd_es_import,
                args = [
                    dict(flags = ('--input_folder' ,), kwargs = dict(required=False, type=str, help="Input folder containing the exported index.")),
                    dict(flags = ('--rename_to'    ,), kwargs = dict(required=False, type=str, default=None,    help="Rename index to this name on target server.")),
                    dict(flags = ('--chunk_size'   ,), kwargs = dict(required=False, type=int, default=1000000, help="Number of documents to export per batch (default=1000000).")),
                    dict(flags = ('--replace_existing',  '-r'), kwargs = dict(action='store_true', help="Replace existing files in the output folder if they exist.")),
                    dict(flags = ('--force'           ,  '-f'), kwargs = dict(action='store_true', help="Force replace without prompting for confirmation."))
                ],
                common_args = ['env'],
            ),
            'copy' : dict(
                help = "Copy index across servers.",
                func = cmd_es_copy,
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
            'index' : dict(
                help = "Operations related to an individual index.",
                func = cmd_es_index,
                args = [
                    dict(flags = ('--index_name'   ,), kwargs = dict(required=True,  type=str, default=None, help="Name of the index to manage.")),
                    dict(flags = ('--create_alias' ,), kwargs = dict(required=False, type=str, default=None, help="Create alias pointing to index.")),
                    # dict(flags = ('--replace_existing', '-r'), kwargs = dict(action='store_true', default=False, help="Replace existing alias.")),
                    # dict(flags = ('--force'           , '-f'), kwargs = dict(action='store_true', default=False, help="Force replace without prompting for confirmation."))
                ],
                common_args = ['env'],
            ),
        }
    ),

    #---------------------#
    # Command: ai         #
    #---------------------#
    'ai' : dict(
        help = "Interact with GraphAI API.",
        common_args = dict(),
        commands = {
            'test' : dict(
                help = "Test connectivity to the GraphAI server using a simple translation request.",
                func = cmd_ai_test,
                args = [],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Command: data       #
    #---------------------#
    'data' : dict(
        help = "Manage base registry data.",
        common_args = dict(),
        commands = {
            'import' : dict(
                help = "Import data from json file.",
                func = cmd_data_import,
                args = [
                    dict(flags=('--input_file',   ), kwargs=dict(required=True,  type=str, default=None,     help="Import data from file [=path/to/file.json]")),
                    dict(flags=('--import_method',), kwargs=dict(required=False, type=str, default='object', help="Import objects one by one [=object] or as a list [=list] (default=object).")),
                    dict(flags=('--actions',      ), kwargs=dict(required=False, type=str, default='eval',   help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
                    dict(flags=('--detect_concepts', '-dc'), kwargs=dict(action='store_true', default=False, help="Detect concepts on import.")),
                ],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Command: airflow    #
    #---------------------#
    'airflow' : dict(
        help = "Manage Airflow orchestrator operations.",
        common_args = dict(),
        commands = {
            'reset' : dict(
                help = "Reset orchestrator 'to_process' flags across all Registry tables.",
                func = cmd_airflow_reset,
                args = [
                    dict(flags=('--doc_type',), kwargs=dict(required=False, type=str, default=None,        help="Restrict reset to a single document type (default: all types).")),
                    dict(flags=('--options', ), kwargs=dict(required=False, type=str, default='typeflags', help="Comma-separated options to apply. Options: typeflags,airflow,cache (default: typeflags).")),
                    dict(flags=('--verbose', '-v'), kwargs=dict(action='store_true', default=False, help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'sync' : dict(
                help = "Sync new Registry data with Airflow tables.",
                func = cmd_airflow_sync,
                args = [],
                common_args = []
            ),
            'status' : dict(
                help = "Display all Airflow status tables.",
                func = cmd_airflow_status,
                args = [],
                common_args = []
            ),
            'to_process' : dict(
                help = "Inspect or reset the Airflow 'to_process' flags across Airflow tables.",
                func = cmd_airflow_to_process,
                args = [
                    dict(flags=('--count', '-c'), kwargs=dict(action='store_true', default=False, help="Count rows with to_process=1 in each Airflow table.")),
                    dict(flags=('--reset', '-r'), kwargs=dict(action='store_true', default=False, help="Set to_process=0 for all rows currently marked to_process=1 in Airflow tables."))
                ],
                common_args = []
            ),
            'config' : dict(
                help = "Apply typeflags configuration JSON to the Airflow orchestrator.",
                func = cmd_airflow_config,
                args = [
                    dict(flags=('--typeflags',), kwargs=dict(required=True, type=str, default=None, help="Typeflags configuration as a JSON string, or '@path/to/file.json' to load JSON from a file."))
                ],
                common_args = []
            ),
            'update_checksums' : dict(
                help = "Update object checksums based on typeflag activation.",
                func = cmd_airflow_update_checksums,
                args = [
                    dict(flags=('--verbose', '-v'), kwargs=dict(action='store_true', default=False, help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'expire' : dict(
                help = "Mark objects as expired (has_expired=1) based on when they were last cached.",
                func = cmd_airflow_expire,
                args = [
                    dict(flags=('--doc_type',      ), kwargs=dict(required=False, type=str, default=None, help="Restrict expiration to a single document type (default: all types).")),
                    dict(flags=('--older_than',    ), kwargs=dict(required=False, type=int, default=None, help="Expire objects last cached more than N days ago (default: 90).")),
                    dict(flags=('--limit_per_type',), kwargs=dict(required=False, type=int, default=None, help="Maximum number of objects to expire per document type (default: 100).")),
                    dict(flags=('--count',   '-c'), kwargs=dict(action='store_true', default=False, help="Only show how many objects would be affected (do not modify data).")),
                    dict(flags=('--verbose', '-v'), kwargs=dict(action='store_true', default=False, help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'refresh' : dict(
                help = "Refresh 'to_process' flags based on changed checksums, expired dates, and/or objects being new.",
                func = cmd_airflow_refresh,
                args = [
                    dict(flags=('--doc_type',      ), kwargs=dict(required=False, type=str, default=None, help="Restrict refresh to a single document type (default: all types).")),
                    dict(flags=('--limit_per_type',), kwargs=dict(required=False, type=int, default=None, help="Maximum number of objects to refresh per document type (default: 100).")),
                    dict(flags=('--refresh_checksums', '-r'), kwargs=dict(action='store_true', default=False, help="Recompute and persist checksums for matching objects.")),
                    dict(flags=('--verbose',           '-v'), kwargs=dict(action='store_true', default=False, help="Execute in verbose mode.")),
                ],
                common_args = []
            ),
            'rollover' : dict(
                help = "Rollover checksums after a processing cycle (register current checksums as 'previous').",
                func = cmd_airflow_rollover,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval)."))
                ],
                common_args = []
            ),
            'update_dates' : dict(
                help = "Update 'last_date_cached' values for all affected objects after a processing cycle.",
                func = cmd_airflow_update_dates,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, help="Comma-separated actions to perform: print,eval,commit (default=eval)."))
                ],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Command: cache      #
    #---------------------#
    'cache' : dict(
        help = "Manage and update the computations cache for Knowledge Graph construction.",
        common_args = dict(),
        commands = {
            'update' : dict(
                help = "Execute computations for Knowledge Graph construction (selected subset based on Airflow config).",
                func = cmd_cache_update,
                args = [
                    dict(flags=('--formulas',), kwargs=dict(required=False, type=str, default=None,   help="Comma-separated formulas to apply: fields,views,traversals,scores (default=none).")),
                    dict(flags=('--matrix',  ), kwargs=dict(action='store_true',      default=False,  help="(Re)calculate scores matrix.")),
                    dict(flags=('--actions', ), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
                ],
                common_args = []
            ),
            'to_process' : dict(
                help = "Inspect or reset the Airflow 'to_process' flags across cache tables.",
                func = cmd_cache_to_process,
                args = [
                    dict(flags=('--count', '-c'), kwargs=dict(action='store_true', help="Count rows with to_process=1 in each cache table.")),
                    dict(flags=('--reset', '-r'), kwargs=dict(action='store_true', help="Set to_process=0 for all rows currently marked to_process=1 in cache tables."))
                ],
                common_args = []
            ),
            'debug' : dict(
                help = "General purpose debugging command to inspect class methods.",
                func = cmd_cache_debug,
                args = [],
                common_args = []
            )
        }
    ),

    #---------------------#
    # Command: index      #
    #---------------------#
    'index' : dict(
        help = "Manage and update the data structure (index) for the GraphSearch app.",
        common_args = dict(),
        commands = {
            'build' : dict(
                help = "Build up and/or update index field tables.",
                func = cmd_index_build,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, help="Comma-separated actions to perform: print,eval,commit (default=eval)."))
                ],
                common_args = []
            ),
            'patch' : dict(
                help = "Apply vertical and horizontal patching to all index tables.",
                func = cmd_index_patch,
                args = [
                    dict(flags=('--actions',), kwargs=dict(required=False, type=str, help="Comma-separated actions to perform: print,eval,commit (default=eval)."))
                ],
                common_args = []
            ),
            'mixed_views' : dict(
                help = "Generate mixed views for ElasticSearch.",
                func = cmd_index_mixed_views,
                args = [
                    dict(flags=('--replace_existing', '-r'), kwargs=dict(action='store_true', default=False, help="Replace existing views if they exist.")),
                    dict(flags=('--test_mode',        '-t'), kwargs=dict(action='store_true', default=False, help="Execute in test mode only."))
                ],
                common_args = []
            ),
            'generate' : dict(
                help = "Generate index data for other environments such as ElasticSearch.",
                func = cmd_index_generate,
                args = [
                    dict(flags=('--target',    ), kwargs=dict(required=False, type=str, default='elasticsearch', help="Target platform (default=elasticsearch).")),
                    dict(flags=('--index_date',), kwargs=dict(required=False, type=str, default=None, help="Date of ElasticSearch index in YYYY-MM-DD format (default=today's date).")),
                    dict(flags=('--ignore_warnings',    '-i'), kwargs=dict(action='store_true', default=False, help="Ignore warning messages.")),
                    dict(flags=('--replace_existing',   '-r'), kwargs=dict(action='store_true', default=False, help="Replace existing local cache and index files.")),
                    dict(flags=('--force_replace',      '-f'), kwargs=dict(action='store_true', default=False, help="Force replace without prompting.")),
                    dict(flags=('--local_cache_only', '-lco'), kwargs=dict(action='store_true', default=False, help="Generate local cache only.")),
                    dict(flags=('--index_file_only',  '-ifo'), kwargs=dict(action='store_true', default=False, help="Generate index file only (from existing local cache)."))
                ],
                common_args = []
            )
        }
    ),

}