# graphregistry/entrypoints/cli/specs.py
from typing import Any, Dict
from pathlib import Path

# Import all command handler functions
from graphregistry.entrypoints.cli.cmd_setup import (
    cmd_setup_init,
)
from graphregistry.entrypoints.cli.cmd_config import (
    cmd_config_index,
)
from graphregistry.entrypoints.cli.cmd_es import (
    cmd_es_test,
    cmd_es_info,
    cmd_es_health,
    cmd_es_list,
    cmd_es_copy,
    cmd_es_export,
    cmd_es_import,
    cmd_es_index
)
from graphregistry.entrypoints.cli.cmd_ai import (
    cmd_ai_detect_concepts,
)
from graphregistry.entrypoints.cli.cmd_data import (
    cmd_data_list,
    cmd_data_exists,
    cmd_data_get,
    cmd_data_save,
    cmd_data_delete,
    cmd_data_import,
    cmd_data_delete_loose_ends,
)
from graphregistry.entrypoints.cli.cmd_airflow import (
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
from graphregistry.entrypoints.cli.cmd_cache import (
    cmd_cache_update,
    cmd_cache_to_process,
    cmd_cache_debug,
)
from graphregistry.entrypoints.cli.cmd_run import (
    cmd_run_formula,
)
from graphregistry.entrypoints.cli.cmd_index import (
    cmd_index_build,
    cmd_index_patch,
    cmd_index_mixed_views,
    cmd_index_generate
)

# Import db config
from graphdb.core.config import GraphDBConfig

from graphregistry.common.paths import CONFIG_DB_PATH
db_config = GraphDBConfig.from_file(CONFIG_DB_PATH)
# db_config = GraphDBConfig.from_file("config/config_db.yaml")

# Global common arguments
global_common_args = {
    'env' : dict(
        flags = ('--env',),
        kwargs = dict(
            help = "Specify environment (default=test).",
            choices = tuple(db_config.environments.keys()),
            default = db_config.default_env
        )
    )
}

#===================================================#
# CLI Definitions for all Subcommands and Arguments #
#===================================================#
cli_definitions: Dict[str, Any] = {

    #---------------------#
    # Command: setup      #
    #---------------------#
    'setup' : dict(
        help = "Initialize a new Registry instance with base data and configuration.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'init' : dict(
                help = "Initialize the Registry instance.",
                func = cmd_setup_init,
                args = [dict(flags = ('--dry_run'     , '-d'), kwargs = dict(action='store_true', default=False, help="Execute in dry run mode (do not modify any data).")),
                        dict(flags = ('--verbose'     , '-v'), kwargs = dict(action='store_true', default=False, help="Display detailed output.")),
                        dict(flags = ('--index_tables', '-i'), kwargs = dict(action='store_true', default=False, help="Ensure index buildup tables from config_index.json exist.")),
                ],
                common_args = ['env'],
            )
        }
    ),

    #---------------------#
    # Command: config     #
    #---------------------#
    'config' : dict(
        help = "Inspect and validate Registry configuration files.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'index' : dict(
                help = "Print out index config.",
                func = cmd_config_index,
                args = [],
                common_args = ['env'],
            )
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
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'detect_concepts' : dict(
                help = "Detect concepts for nodes using GraphAI.",
                func = cmd_ai_detect_concepts,
                args = [],
                common_args = ['env']
            )
        }
    ),

    #---------------------#
    # Command: data       #
    #---------------------#
    'data' : dict(
        help = "Manage base registry data.",
        common_args = {
            'env': global_common_args['env']
        },
        commands = {
            'list' : dict(
                help = "List existing nodes or edges for given object type(s).",
                func = cmd_data_list,
                args = [
                    dict(flags=('--node_request',), kwargs=dict(required=False, type=str, default=None, help="Request JSON file containing object type(s) and optional filters for listing nodes.")),
                    dict(flags=('--edge_request',), kwargs=dict(required=False, type=str, default=None, help="Request JSON file containing object type(s) and optional filters for listing edges.")),
                ],
                common_args = ['env']
            ),
            'import' : dict(
                help = "Import data from json file.",
                func = cmd_data_import,
                args = [
                    dict(flags=('--input_file',   ), kwargs=dict(required=True,  type=str, default=None,     help="Import data from file [=path/to/file.json]")),
                    dict(flags=('--import_method',), kwargs=dict(required=False, type=str, default='object', help="Import objects one by one [=object] or as a list [=list] (default=object).")),
                    dict(flags=('--actions',      ), kwargs=dict(required=False, type=str, default='eval',   help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
                    dict(flags=('--detect_concepts', '-dc'), kwargs=dict(action='store_true', default=False, help="Detect concepts on import.")),
                ],
                common_args = ['env']
            ),
            'exists' : dict(
                help = "Check if node or edge exists in the registry.",
                func = cmd_data_exists,
                args = [
                    dict(flags=('--node_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to node key JSON file.")),
                    dict(flags=('--edge_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key JSON file.")),
                    dict(flags=('--node_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to node key list JSON file.")),
                    dict(flags=('--edge_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key list JSON file.")),
                ],
                common_args = ['env']
            ),
            'get' : dict(
                help = "Import data from input or json file.",
                func = cmd_data_get,
                args = [
                    dict(flags=('--node_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to node key JSON file.")),
                    dict(flags=('--edge_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key JSON file.")),
                    dict(flags=('--node_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to node key list JSON file.")),
                    dict(flags=('--edge_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key list JSON file.")),
                ],
                common_args = ['env']
            ),
            'save' : dict(
                help = "Save node(s) or edge(s) from JSON file.",
                func = cmd_data_save,
                args = [
                    dict(flags=('--node',      ), kwargs=dict(required=False, type=str, default=None, help="Path to node JSON file.")),
                    dict(flags=('--edge',      ), kwargs=dict(required=False, type=str, default=None, help="Path to edge JSON file.")),
                    dict(flags=('--node_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to node list JSON file.")),
                    dict(flags=('--edge_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to edge list JSON file.")),
                    # dict(flags=('--subgraph',  ), kwargs=dict(required=False, type=str, default=None, help="save node and edge list (subgraph) from JSON string, or '@path/to/file.json' to load JSON from a file.")),
                    dict(flags=('--actions',   ), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
                    # dict(flags=('--detect_concepts', '-dc'), kwargs=dict(action='store_true', default=False, help="Detect concepts on save.")),
                ],
                common_args = ['env']
            ),
            'delete' : dict(
                help = "Delete data from Registry.",
                func = cmd_data_delete,
                args = [
                    dict(flags=('--node_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to node key JSON file.")),
                    dict(flags=('--edge_key',      ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key JSON file.")),
                    dict(flags=('--node_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to node key list JSON file.")),
                    dict(flags=('--edge_key_list', ), kwargs=dict(required=False, type=str, default=None, help="Path to edge key list JSON file.")),
                    dict(flags=('--actions',       ), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval)."))
                ],
                common_args = ['env']
            ),
            'delete_loose_ends' : dict(
                help = "Delete loose ends from cache and graphsearch index tables.",
                func = cmd_data_delete_loose_ends,
                args = [
                    dict(flags=('--update_loose_ends',   '-ul'), kwargs=dict(action='store_true', default=False, help="Refresh the NoLooseEnds reference table from source page profiles before cleaning.")),
                    dict(flags=('--include_scores_matrix', '-is'), kwargs=dict(action='store_true', default=False, help="Also clean scores matrix tables in graph_cache.")),
                    dict(flags=('--refresh_graph',       '-rg'), kwargs=dict(action='store_true', default=False, help="Force recalculation of the Operations_N_Object_T_LargestConnectedGraph cache table.")),
                    dict(flags=('--actions',               ), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
                ],
                common_args = ['env']
            ),
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
                    dict(flags=('--limit_per_type',), kwargs=dict(required=False, type=int, default=None, help=f"Maximum number of objects to expire per document type (default: 100, max: see config limits.limit_per_type_max).")),
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
                    dict(flags=('--limit_per_type',), kwargs=dict(required=False, type=int, default=None, help="Maximum number of objects to refresh per document type (default: 100, max: see config limits.limit_per_type_max).")),
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
                    dict(flags=('--formulas',    ), kwargs=dict(required=False, type=str, default=None,   help="Comma-separated formulas to apply: fields,views,traversals,scores (default=none).")),
                    dict(flags=('--formula_path',), kwargs=dict(required=False, type=str, default=None,   help="Relative path to a single SQL formula file under database/formulas, e.g. 'traversals/formula.007.course-lecture-slide-concept.concept_detection'. Folder aliases (fields, traversals, scores) and omission of the .sql suffix are supported. Runs independently of --formulas.")),
                    dict(flags=('--matrix',      ), kwargs=dict(action='store_true',      default=False,  help="(Re)calculate scores matrix.")),
                    dict(flags=('--actions',     ), kwargs=dict(required=False, type=str, default='eval', help="Comma-separated actions to perform: print,eval,commit (default=eval).")),
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
    # Command: run        #
    #---------------------#
    'run' : dict(
        help = "Run ad-hoc scripts for testing and debugging purposes.",
        common_args = dict(),
        commands = {
            'formula' : dict(
                help = "Execute SQL formula with placeholders.",
                func = cmd_run_formula,
                args = [
                    dict(flags=('--input'       , '-i'), kwargs=dict(required=False, type=str, default=None, help="Path to SQL file containing the formula to execute.")),
                    dict(flags=('--resolve_only', '-r'), kwargs=dict(action='store_true', default=False, help="Only resolve placeholders and print final SQL without executing.")),
                    dict(flags=('--verbose'     , '-v'), kwargs=dict(action='store_true', default=False, help="Execute in verbose mode.")),
                ],
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
    )
}