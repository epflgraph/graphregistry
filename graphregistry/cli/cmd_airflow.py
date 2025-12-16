# Graph Registry CLI: airflow
import json
from pathlib import Path

# #==============================#
# # Register domain and commands #
# #==============================#
# def register(subparsers):
#     """
#     Register 'airflow' domain commands.

#     Usage:
#       graphregistry airflow to_process --count
#       graphregistry airflow to_process --reset
#     """

#     # Register Level 1 parser
#     # >> graphregistry airflow [-h|...]
#     parser = subparsers.add_parser("airflow",
#         help="Synchronize Registry with Airflow and manage type-flag configurations.")

#     # Register Level 2 parser
#     # >> graphregistry airflow cmd [-h|...]
#     subcmd_airflow = parser.add_subparsers(dest="airflow_cmd", metavar="<command>", required=True,
#         help="Airflow subcommand (e.g. 'sync').")

#     #--------------------------#
#     # Level 2 subcommand: sync #
#     #--------------------------#

#     # Register Level 2 subcommand
#     # >> graphregistry airflow sync [-h|...]
#     parser_airflow_sync = subcmd_airflow.add_parser("sync", help="Sync registry with Airflow")

#     # Bind handler
#     parser_airflow_sync.set_defaults(func=cmd_airflow_sync)

#     #----------------------------#
#     # Level 2 subcommand: status #
#     #----------------------------#

#     # Register Level 2 subcommand
#     # >> graphregistry airflow status [-h|...]
#     parser_airflow_status = subcmd_airflow.add_parser("status", help="Get status from Airflow")

#     # Bind handler
#     parser_airflow_status.set_defaults(func=cmd_airflow_status)

#     #--------------------------------#
#     # Level 2 subcommand: to_process #
#     #--------------------------------#

#     # Register Level 2 subcommand
#     # >> graphregistry airflow to_process [-h|...]
#     parser_airflow_to_process = subcmd_airflow.add_parser("to_process",
#         help="Operations on the 'to process' queue for airflow jobs.")

#     # Register mutually exclusive arguments
#     # >> graphregistry airflow to_process --count | --reset
#     group_airflow_to_process = parser_airflow_to_process.add_mutually_exclusive_group(required=True)

#     # Register --count argument
#     # >> graphregistry airflow to_process --count
#     group_airflow_to_process.add_argument("--count", action="store_true",
#         help="Show number of items waiting to be processed.")

#     # Register --reset argument
#     # >> graphregistry airflow to_process --reset
#     group_airflow_to_process.add_argument("--reset", action="store_true",
#         help="Reset the 'to process' queue / counters.")

#     # Bind handler
#     parser_airflow_to_process.set_defaults(func=cmd_airflow_to_process)

#     #----------------------------#
#     # Level 2 subcommand: config #
#     #----------------------------#

#     # Register Level 2 subcommand
#     # >> graphregistry airflow config [-h|...]
#     parser_airflow_config = subcmd_airflow.add_parser("config",
#         help="Configure Airflow typeflags for orchestration.")

#     # Register --typeflags argument
#     # >> graphregistry airflow config --typeflags='{json}' | --typeflags=@file.json
#     parser_airflow_config.add_argument("--typeflags", required=True, type=str,
#         help="Typeflags configuration as a JSON string, or '@path/to/file.json' to load JSON from a file.")

#     # Bind handler
#     parser_airflow_config.set_defaults(func=cmd_airflow_config)

#     #----------------------------#
#     # Level 2 subcommand: expire #
#     #----------------------------#

#     # Register Level 2 subcommand
#     # >> graphregistry airflow expire [-h|...]
#     parser_airflow_expire = subcmd_airflow.add_parser("expire",
#         help="Set 'has_expired' flag to 1 for objects based on date when they were last cached.")

#     # Register --object_type argument
#     # >> graphregistry airflow expire --object_type=<str>
#     parser_airflow_expire.add_argument("--object_type", required=False, type=str,
#         help="Process only the input object type (default=all).")

#     # Register --older_than argument
#     # >> graphregistry airflow expire --older_than=<int>
#     parser_airflow_expire.add_argument("--older_than", required=False, type=int,
#         help="Set 'has_expired' flag to 1 for objects older than <int> in days (default=90).")

#     # Register --limit_per_type argument
#     # >> graphregistry airflow expire --limit_per_type=<int>
#     parser_airflow_expire.add_argument("--limit_per_type", required=False, type=int,
#         help="Limit number of objects to process (default=100).")

#     # Register --verbose argument
#     # >> graphregistry airflow expire --verbose
#     parser_airflow_expire.add_argument("--verbose", action="store_true",
#         help="Execute in verbose mode.")

#     # Bind handler
#     parser_airflow_expire.set_defaults(func=cmd_airflow_expire)

# #===========================#
# # Command handler functions #
# #===========================#

#-------------------------------------------#
# Handler: Sync new data into airflow table #
#-------------------------------------------#
def cmd_airflow_sync(args):
    """
    Handle:
      graphregistry cache sync
    """

    # Fetch context objects
    gr = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Sync new registry data with airflow tables.")

    # Execute command:
    # - Sync new registry data with airflow tables
    gr.orchestrator.sync()

    # Print footers
    print("🖥️  ~ Done.")

#--------------------------------------#
# Handler: Print airflow status tables #
#--------------------------------------#
def cmd_airflow_status(args):
    """
    Handle:
      graphregistry cache status
    """

    # Fetch context objects
    gr = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Display airflow status tables.")

    # Execute command:
    # - Display airflow status tables
    gr.orchestrator.status()

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_to_process(args):
    """
    Handle:
      graphregistry airflow to_process --count
      graphregistry airflow to_process --reset
    """

    # Fetch context objects
    glbcfg = args.ctx.global_config
    db = args.ctx.db

    # Get list of tables where to_process flag is present
    list_of_tables = [
        table_name
        for table_name  in db.get_tables_in_schema(engine_name='test', schema_name=glbcfg.schema_airflow)
        if 'to_process' in db.get_column_names(    engine_name='test', schema_name=glbcfg.schema_airflow, table_name=table_name)
    ]

    # Check arguments:
    # >> graphregistry airflow to_process --count
    if args.count:

        # Print headers
        print("🖥️  ~ Graph Registry CLI. Count number of rows where 'to_process=1' in airflow tables.")
        print(f"\n{'-'*78}\ntable_name{' '*(64 - len('table_name') + 2)}n_to_process\n{'-'*78}")

        # Execute command:
        # - for each table, count and print the number of rows where to_process=1
        for table_name in list_of_tables:
            n = db.count_rows_in_table(engine_name='test', schema_name=glbcfg.schema_airflow, table_name=table_name, where_clause="to_process = 1")
            print(f"{table_name} {'.'*(64 - len(table_name))} {n}")

        # Print footers
        print('-'*78+'\n')
        print("🖥️  ~ Done counting number of rows to process.")

    # >> graphregistry airflow to_process --reset
    elif args.reset:

        # Print headers
        print("🖥️  ~ Graph Registry CLI. Reset 'to_process' flags in airflow tables.")

        # Execute command:
        # - for each table, set all cells to_process=0
        for table_name in list_of_tables:
            print(f"⚙️  Processing table: {table_name} ...")
            db.set_cells(engine_name='test', schema_name=glbcfg.schema_airflow, table_name=table_name, set=[('to_process', 0)], where=[('to_process', 1)], verbose=False)

        # Print footers
        print("🖥️  ~ Done.")

#--------------------------------------#
# Handler: Configure Airflow typeflags #
#--------------------------------------#
def cmd_airflow_config(args):
    """
    Handle:
      graphregistry airflow config --typeflags='{json}'
      graphregistry airflow config --typeflags=@path/to/file.json
    """

    # Print headers
    print(f"🖥️  ~ Graph Registry CLI. Set type flags configuration.")

    # Fetch context objects
    gr = args.ctx.registry

    # Resolve the configuration
    tf_arg = args.typeflags

    # Case 1: --typeflags=@path/to/file.json
    if tf_arg.startswith("@"):
        path_str = tf_arg[1:]
        cfg_path = Path(path_str)

        if not cfg_path.exists():
            raise FileNotFoundError(f"Typeflags config file not found: {cfg_path}")

        print(f"Loading typeflags configuration from file: {cfg_path}")
        with cfg_path.open("r") as fp:
            cfg = json.load(fp)

    # Case 2: --typeflags='<json>'
    else:
        print("Parsing inline JSON typeflags configuration.")
        try:
            cfg = json.loads(tf_arg)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON passed to --typeflags: {e}") from e

    # Execute command:
    # - Configure typeflags in orchestrator
    print("⚙️  Applying typeflags configuration to Airflow orchestrator ...")
    gr.orchestrator.typeflags.config(config_json=cfg)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_expire(args):
    """
    Handle:
      graphregistry airflow expire [...]
    """

    # Fetch context objects
    gr = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Set 'has_expired' flag to 1 for objects based on date when they were last cached.")
    if args.object_type or args.older_than or args.limit_per_type or args.verbose:
        print("\nInput options:")
        if args.object_type:
            print(f"  object_type .......... {args.object_type}")
        if args.older_than:
            print(f"  older_than ........... {args.older_than}")
        if args.limit_per_type:
            print(f"  limit_per_type ....... {args.limit_per_type}")
        if args.verbose:
            print(f"  verbose .............. {args.verbose}")
        print('')

    # Execute command:
    # - Set 'has_expired' flag to 1 for objects based on date when they were last cached.
    gr.orchestrator.expire(doc_type=args.object_type, older_than=args.older_than, limit_per_type=args.limit_per_type, verbose=args.verbose)

    # Print footers
    print("🖥️  ~ Done.")
