# graphregistry/entrypoints/cli/cmd_airflow.py
import json
from pathlib import Path
from loguru import logger as sysmsg

# #===========================#
# # Command handler functions #
# #===========================#

#---------------------------------------------------#
# Helper: Validate limit_per_type against config cap #
#---------------------------------------------------#
def _validate_limit_per_type(limit_per_type, limit_per_type_max):
    """
    Refuse to process if limit_per_type exceeds the configured maximum.
    Returns True when the value is acceptable (including None), False otherwise.
    """
    if limit_per_type is not None and limit_per_type > limit_per_type_max:
        sysmsg.warning(
            f"limit_per_type ({limit_per_type}) exceeds LIMIT_PER_TYPE_MAX ({limit_per_type_max}). "
            f"Large values may cause instability and frozen SQL operations that take a long time to rollback. "
            f"Refusing to process."
        )
        return False
    return True

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
        for table_name  in db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=glbcfg.schema_airflow)
        if 'to_process' in db.get_column_names(    engine_name='xaas_coresrv', schema_name=glbcfg.schema_airflow, table_name=table_name)
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
            n = db.count_rows_in_table(engine_name='xaas_coresrv', schema_name=glbcfg.schema_airflow, table_name=table_name, where_clause="to_process = 1")
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
            db.set_cells(engine_name='xaas_coresrv', schema_name=glbcfg.schema_airflow, table_name=table_name, set=[('to_process', 0)], where=[('to_process', 1)], verbose=False)

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
      graphregistry airflow config --typeflags=path/to/file.json
    """

    # Print headers
    print(f"🖥️  ~ Graph Registry CLI. Set type flags configuration.")

    # Fetch context objects
    gr = args.ctx.registry

    # Resolve the configuration
    tf_arg = args.typeflags

    # Case 1: --typeflags=@path/to/file.json or --typeflags=path/to/file.json
    path_str = tf_arg[1:] if tf_arg.startswith("@") else tf_arg
    cfg_path = Path(path_str)
    if cfg_path.is_file():
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

#----------------------------------#
# Handler: Update object checksums #
#----------------------------------#
def cmd_airflow_update_checksums(args):
    """
    Handle:
      graphregistry airflow update_checksums [...]
    """

    # Fetch context objects
    gr = args.ctx.registry

    # Get input options
    actions = tuple(args.actions.split(',')) if args.actions else ('commit',)
    v = args.verbose

    # Print headers
    print("🖥️  ~ Graph Registry CLI. update_checksums.")

    # Execute command:
    # - ...
    gr.orchestrator.update_checksums_v2(actions=actions, verbose=v)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_reset(args):
    """
    Handle:
      graphregistry airflow reset [...]
    """

    # Fetch context objects
    gr = args.ctx.registry

    # Get input options
    doc_type = args.doc_type
    options  = tuple(args.options.split(','))  if args.options else ()
    v = args.verbose

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Reset 'to_process' flags.")

    # Execute command:
    # - ...
    gr.orchestrator.reset(options=options, doc_type=doc_type, verbose=v)

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
    glbcfg = args.ctx.global_config
    c = args.count
    v = args.verbose

    # Validate safety limits
    if not _validate_limit_per_type(args.limit_per_type, glbcfg.limit_per_type_max):
        return

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Set 'has_expired' flag to 1 for objects based on date when they were last cached.")
    if args.doc_type or args.older_than or args.limit_per_type or args.verbose:
        print("\nInput options:")
        if args.doc_type:
            print(f"  doc_type ............. {args.doc_type}")
        if args.older_than:
            print(f"  older_than ........... {args.older_than}")
        if args.limit_per_type:
            print(f"  limit_per_type ....... {args.limit_per_type}")
        if args.verbose:
            print(f"  verbose .............. {args.verbose}")
        print('')

    # Execute command:
    # - Set 'has_expired' flag to 1 for objects based on date when they were last cached.
    gr.orchestrator.expire(
        doc_type       = args.doc_type,
        older_than     = args.older_than,
        limit_per_type = args.limit_per_type,
        count_only = c,
        verbose    = v
    )

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_refresh(args):
    """
    Handle:
      graphregistry airflow refresh [...]
    """

    # Fetch context objects
    registry = args.ctx.registry
    glbcfg = args.ctx.global_config
    r = args.refresh_checksums
    v = args.verbose

    # Validate safety limits
    if not _validate_limit_per_type(args.limit_per_type, glbcfg.limit_per_type_max):
        return

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Set 'has_expired' flag to 1 for objects based on date when they were last cached.")

    if args.doc_type or args.limit_per_type or args.verbose:
        print("\nInput options:")
        if args.doc_type:
            print(f"  doc_type ............. {args.doc_type}")
        if args.limit_per_type:
            print(f"  limit_per_type ....... {args.limit_per_type}")
        if args.verbose:
            print(f"  verbose .............. {args.verbose}")
        print('')

    # Execute command:
    # - Set 'has_expired' flag to 1 for objects based on date when they were last cached.
    registry.orchestrator.refresh(
        doc_type = args.doc_type,
        refresh_checksums = r,
        limit_per_type = args.limit_per_type,
        verbose = v
    )

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_rollover(args):
    """
    Handle:
      graphregistry airflow rollover [...]
    """

    # Fetch context objects
    registry = args.ctx.registry
    actions = tuple(args.actions.split(',')) if args.actions else ()

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Rollover orchestrator tasks.")

    # Execute command:
    # - Rollover orchestrator tasks.
    registry.orchestrator.rollover(actions=actions)

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------------#
# Handler: Operations on to_process flags #
#-----------------------------------------#
def cmd_airflow_update_dates(args):
    """
    Handle:
      graphregistry airflow update_dates [...]
    """

    # Fetch context objects
    registry = args.ctx.registry
    actions = tuple(args.actions.split(',')) if args.actions else ()

    # Print headers
    print("🖥️  ~ Graph Registry CLI. update_dates orchestrator tasks.")

    # Execute command:
    # - Rollover orchestrator tasks.
    registry.orchestrator.update_dates(actions=actions)

    # Print footers
    print("🖥️  ~ Done.")
