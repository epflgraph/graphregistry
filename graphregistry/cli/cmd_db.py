# graphregistry/cli/cmd_db.py
# Manage MySQL/MariaDB server operations.

#-----------------------------------#
# Handler: Test server connectivity #
#-----------------------------------#
def cmd_db_test(args):
    """
    Usage:
        graphregistry db test [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Test server connectivity.")

    # Execute command:
    # - Test connection to MySQL server
    if args.env:
        if db.test(engine_name=args.env) is True:
            print(f"✅ MySQL server is up and running [env='{args.env}'].")
        else:
            print(f"❌ MySQL server is down or unreachable [env='{args.env}'].")
    else:
        for engine in db.engine.keys():
            if db.test(engine_name=engine) is True:
                print(f"✅ MySQL server is up and running [env='{engine}'].")
            else:
                print(f"❌ MySQL server is down or unreachable [env='{engine}'].")

    # Print footers
    print("🖥️  ~ Done.")

#--------------------------------------------#
# Handler: Export database into local folder #
#--------------------------------------------#
def cmd_db_export(args):
    """
    Usage:
        graphregistry db export [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Export database into local folder.")

    # Get export options
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data

    #------------------#
    # Execute commands #
    #------------------#

    # Export MySQL table definitions only
    if t and c and not d:
        db.export_create_table(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            table_name    = args.table_name,
            output_folder = args.output_folder
        )

    # Export MySQL table definitions and data
    elif t and d:
        db.export_table(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            table_name    = args.table_name,
            output_folder = args.output_folder,
            filter_by     = args.filter_by,
            chunk_size    = args.chunk_size,
            include_create_tables = c
        )

    # Export MySQL table definitions for entire database
    elif not t and c and not d:
        db.export_create_tables_in_database(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            output_folder = args.output_folder
        )

    # Export MySQL table definitions and data for entire database
    elif not t and d:
        db.export_database(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            output_folder = args.output_folder,
            filter_by     = args.filter_by,
            chunk_size    = args.chunk_size,
            include_create_tables = c
        )

    # Nothing to do
    else:
        print("⚠️  No export action specified. Please provide valid options.")

    # Print footers
    print("🖥️  ~ Done.")

#--------------------------------------------#
# Handler: Import database from local folder #
#--------------------------------------------#
def cmd_db_import(args):
    """
    Usage:
        graphregistry db import [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import database from local folder.")

    # Get import options
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data
    i = args.ignore_existing

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t and c and not d:
        db.import_create_table(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = f"{args.input_folder}/{args.table_name}",
            include_keys = False,
            ignore_existing = i
        )

    # Import MySQL table definitions and data
    elif t and d:
        db.import_table(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = f"{args.input_folder}/{args.table_name}",
            create_keys_after_import = True,
            ignore_existing = i
        )

    # Import MySQL table definitions and data for entire database
    elif not t:
        db.import_database(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = args.input_folder,
            create_keys_after_import = True,
            ignore_existing = i
        )

    # Nothing to do
    else:
        print("⚠️  No import action specified. Please provide valid options.")

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------------------#
# Handler: Copy database or tables across servers #
#-------------------------------------------------#
def cmd_db_copy(args):
    """
    Usage:
        graphregistry db copy [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Copy database or tables across servers.")

    # Get import options
    t = args.table_name is not None

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t:
        db.copy_table(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            table_name           = args.table_name,
            chunk_size           = args.chunk_size,
            create_keys_after_import = True
        )
    else:
        db.copy_database(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            chunk_size           = args.chunk_size,
            create_keys_after_import = True
        )

    # Print footers
    print("🖥️  ~ Done.")

#----------------------------------------------------#
# Handler: Compare database or tables across servers #
#----------------------------------------------------#
def cmd_db_compare(args):
    """
    Usage:
        graphregistry db compare [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Compare database or tables across servers.")

    # Get import options
    t = args.table_name is not None
    e = args.exact_row_count

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t:
        db.compare_tables(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            table_name           = args.table_name,
            exact_row_count      = e
        )
    else:
        db.compare_databases(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            exact_row_count      = e
        )

    # Print footers
    print("🖥️  ~ Done.")
