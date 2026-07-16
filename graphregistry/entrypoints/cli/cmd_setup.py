# graphregistry/entrypoints/cli/cmd_config.py
from loguru import logger as sysmsg
import os, rich, glob, re

#-------------------------------------------#
# Handler: Initialize the Registry instance #
#-------------------------------------------#
def cmd_setup_init(args):

    # Fetch context objects
    glbcfg = args.ctx.global_config
    db = args.ctx.db

    # Get command-line arguments
    db_env  = args.env
    commit  = not args.dry_run
    verbose = args.verbose
    exit_on_critical = True

    #===========================================================================#
    # Step 2: Check if required MySQL databases exist and create them otherwise #
    #===========================================================================#

    # Schemas to process
    schemas_to_process = ['registry', 'lectures', 'airflow', 'graph_cache_test', 'graphsearch_test', 'elasticsearch_cache']

    # Execute step?
    if True:

        # Print info message
        sysmsg.info("🗄️ 📝 Check if required databases exist. Create them otherwise.")

        # Loop over all required database schema names
        for schema_key in schemas_to_process:

            # Get the schema name from config
            schema_name = glbcfg.settings['mysql']['db_schema_names'][schema_key]

            # Loop over 3 execution modes: pytests, dev, prod
            for execution_mode_prefix in ['_0_PYTESTS_', '_1_DEV_', '']:

                # Ignore prod databases if in dev mode
                if glbcfg.mysql_execution_mode == 'dev' and execution_mode_prefix == '':
                    sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                    continue

                # Build execution schema name
                execution_schema_name = execution_mode_prefix + schema_name

                # Check if the database exists, create it otherwise
                if db.database_exists(engine_name=db_env, schema_name=execution_schema_name):
                    sysmsg.warning(f"Database '{execution_schema_name}' exists in the MySQL test server.")
                else:
                    sysmsg.trace(f"Database '{execution_schema_name}' does not exist in the MySQL test server. Creating database ...")

                    # ‼️ Execute database creation
                    if commit:
                        db.create_database(engine_name=db_env, schema_name=execution_schema_name)

                    # Verify if database was created
                    if db.database_exists(engine_name=db_env, schema_name=execution_schema_name):
                        sysmsg.trace(f"Database '{execution_schema_name}' successfully created in the MySQL test server.")
                    else:
                        sysmsg.error(f"🗄️ ❌ Failed to create database '{execution_schema_name}' in the MySQL test server.")
                        exit()

        # Print success message
        sysmsg.success("🗄️ ✅ All required databases exist (or created) in the MySQL test server.\n")

    #==========================================================#
    # Step 3: Create required MySQL tables if they don't exist #
    #==========================================================#

    # Execute step?
    if True:

        # Print info message
        sysmsg.info("🗂️ 📝 Create required MySQL tables if they don't exist.")

        # Loop over all required database schema names
        for schema_key in schemas_to_process:

            # Get the schema name from config
            schema_name = glbcfg.settings['mysql']['db_schema_names'][schema_key]

            # Loop over 3 execution modes: pytests, dev, prod
            for execution_mode_prefix in ['_0_PYTESTS_', '_1_DEV_', '']:

                # Ignore prod databases if in dev mode
                if glbcfg.mysql_execution_mode == 'dev' and execution_mode_prefix == '':
                    sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                    continue

                # Build execution schema name
                execution_schema_name = execution_mode_prefix + schema_name

                # Print info message
                sysmsg.trace(f"\nProcessing database '{execution_schema_name}' ...")

                # Get SQL file path
                sql_file_path = f'database/init/schemas/schema_{schema_key}.sql'

                # Check if file exists
                if not os.path.isfile(sql_file_path):
                    sysmsg.critical(f"🗂️ ❌ SQL file '{sql_file_path}' not found for database '{execution_schema_name}'.")
                    if exit_on_critical:
                        exit()

                # Open SQL file and get all table names that should be created
                with open(sql_file_path, 'r') as sql_file:
                    match = re.findall(r'CREATE (TABLE IF NOT EXISTS|OR REPLACE VIEW)\s*([^\s]*)\s*', sql_file.read())

                # Check if any tables were found in the SQL file
                if not match:
                    sysmsg.warning(f"🗂️  No CREATE TABLE or VIEW statements found in SQL file.")
                    required_tables = []
                else:
                    sysmsg.trace(f"Found {len(match)} CREATE TABLE or VIEW statements in SQL file:")
                    required_tables = [table_name for _, table_name in match]
                    if True:
                        for table_name in required_tables:
                            print(f" - {table_name}")

                # Print info message
                sysmsg.trace(f"Executing CREATE TABLE or VIEW statements for database '{execution_schema_name}' ...")

                # ‼️ Execute SQL file
                if commit:
                    db.execute_query_from_file(engine_name=db_env, file_path=sql_file_path, database=execution_schema_name, verbose=verbose)

                # Print info message
                sysmsg.trace(f"Verifying that all required tables were created ...")

                # Get list of tables in schema
                tables_in_schema = sorted(db.get_tables_in_schema(engine_name=db_env, schema_name=execution_schema_name, include_views=True))

                # Check if all required tables were created
                if not set([t.lower() for t in required_tables]).issubset([t.lower() for t in tables_in_schema]):
                    sysmsg.error(f"Not all required tables were created. Tables missing: {set(required_tables) - set(tables_in_schema)}")
                    sysmsg.critical(f"🗂️ ❌ Failed to create all required tables in database '{execution_schema_name}'.")
                    if exit_on_critical:
                        exit()

                # Check if there are any extra tables and warn if so
                if len(tables_in_schema) > len(required_tables):
                    sysmsg.warning(f"Database '{execution_schema_name}' contains extra tables that were not created by the init script: {set(tables_in_schema) - set(required_tables)}")

                # Print success message
                sysmsg.trace(f"☑️ Done creating tables in database '{execution_schema_name}'.")

        # Print success message
        sysmsg.success("🗂️ ✅ All required MySQL tables were created.\n")

    #===========================================================#
    # Step 3b: Ensure dynamic index buildup tables exist        #
    #===========================================================#

    if args.index_tables:

        # Print info message
        sysmsg.info("🗂️ 📝 Ensure index buildup tables from config_index.json exist.")

        # Import helpers locally to keep plain setup init startup fast
        from graphregistry.common.dbstruct import DynamicSQL, GraphTable

        # Get lazy DB client and dynamic SQL metadata (doc types come from config_index.json)
        db = args.ctx.db
        dynsql = DynamicSQL(db=db)

        # Target schema for index buildup tables
        cache_schema_name = glbcfg.schema_graph_cache_test

        # Collect expected doc buildup tables
        expected_doc_tables = [
            f"IndexBuildup_Fields_Docs_{doc_type}"
            for doc_type in dynsql.doc_types
        ]

        # Collect expected doc-link buildup tables (parent-child, organisational)
        expected_doclink_tables = sorted(set(
            f"IndexBuildup_Fields_Links_ParentChild_{sorted([doc_type, link_type])[0]}_{sorted([doc_type, link_type])[1]}"
            for doc_type, link_type in dynsql.doclink_types_org
        ))

        # Find missing tables
        missing_tables = [
            table_name
            for table_name in expected_doc_tables + expected_doclink_tables
            if not db.table_exists(engine_name=db_env, schema_name=cache_schema_name, table_name=table_name)
        ]

        if not missing_tables:
            sysmsg.success("🗂️ ✅ All index buildup tables already exist.\n")
        elif not commit:
            sysmsg.info(f"🗂️ 💡 Dry run: would create {len(missing_tables)} index buildup tables:")
            for table_name in missing_tables:
                sysmsg.trace(f" - {cache_schema_name}.{table_name}")
            print('')
        else:
            # Create missing tables using the existing GraphTable helper
            # (no need to spin up the full GraphRegistry for schema creation).
            sysmsg.trace(f"Creating {len(missing_tables)} missing index buildup tables ...")
            for table_name in missing_tables:
                tb = GraphTable(db=db, schema_name=cache_schema_name, table_name=table_name)
                db.execute_query_in_shell(
                    engine_name = db_env,
                    query       = tb.create_table_sql,
                    verbose     = False,
                    query_id    = 'setup-init-index'
                )
                # Verify
                if not db.table_exists(engine_name=db_env, schema_name=cache_schema_name, table_name=table_name):
                    sysmsg.critical(f"🗂️ ❌ Failed to create table '{cache_schema_name}.{table_name}'.")
                    exit()
            sysmsg.success("🗂️ ✅ Index buildup tables ensured.\n")

    #===============================================#
    # Step 4: Insert default data into MySQL tables #
    #===============================================#

    # Execute step?
    if True:

        # Print info message
        sysmsg.info("➡️ 📝 Insert default data into MySQL tables.")

        # Get list of SQL files with default data
        list_of_sql_files = sorted(glob.glob('database/init/default_data/*.sql'))

        # Loop over SQL files and execute them
        for sql_file in list_of_sql_files:

            # Extract the schema name from the file name
            match = re.match(r'.*schema_([a-z]*)\.data\..*\.sql', os.path.basename(sql_file))
            if not match:
                sysmsg.critical(f"➡️ ❌ Could not extract schema name from file name '{os.path.basename(sql_file)}'.")
                if exit_on_critical:
                    exit()

            # Get the schema name
            assert match, f"Could not extract schema name from file name '{os.path.basename(sql_file)}'."
            schema_key = match.group(1)

            # Print status
            sysmsg.trace(f"Processing default data SQL file '{sql_file}' for schema '{schema_key}' ...")

            # Loop over 3 execution modes: pytests, dev, prod
            for execution_mode_prefix in ['_0_PYTESTS_', '_1_DEV_', '']:

                # Ignore prod databases if in dev mode
                if glbcfg.mysql_execution_mode == 'dev' and execution_mode_prefix == '':
                    sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                    continue

                # Build execution schema name
                execution_schema_name = execution_mode_prefix + glbcfg.settings['mysql']['db_schema_names'][schema_key]

                # ‼️ Execute SQL file
                if commit:
                    db.execute_query_from_file(engine_name=db_env, file_path=sql_file, database=execution_schema_name)

        # Print success message
        sysmsg.success("➡️ ✅ Done inserting default data.\n")
