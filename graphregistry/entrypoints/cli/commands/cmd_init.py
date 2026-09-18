# graphregistry/entrypoints/cli/commands/cmd_init.py
from __future__ import annotations
import glob
import os
import re
from typing import Annotated
import typer
from loguru import logger as sysmsg
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext

# Create the Typer sub-app for the init command.
app = typer.Typer(
    help                   = "Initialize the Registry instance with required databases, tables, and default data.",
    no_args_is_help        = False,
    invoke_without_command = True,
    context_settings       = {"help_option_names": ["--help", "-h"]},
)

# Public Method: Initialize a new Registry instance (databases, tables, default data).
@app.callback()
def cmd_init(
    ctx: typer.Context,
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="Execute in dry run mode (do not modify any data).")] = False,
    index_tables: Annotated[bool, typer.Option("--index-tables", "-i", help="Ensure index buildup tables from config/application/config_index.json exist.")] = False,
    import_ontology_sample: Annotated[bool, typer.Option("--import-ontology-sample", help="Import the graph ontology sample set before initializing Registry tables.")] = False,
    force: Annotated[bool, typer.Option("--force", help="Allow execution in prod execution mode.")] = False,
) -> None:
    """Initialize the Registry instance with required databases, tables, and default data."""
    cli_ctx: CLIContext = ctx.obj
    glbcfg = cli_ctx.global_config
    db = cli_ctx.db
    commit = not dry_run

    # Refuse to run in prod execution mode unless explicitly forced.
    if glbcfg.mysql_execution_mode == "prod" and not force:
        raise typer.BadParameter(
            "init is only allowed in 'dev' execution mode. "
            "Use --force if you really want to run it in 'prod' mode."
        )

    #------------------------------------------------------------#
    # Step 0: Import graph ontology sample set                     #
    #------------------------------------------------------------#
    if import_ontology_sample:
        # Use the configured ontology schema name and bundled sample SQL folder.
        ontology_input_folder = "database/init/sample_sets/graph_ontology/sql"
        ontology_schema_name = glbcfg.schema_ontology

        # Determine whether the ontology schema already has tables.
        ontology_already_populated = (
            db.database_exists(engine_name=env, schema_name=ontology_schema_name)
            and len(db.get_tables_in_schema(engine_name=env, schema_name=ontology_schema_name, include_views=False)) > 0
        )

        # Import ontology tables/data only if the schema is still empty.
        if ontology_already_populated:
            sysmsg.warning(
                f"🧬 Ontology schema '{ontology_schema_name}' already contains tables; "
                "skipping ontology table/data import. Drop the schema first if you want to re-import."
            )
        else:
            sysmsg.info("🧬 📝 Importing graph ontology sample set.")
            sysmsg.trace(f"Importing ontology into schema '{ontology_schema_name}' from '{ontology_input_folder}' ...")

            # Replicate: graphdb import --schema_name <ontology> --input_folder <folder> -c -d -z
            if commit:
                db.import_database(
                    engine_name              = env,
                    schema_name              = ontology_schema_name,
                    input_folder             = ontology_input_folder,
                    create_keys_after_import = True,
                    ignore_existing          = False,
                    verbose                  = verbose,
                    compress                 = True,
                )
                sysmsg.success("🧬 ✅ Graph ontology sample set imported.")
            else:
                sysmsg.success("🧬 💡 Dry run: would import graph ontology sample set.")

        # Import (or refresh) ontology views. Views use [[ontology]] placeholder for the schema name.
        ontology_views_folder = "database/init/sample_sets/graph_ontology/views"
        view_sql_files = sorted(glob.glob(f"{ontology_views_folder}/*.sql"))
        if view_sql_files:
            sysmsg.info("🧬 📝 Importing graph ontology views.")
            for view_sql_file in view_sql_files:
                view_name = os.path.basename(view_sql_file)
                sysmsg.trace(f"Importing view '{view_name}' into schema '{ontology_schema_name}' ...")
                if commit:
                    with open(view_sql_file, "r", encoding="utf-8") as fp:
                        view_sql = fp.read().replace("[[ontology]]", ontology_schema_name)
                    db.execute_query(
                        engine_name = env,
                        query       = view_sql,
                        schema_name = ontology_schema_name,
                        verbose     = verbose,
                    )
                else:
                    sysmsg.trace(f"🧬 💡 Dry run: would execute view '{view_name}' against '{ontology_schema_name}'.")
            sysmsg.success("🧬 ✅ Graph ontology views imported.\n")
        else:
            sysmsg.warning(f"🧬 No ontology view SQL files found in '{ontology_views_folder}'.")
            print("")

    # Schemas that must exist and be initialized for the Registry to function.
    schemas_to_process = [
        "registry",
        "lectures",
        "airflow",
        "graph_cache_test",
        "graphsearch_test",
        "elasticsearch_cache",
        "traversals",
    ]

    #------------------------------------------------------------#
    # Step 1: Ensure required MySQL databases exist                #
    #------------------------------------------------------------#
    sysmsg.info("🗄️ 📝 Check if required databases exist. Create them otherwise.")

    # Iterate over the collection.
    for schema_key in schemas_to_process:
        schema_name = glbcfg.schema_names[schema_key]

        # Iterate over the collection.
        for execution_mode_prefix in ["_0_PYTESTS_", "_1_DEV_", ""]:
            # Ignore prod databases when running in dev mode.
            if glbcfg.mysql_execution_mode == "dev" and execution_mode_prefix == "":
                sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                continue

            # Prepare execution_schema_name for the following steps.
            execution_schema_name = execution_mode_prefix + schema_name

            # Handle the conditional case.
            if db.database_exists(engine_name=env, schema_name=execution_schema_name):
                sysmsg.warning(f"Database '{execution_schema_name}' already exists.")
            elif commit:
                sysmsg.trace(f"Database '{execution_schema_name}' does not exist. Creating database ...")
                db.create_database(engine_name=env, schema_name=execution_schema_name)

                # Handle the conditional case.
                if db.database_exists(engine_name=env, schema_name=execution_schema_name):
                    sysmsg.trace(f"Database '{execution_schema_name}' successfully created.")
                else:
                    sysmsg.error(f"🗄️ ❌ Failed to create database '{execution_schema_name}'.")
                    raise typer.Exit(code=1)
            else:
                sysmsg.trace(f"🗄️ 💡 Dry run: would create database '{execution_schema_name}'.")

    # Continue with the next step.
    sysmsg.success("🗄️ ✅ All required databases exist (or would be created).\n")

    #------------------------------------------------------------#
    # Step 2: Create required MySQL tables and views               #
    #------------------------------------------------------------#
    sysmsg.info("🗂️ 📝 Create required MySQL tables if they don't exist.")

    # Iterate over the collection.
    for schema_key in schemas_to_process:
        schema_name = glbcfg.schema_names[schema_key]

        # Iterate over the collection.
        for execution_mode_prefix in ["_0_PYTESTS_", "_1_DEV_", ""]:
            if glbcfg.mysql_execution_mode == "dev" and execution_mode_prefix == "":
                sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                continue

            # Prepare execution_schema_name for the following steps.
            execution_schema_name = execution_mode_prefix + schema_name
            sysmsg.trace(f"\nProcessing database '{execution_schema_name}' ...")

            # Prepare sql_file_path for the following steps.
            sql_file_path = f"database/init/schemas/schema_{schema_key}.sql"
            if not os.path.isfile(sql_file_path):
                sysmsg.critical(f"🗂️ ❌ SQL file '{sql_file_path}' not found for database '{execution_schema_name}'.")
                raise typer.Exit(code=1)

            # Manage the resource context.
            with open(sql_file_path, "r", encoding="utf-8") as sql_file:
                match = re.findall(
                    r"CREATE\s+(TABLE(?:\s+IF\s+NOT\s+EXISTS)?|OR\s+REPLACE\s+VIEW)\s+([^\s(]+)",
                    sql_file.read(),
                    re.IGNORECASE,
                )

            # Handle the conditional case.
            if not match:
                sysmsg.warning("🗂️  No CREATE TABLE or VIEW statements found in SQL file.")
                required_tables: list[str] = []
            else:
                sysmsg.trace(f"Found {len(match)} CREATE TABLE or VIEW statements in SQL file:")
                required_tables = [table_name for _, table_name in match]
                for table_name in required_tables:
                    print(f" - {table_name}")

            # Continue with the next step.
            sysmsg.trace(f"Executing CREATE TABLE or VIEW statements for database '{execution_schema_name}' ...")

            # Handle the conditional case.
            if commit:
                db.execute_query_from_file(
                    engine_name = env,
                    file_path   = sql_file_path,
                    database    = execution_schema_name,
                    verbose     = verbose,
                )

                # Continue with the next step.
                sysmsg.trace(f"Verifying that all required tables were created ...")

                # Prepare tables_in_schema for the following steps.
                tables_in_schema = sorted(
                    db.get_tables_in_schema(engine_name=env, schema_name=execution_schema_name, include_views=True)
                )
                required_tables_lower = {t.lower() for t in required_tables}
                tables_in_schema_lower = {t.lower() for t in tables_in_schema}

                # Handle the conditional case.
                if not required_tables_lower.issubset(tables_in_schema_lower):
                    missing = required_tables_lower - tables_in_schema_lower
                    sysmsg.error(f"Not all required tables were created. Tables missing: {missing}")
                    sysmsg.critical(f"🗂️ ❌ Failed to create all required tables in database '{execution_schema_name}'.")
                    raise typer.Exit(code=1)

                # Handle the conditional case.
                if len(tables_in_schema) > len(required_tables):
                    sysmsg.warning(
                        f"Database '{execution_schema_name}' contains extra tables: "
                        f"{set(tables_in_schema) - set(required_tables)}"
                    )

                # Continue with the next step.
                sysmsg.trace(f"☑️ Done creating tables in database '{execution_schema_name}'.")
            else:
                sysmsg.trace(f"🗂️ 💡 Dry run: would execute '{sql_file_path}' against '{execution_schema_name}'.")

    # Continue with the next step.
    sysmsg.success("🗂️ ✅ All required MySQL tables were created (or would be created).\n")

    #------------------------------------------------------------#
    # Step 3: Ensure dynamic index buildup tables exist            #
    #------------------------------------------------------------#
    if index_tables:
        sysmsg.info("🗂️ 📝 Ensure index buildup tables from config/application/config_index.json exist.")

        # Import dynamic SQL helpers only when index tables must be ensured.
        from graphregistry.common.dbstruct import DynamicSQL, GraphTable

        # Prepare dynsql for the following steps.
        dynsql = DynamicSQL(db=db)
        cache_schema_name = glbcfg.schema_graph_cache_test

        # Prepare expected_doc_tables for the following steps.
        expected_doc_tables = [
            f"IndexBuildup_Fields_Docs_{doc_type}"
            for doc_type in dynsql.doc_types
        ]

        # Prepare expected_doclink_tables for the following steps.
        expected_doclink_tables = sorted({
            f"IndexBuildup_Fields_Links_ParentChild_{sorted([doc_type, link_type])[0]}_{sorted([doc_type, link_type])[1]}"
            for doc_type, link_type in dynsql.doclink_types_org
        })

        # Prepare missing_tables for the following steps.
        missing_tables = [
            table_name
            for table_name in expected_doc_tables + expected_doclink_tables
            if not db.table_exists(engine_name=env, schema_name=cache_schema_name, table_name=table_name)
        ]

        # Handle the conditional case.
        if not missing_tables:
            sysmsg.success("🗂️ ✅ All index buildup tables already exist.\n")
        elif not commit:
            sysmsg.info(f"🗂️ 💡 Dry run: would create {len(missing_tables)} index buildup tables:")
            for table_name in missing_tables:
                sysmsg.trace(f" - {cache_schema_name}.{table_name}")
            print("")
        else:
            sysmsg.trace(f"Creating {len(missing_tables)} missing index buildup tables ...")
            for table_name in missing_tables:
                tb = GraphTable(db=db, schema_name=cache_schema_name, table_name=table_name)
                db.execute_query_in_shell(
                    engine_name = env,
                    query       = tb.create_table_sql,
                    verbose     = False,
                    query_id    = "setup-init-index",
                )
                if not db.table_exists(engine_name=env, schema_name=cache_schema_name, table_name=table_name):
                    sysmsg.critical(f"🗂️ ❌ Failed to create table '{cache_schema_name}.{table_name}'.")
                    raise typer.Exit(code=1)
            sysmsg.success("🗂️ ✅ Index buildup tables ensured.\n")

    #------------------------------------------------------------#
    # Step 4: Insert default data into MySQL tables                #
    #------------------------------------------------------------#
    sysmsg.info("➡️ 📝 Insert default data into MySQL tables.")

    # Map schema directory names (e.g. "graph_registry") back to schema keys.
    schema_name_to_key = {v: k for k, v in glbcfg.schema_names.items()}

    # Prepare list_of_sql_files for the following steps, including nested folders.
    list_of_sql_files = sorted(glob.glob("database/init/default_data/**/*.sql", recursive=True))

    # Iterate over the collection.
    for sql_file in list_of_sql_files:
        basename = os.path.basename(sql_file)
        match = re.match(r".*schema_([a-z_]*)\.data\..*\.sql", basename)
        if match:
            schema_key = match.group(1)
        else:
            # Fall back to the schema folder name (e.g. "graph_registry" -> "registry").
            # SQL files may live in an intermediate "sql" subfolder, so look two levels up.
            folder_name = os.path.basename(os.path.dirname(os.path.dirname(sql_file)))
            schema_key = schema_name_to_key.get(folder_name)
            if schema_key is None:
                sysmsg.critical(f"➡️ ❌ Could not extract schema name from file '{sql_file}'.")
                raise typer.Exit(code=1)

        # Log the resolved schema before executing the file against each mode.
        sysmsg.trace(f"Processing default data SQL file '{sql_file}' for schema '{schema_key}' ...")

        # Iterate over the collection.
        for execution_mode_prefix in ["_0_PYTESTS_", "_1_DEV_", ""]:
            if glbcfg.mysql_execution_mode == "dev" and execution_mode_prefix == "":
                sysmsg.warning(f"Skipping prod database for schema '{schema_key}' since execution mode is 'dev'.")
                continue

            # Prepare execution_schema_name for the following steps.
            execution_schema_name = execution_mode_prefix + glbcfg.schema_names[schema_key]

            # Handle the conditional case.
            if commit:
                db.execute_query_from_file(engine_name=env, file_path=sql_file, database=execution_schema_name)
            else:
                sysmsg.trace(f"➡️ 💡 Dry run: would execute '{sql_file}' against '{execution_schema_name}'.")

    # Continue with the next step.
    sysmsg.success("➡️ ✅ Done inserting default data (or would insert).\n")
