#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphDB, GraphIndex
from sqlalchemy import create_engine as SQLEngine, text
from loguru import logger as sysmsg
from yaml import safe_load
import os, rich, glob, re

# Load global config file
with open('config.yaml', 'r') as fp:
    global_config = safe_load(fp)

# Initialize the GraphDB instance
db = GraphDB()

# Initialize the ElasticSearch instance
es = GraphIndex()

#==================================================#
# Step 1: Test MySQL and ElasticSearch connections #
#==================================================#

# Define function to test MySQL connection
def test_mysql():

    # Test the MySQL connection
    test_result = db.test(engine_name='test')

    # Provide feedback based on the test result
    match test_result:
        case True:
            sysmsg.success("🔌 ✅ Successfully connected to the MySQL test server.\n")
        case False:
            sysmsg.error("🔌 ❌ Failed to connect to the MySQL test server.\n")
            exit()
        case None:
            sysmsg.critical("🔌 🚨 An error occurred while trying to connect to the MySQL test server.\n")
            exit()

# Execute step?
if True:

    # Print info message
    sysmsg.info("🔌 📝 Test MySQL connection.")

    # Test MySQL connection
    test_mysql()

# Define function to test ElasticSearch connection
def test_elastic():

    # Test the ElasticSearch connection
    test_result = es.test(engine_name='test')

    # Provide feedback based on the test result
    match test_result:
        case True:
            sysmsg.success("🔌 ✅ Successfully connected to the ElasticSearch test server.\n")
        case False:
            sysmsg.error("🔌 ❌ Failed to connect to the ElasticSearch test server.\n")
            exit()
        case None:
            sysmsg.critical("🔌 🚨 An error occurred while trying to connect to the ElasticSearch test server.\n")
            exit()

# Execute step?
if True:

    # Print info message
    sysmsg.info("🔌 📝 Test ElasticSearch connection.")

    # Test MySQL connection
    test_elastic()

#===========================================================================#
# Step 2: Check if required MySQL databases exist and create them otherwise #
#===========================================================================#

# Schemas to process
schemas_to_process = ['ontology', 'registry', 'lectures', 'airflow', 'elasticsearch_cache', 'graph_cache_test', 'graphsearch_test', 'website']

# Execute step?
if True:

    # Print info message
    sysmsg.info("🗄️ 📝 Check if required databases exist. Create them otherwise.")

    # Loop over all required database schema names
    for schema_key in schemas_to_process:

        # Get the schema name from config
        schema_name = global_config['mysql']['db_schema_names'][schema_key]

        # Check if the database exists, create it otherwise
        if db.database_exists(engine_name='test', schema_name=schema_name):
            sysmsg.warning(f"Database '{schema_name}' exists in the MySQL test server.")
        else:
            sysmsg.trace(f"Database '{schema_name}' does not exist in the MySQL test server. Creating database ...")
            db.create_database(engine_name='test', schema_name=schema_name)
            if db.database_exists(engine_name='test', schema_name=schema_name):
                sysmsg.trace(f"Database '{schema_name}' successfully created in the MySQL test server.")
            else:
                sysmsg.error(f"🗄️ ❌ Failed to create database '{schema_name}' in the MySQL test server.")
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
        schema_name = global_config['mysql']['db_schema_names'][schema_key]

        # Print info message
        sysmsg.trace(f"Processing database '{schema_name}' ...")

        # Get SQL file path
        sql_file_path = f'database/init/create_tables/schema_{schema_key}.sql'

        # Open SQL file and get all table names that should be created
        with open(sql_file_path, 'r') as sql_file:
            match = re.findall(r'CREATE (TABLE IF NOT EXISTS|OR REPLACE VIEW)\s*([^\s]*)\s*', sql_file.read())

        # Check if any tables were found in the SQL file
        if not match:
            sysmsg.error(f"🗂️ ❌ No CREATE TABLE or VIEW statements found in SQL file.")
            exit()
        else:
            sysmsg.trace(f"Found {len(match)} CREATE TABLE or VIEW statements in SQL file:")
            required_tables = [table_name for _, table_name in match]
            if False:
                for table_name in required_tables:
                    print(f" - {table_name}")

        # Print info message
        sysmsg.trace(f"Executing CREATE TABLE or VIEW statements for database '{schema_name}' ...")

        # Test MySQL connection
        # test_mysql()

        # Execute SQL file
        db.execute_query_from_file(engine_name='test', file_path=sql_file_path, database=schema_name, verbose=True)

        # Print info message
        sysmsg.trace(f"Verifying that all required tables were created ...")

        # Get list of tables in schema
        tables_in_schema = sorted(db.get_tables_in_schema(engine_name='test', schema_name=schema_name, include_views=True))

        # Check if all required tables were created
        if not set([t.lower() for t in required_tables]).issubset([t.lower() for t in tables_in_schema]):
            sysmsg.trace(f"Not all required tables were created. Tables missing: {set(required_tables) - set(tables_in_schema)}")
            sysmsg.error(f"🗂️ ❌ Failed to create all required tables in database '{schema_name}'.")
            exit()

        # Check if there are any extra tables and warn if so
        if len(tables_in_schema) > len(required_tables):
            sysmsg.warning(f"Database '{schema_name}' contains extra tables that were not created by the init script: {set(tables_in_schema) - set(required_tables)}")

        # Print success message
        sysmsg.trace(f"Done creating tables in database '{schema_name}'.")

    # Print success message
    sysmsg.success("🗂️ ✅ All required MySQL tables were created.\n")

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
            sysmsg.error(f"➡️ ❌ Could not extract schema name from file name '{os.path.basename(sql_file)}'.")
            exit()

        # Get the schema name
        schema_key = match.group(1)

        # Execute SQL file
        db.execute_query_from_file(engine_name='test', file_path=sql_file, database=global_config['mysql']['db_schema_names'][schema_key])

    # Print success message
    sysmsg.success("➡️ ✅ Done inserting default data.\n")
