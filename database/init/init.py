#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphDB
from sqlalchemy import create_engine as SQLEngine, text
from loguru import logger as sysmsg
from yaml import safe_load
import os, rich, glob, re

# Load global config file
with open('config.yaml', 'r') as fp:
    global_config = safe_load(fp)

# Initialize the GraphDB instance
db = GraphDB()

#==================================================#
# Step 1: Test MySQL and ElasticSearch connections #
#==================================================#

# Execute step?
if True:

    # Print info message
    sysmsg.info("🔌 📝 Test MySQL connection.")

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

#===========================================================================#
# Step 2: Check if required MySQL databases exist and create them otherwise #
#===========================================================================#

# Execute step?
if True:

    # Print info message
    sysmsg.info("🗄️ 📝 Check if required databases exist. Create them otherwise.")

    for schema_name in global_config['mysql']['db_schema_names'].values():

        # Skip production databases
        if 'prod' in schema_name:
            continue

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

    # Execute CREATE TABLE statements from files
    for schema_name in ['registry', 'lectures', 'airflow', 'graph_cache_test']:

        # Print info message
        sysmsg.trace(f"Processing database '{global_config['mysql']['db_schema_names'][schema_name]}' ...")

        # Get SQL file path
        sql_file_path = f'/Users/francisco/Cloud/Academia/CEDE/EPFLGraph/GitHub/graphregistry/database/init/create_tables/schema_{schema_name}.sql'

        # Open SQL file and get all table names that should be created
        with open(sql_file_path, 'r') as sql_file:
            required_tables = re.findall(r'CREATE TABLE IF NOT EXISTS\s*([^\s]*)\s*', sql_file.read())

        # Check if any tables were found in the SQL file
        if not required_tables:
            sysmsg.error(f"🗂️ ❌ No CREATE TABLE statements found in SQL file.")
            exit()
        else:
            sysmsg.trace(f"Found {len(required_tables)} CREATE TABLE statements in SQL file:")
            for table_name in required_tables:
                print(f" - {table_name}")

        # Print info message
        sysmsg.trace(f"Executing CREATE TABLE statements ...")

        # Execute SQL file
        db.execute_query_from_file(engine_name='test', file_path=sql_file_path, database=global_config['mysql']['db_schema_names'][schema_name], verbose=True)

        # Print info message
        sysmsg.trace(f"Verifying that all required tables were created ...")

        # Get list of tables in schema
        tables_in_schema = sorted(db.get_tables_in_schema(engine_name='test', schema_name=global_config['mysql']['db_schema_names'][schema_name]))

        # Check if all required tables were created
        if not set(required_tables).issubset(tables_in_schema):
            sysmsg.trace(f"Not all required tables were created. Tables missing: {set(required_tables) - set(tables_in_schema)}")
            sysmsg.error(f"🗂️ ❌ Failed to create all required tables in database '{global_config['mysql']['db_schema_names'][schema_name]}'.")
            exit()

        # Check if there are any extra tables and warn if so
        if len(tables_in_schema) > len(required_tables):
            sysmsg.warning(f"Database '{global_config['mysql']['db_schema_names'][schema_name]}' contains extra tables that were not created by the init script: {set(tables_in_schema) - set(required_tables)}")

        # Print success message
        sysmsg.trace(f"Done creating tables in database '{global_config['mysql']['db_schema_names'][schema_name]}'.")

    # Print success message
    sysmsg.success("🗂️ ✅ All required MySQL tables were created.\n")