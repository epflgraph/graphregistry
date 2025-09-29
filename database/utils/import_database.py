#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphDB
import sys, os

# Initialize the GraphDB instance
db = GraphDB()

# Print usage if incorrect number of arguments
if len(sys.argv) != 3:
    print("Usage: python import_database.py <db_schema_name> <data_folder_path>")
    sys.exit(1)

# Fetch database schema name from terminal input
db_schema_name = sys.argv[1]

# Fetch data folder path from terminal input
data_folder_path = sys.argv[2]

# Export the database tables to the specified folder
db.import_database_from_folder(
    engine_name = 'test',
    folder_path = data_folder_path,
    schema_name = db_schema_name,
    create_if_not_exists = True,
    replace_existing     = True,
    include_views        = True
)
