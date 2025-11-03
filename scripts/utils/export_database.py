#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.registry.graphregistry import GraphDB
import sys, os

# Initialize the GraphDB instance
db = GraphDB()

# Print usage if incorrect number of arguments
if len(sys.argv) != 3:
    print("Usage: python export_database.py <db_schema_name> <export_root_path>")
    sys.exit(1)

# Fetch database schema name from terminal input
db_schema_name = sys.argv[1]

# Fetch export path from terminal input
export_root_path = sys.argv[2]

# Export the database tables to the specified folder
db.dump_database_to_folder(
    engine_name = 'test',
    schema_name = db_schema_name,
    folder_path = export_root_path,
    filter_by   = 'TRUE',
    chunk_size  = 100000,
    include_create_tables = True,
    include_views = True
)
