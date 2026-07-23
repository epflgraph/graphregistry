# graphregistry/application/core/cor_registry.py
from graphregistry.common.auxfcn import print_dataframe, print_colour
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.common.dbstruct import DynamicSQL, GraphTable
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
from graphdb.models.sqlquery import SQLQuery, print_sql
from graphregistry.adapters.clients.elasticsearch import GraphES, es_degree_score_factors
from tqdm import tqdm
from loguru import logger as sysmsg
from copy import deepcopy
from itertools import combinations_with_replacement
from pathlib import Path
from decimal import Decimal
import numpy as np
import pandas as pd
import os, re, sys, json, datetime, itertools, gzip, os, glob, rich

#------------------------------#
# Class objects initialisation #
#------------------------------#

# Initialise config objects
glbcfg = GlobalConfig()
idxcfg =  IndexConfig()
scrcfg = ScoresConfig()

# Initialise MySQL client
# db_cfg = GraphDBConfig.from_file("config/config_db.yaml")
from graphregistry.common.paths import (
    CONFIG_DB_PATH,
    DATABASE_CONFIG_DATATYPES_PATH,
    REPO_ROOT as PROJECT_ROOT,
)
db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
db = GraphDB(config=db_cfg)

# Initialise dynamic sql object
dynsql = DynamicSQL(db=db)

# Initialise clients
es = GraphES()

#------------------------------------------------#
# Progress bar and system messages configuration #
#------------------------------------------------#

# Width of the progress bar
PBWIDTH = 92

# Set up system message handler to display TRACE messages
sysmsg.remove()
sysmsg.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line:06d}</cyan> - "
           "<level>{message}</level>",
    level="TRACE"
)

#---------------#
# Resolve paths #
#---------------#

# Resolve repository root path
REPO_ROOT = PROJECT_ROOT

# Function to resolve paths
from typing import Union
def resolve_repo_path(p: Union[str, Path]) -> Path:
    """Return an absolute path. If 'p' is relative, resolve it against the repo root."""
    p = Path(p)
    return p if p.is_absolute() else (REPO_ROOT / p)

# Resolve SQL Formulas folder path
SQL_FORMULAS_PATH = resolve_repo_path('database/formulas')

# Short aliases for formula folder names (e.g. --formula_path=traversals/... -> graph_traversals)
SQL_FORMULAS_FOLDER_ALIASES = {
    'fields':     'calculated_fields',
    'traversals': 'graph_traversals',
    'scores':     'calculated_scores',
}

# Resolve Elasticsearch export path
ELASTICSEARCH_DATA_EXPORT_PATH = resolve_repo_path(glbcfg.settings["elasticsearch"]["data_export_path"])


#------------------------------#
# Index field datatypes config #
#------------------------------#

# Fetch index field datatypes from config file
with open(DATABASE_CONFIG_DATATYPES_PATH, 'r', encoding="utf-8") as f:
    datatypes_config = json.load(f)

# SQL data type mapping dictionary
sql_data_type_mapping = {
    'char'     : 'VARCHAR(255)',
    'text'     : 'MEDIUMTEXT',
    'longtext' : 'LONGTEXT',
    'int'      : 'MEDIUMINT UNSIGNED',
    'bool'     : 'TINYINT(1)',
    'date'     : 'DATE',
    'datetime' : 'DATETIME'
}

# Define mapping from field datatypes onto "castable" types
cast_mapping = {
    "TINYINT(1)"         : "CAST(%s AS UNSIGNED)",
    "SMALLINT UNSIGNED"  : "CAST(%s AS UNSIGNED)",
    "YEAR"               : "CAST(%s AS UNSIGNED)",
    "VARCHAR(16)"        : "CAST(%s AS CHAR)",
    "VARCHAR(255)"       : "CAST(%s AS CHAR)",
    "MEDIUMTEXT"         : "CAST(%s AS CHAR)",
    "LONGTEXT"           : "CAST(%s AS CHAR)",
    "DATE"               : "CAST(%s AS DATE)",
    "DATETIME"           : "CAST(%s AS DATETIME)",
    "MEDIUMINT UNSIGNED" : "CAST(%s AS UNSIGNED)"
}

#---------------------------------------#
# Auxiliary functions for GraphRegistry #
#---------------------------------------#

# Auxiliary function: Generate Airflow where conditions
def generate_airflow_where_conditions(doc_type=None):

    # Fetch typeflags config JSON
    typeflags = GraphRegistry.Orchestration.TypeFlags()
    config_json = typeflags.get_config_json()

    # Get node types to process
    doc_types_fields_to_process     = [ r[0]        for r in config_json['nodes'] if r[1]]
    doc_types_scores_to_process     = [ r[0]        for r in config_json['nodes'] if r[2]]
    doclink_types_fields_to_process = [(r[0], r[1]) for r in config_json['edges'] if r[2]]

    # If doc_type was provided as input, remove any type from lists above that do not contain it
    if doc_type is not None:
        doc_types_fields_to_process     = [e for e in doc_types_fields_to_process     if e == doc_type]
        doc_types_scores_to_process     = [e for e in doc_types_scores_to_process     if e == doc_type]
        doclink_types_fields_to_process = [t for t in doclink_types_fields_to_process if doc_type in t]

    # Return if no types to process
    if (len(doc_types_fields_to_process)     == 0 and
        len(doc_types_scores_to_process)     == 0 and
        len(doclink_types_fields_to_process) == 0
    ):
        return None

    # Initialise WHERE contitions
    where_conditions = {
        'Operations_N_Object_T_FieldsChanged' : f"""object_type IN ({', '.join(repr(e) for e in doc_types_fields_to_process)})""" if len(doc_types_fields_to_process)>0 else "FALSE",
        'Operations_N_Object_T_ScoresExpired' : f"""object_type IN ({', '.join(repr(e) for e in doc_types_scores_to_process)})""" if len(doc_types_scores_to_process)>0 else "FALSE",
        'Operations_N_Object_N_Object_T_FieldsChanged' : f"( (from_object_type, to_object_type) IN ({', '.join(repr(t) for t in doclink_types_fields_to_process)}) OR (to_object_type, from_object_type) IN ({', '.join(repr(t) for t in doclink_types_fields_to_process)}) )" if len(doclink_types_fields_to_process)>0 else "FALSE",
    }

    # Return Airflow WHERE conditions
    return where_conditions

# Auxiliary function: Get scores matrix table name from edge type tuple
def get_scores_matrix_table_name(from_object_type, to_object_type, gbc_or_as):

    #-------------------------#
    # Ontology related tuples #
    #-------------------------#
    if from_object_type in ('Category','Concept','Curated area') or to_object_type in ('Category','Concept','Curated area'):

        # Group-by-concepts table?
        if gbc_or_as.upper()=='GBC':
            table_name = "Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC"
            return table_name

        # Adjusted scores table?
        elif gbc_or_as.upper()=='AS':
            table_name = "Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS"
            return table_name

    #-----------------------------#
    # Non-ontology related tuples #
    #-----------------------------#
    else:

        # Sorted tuple (convention in table)
        sorted_tuple = tuple(sorted([from_object_type, to_object_type]))

        # Generate table name if inputs are correct
        if sorted_tuple in scrcfg.settings['scored_edge_tuple_to_class_mapping'] and gbc_or_as.upper() in ('GBC','AS'):
            research_or_education = scrcfg.settings['scored_edge_tuple_to_class_mapping'][sorted_tuple]
            if research_or_education not in ['education', 'research', 'ontology']:
                return None
            table_name = f"Edges_N_Object_N_Object_T_ScoresMatrix_{research_or_education.title()}_{gbc_or_as.upper()}"
            return table_name
        else:
            raise ValueError(f"Invalid input: ({from_object_type}, {to_object_type}, {gbc_or_as}). No corresponding scores matrix table found.")

# Auxiliary function: Check if table exists and create it if not exists
def create_table_if_not_exists(engine_name, schema_name, table_name):

    # Check if table exists
    if not db.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name):

        # Display warning
        sysmsg.warning(f"Target table '{schema_name}.{table_name}' does not exist. Creating table ...")

        # Create table
        tb = GraphTable(db=db, schema_name=schema_name, table_name=table_name)
        db.execute_query_in_shell(engine_name=engine_name, query=tb.create_table_sql, verbose=False, query_id='v29zYeaA')

        # Check if table was created successfully
        if db.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
            sysmsg.trace("☑️ Table created successfully.")
        else:
            sysmsg.critical(f"❌ Failed to create table '{schema_name}.{table_name}'.")
            exit()

#==================================#
# Class definition: Graph Registry #
#==================================#
class GraphRegistry():

    # Class variable to hold the single instance
    _instance = None

    # Create new instance of class before __init__ is called
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    # Constructor
    def __init__(self, name="GraphRegistry", print_config=False):

        # Check if the instance is already initialized
        if not self._initialized:
            self.name = name
            self._initialized = True
            # print(f"GraphRegistry initialized with name: {self.name}")

        # Initialize all children objects
        # db = GraphDB()
        # self.idx = GraphIndex()
        self.orchestrator = self.Orchestration()
        self.cachemanager = self.CacheManagement()
        self.indexdb = self.IndexDB()
        self.indexes = self.IndexES()

        # Print configuration if requested
        if print_config:
            idxcfg.print(compact=True)
            scrcfg.print()

    # Import data from JSON data
    def import_from_json(self, json_data, skip_concept_detection=False):
        """
        Import data from JSON data into the graph registry.
        The JSON data should contain nodes and edges in the required format:
            json_data = {
                "nodes": [node1, node2, ...],
                "edges": [edge1, edge2, ...]
            }
        """

        node_list = self.NodeList()
        node_list.set_from_json(doc_json_list=json_data['nodes'])
        node_list.commit(actions=('eval'))

        edge_list = self.EdgeList()
        edge_list.set_from_json(doc_json_list=json_data['edges'])
        edge_list.commit(actions=('eval'))

    # Import data from JSON file
    def import_from_file(self, json_file, skip_concept_detection=False):
        """
        Import data from a JSON file into the graph registry.
        The JSON file should contain nodes and edges in the required format:
            {
                "nodes": [node1, node2, ...],
                "edges": [edge1, edge2, ...]
            }
        """

        # Load JSON data from file
        with open(json_file, 'r') as f:
            json_data = json.load(f)

        # Call the import_from_json method
        self.import_from_json(json_data=json_data, skip_concept_detection=skip_concept_detection)

    #--------------------------------------------------#
    # Subclass definition: GraphRegistry Orchestration #
    #--------------------------------------------------#
    class Orchestration():

        # Class constructor
        def __init__(self):
            # db = GraphDB()
            self.typeflags = self.TypeFlags()
            self.fieldschanged = self.FieldsChanged()
            self.scoresexpired = self.ScoresExpired()

        # Print current settings (fields and scores)
        def status(self):
            self.typeflags.status()
            self.fieldschanged.status()
            self.scoresexpired.status()

        # Reset airflow and chache flags
        # Options: ('typeflags', 'airflow', 'cache', 'traversals')
        def reset(self, options=(), doc_type=None, verbose=False):

            # Print status
            sysmsg.info("🧹 📝 Reset 'to_process' flags to 0.")

            # Print input parameters
            if len(options) > 0:
                sysmsg.trace(f"Selected option(s): {options}.")
            else:
                sysmsg.warning("Nothing to do: 'options' parameter missing.")
                sysmsg.warning("options : 'typeflags', 'airflow', 'cache', 'traversals'")

            # Reset types
            if 'typeflags' in options:
                self.typeflags.reset()

            # Reset flags on graph_airflow
            if 'airflow' in options:

                # Print status
                sysmsg.info("🧹 📝 Reset 'to_process' flags in graph_airflow tables.")

                # Get list of tables in 'graph_airflow' schema to process
                list_of_tables = [
                    (glbcfg.schema_airflow, 'Operations_N_Object_N_Object_T_FieldsChanged'),
                    (glbcfg.schema_airflow, 'Operations_N_Object_T_FieldsChanged'),
                    (glbcfg.schema_airflow, 'Operations_N_Object_T_ScoresExpired')
                ]

                # Print list of affected tables
                print('\nThe following tables will be affected:')
                for s,t in list_of_tables:
                    print(f" - {s}.{t}")
                print('')

                # Print status
                sysmsg.trace(f"Processing '{glbcfg.schema_airflow}' fields and scores tables ...")

                # Loop over 'graph_airflow' tables
                with tqdm(list_of_tables, unit='table') as pb:
                    for schema_name, table_name in pb:
                        pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                        db.execute_query_in_shell(engine_name = 'xaas_coresrv', 
                            query = f"UPDATE {schema_name}.{table_name} SET to_process = 0 WHERE to_process = 1;"
                        , query_id='5LEjczg5', verbose=verbose)

                # Print status
                sysmsg.success(f"🧹 ✅ Done resetting 'to_process' flags in '{glbcfg.schema_airflow}' tables.")

            # Reset flags on graph_cache
            if 'cache' in options:

                # Print status
                sysmsg.info("🧹 📝 Reset 'to_process' flags in graph_cache tables.")

                # Get list of tables in 'graph_cache' schema containing 'to_process' column
                list_of_tables = sorted([(glbcfg.schema_graph_cache_test, table_name)
                    for table_name in db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test)
                    if not table_name.startswith('_')
                    and db.has_column(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=table_name, column_name='to_process')])

                # Print list of affected tables
                print('\nThe following tables will be affected:')
                for s,t in list_of_tables:
                    print(f" - {s}.{t}")
                print('')

                # Print status
                sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' fields and scores tables ...")

                # Loop over 'graph_cache' tables
                with tqdm(list_of_tables, unit='table') as pb:
                    for schema_name, table_name in pb:
                        pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                        db.execute_query_in_shell(engine_name = 'xaas_coresrv',
                            query    = f"UPDATE {schema_name}.{table_name} SET to_process = 0 WHERE to_process = 1;",
                            query_id = 'DFEkXX4A',
                            verbose  = verbose
                        )

                # Print status
                sysmsg.success(f"🧹 ✅ Done resetting 'to_process' flags in '{glbcfg.schema_graph_cache_test}' tables.")

            # Reset flags on traversals
            if 'traversals' in options:

                # Print status
                sysmsg.info("🧹 📝 Reset 'to_process' flags in traversals tables.")

                # Get list of tables in 'traversals' schema containing 'to_process' column
                list_of_tables = sorted([(glbcfg.schema_traversals, table_name)
                    for table_name in db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=glbcfg.schema_traversals)
                    if not table_name.startswith('_')
                    and db.has_column(engine_name='xaas_coresrv', schema_name=glbcfg.schema_traversals, table_name=table_name, column_name='to_process')])

                # Print list of affected tables
                print('\nThe following tables will be affected:')
                for s,t in list_of_tables:
                    print(f" - {s}.{t}")
                print('')

                # Print status
                sysmsg.trace(f"Processing '{glbcfg.schema_traversals}' traversals tables ...")

                # Loop over 'traversals' tables
                with tqdm(list_of_tables, unit='table') as pb:
                    for schema_name, table_name in pb:
                        pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                        db.execute_query_in_shell(engine_name = 'xaas_coresrv',
                            query    = f"UPDATE {schema_name}.{table_name} SET to_process = 0 WHERE to_process = 1;",
                            query_id = 'X7vYqZ3A',
                            verbose  = verbose
                        )

                # Print status
                sysmsg.success(f"🧹 ✅ Done resetting 'to_process' flags in '{glbcfg.schema_traversals}' tables.")

            # Print status
            sysmsg.success("🧹 ✅ Done resetting flags.\n")

        # Propagate flags to cache tables
        def propagate(self):

            # Print status
            sysmsg.info("⛳️ 📝 Propagate 'to_process' flags throughout the cache.")

            # Build list for updates in nodes and data tables
            list_of_tables = [
                (glbcfg.schema_graph_cache_test, 'Data_N_Object_T_PageProfile'),
                (glbcfg.schema_graph_cache_test, 'Nodes_N_Object_T_DegreeScores')
            ]

            # Print status
            sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' page profile and degree scores tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    db.execute_query_in_shell(engine_name = 'xaas_coresrv', 
                        query = f"""UPDATE {schema_name}.{table_name} p
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged fc
                                     USING (object_type, object_id)
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                                     USING (object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process = 1
                                       AND tf.to_process = 1
                                       AND  p.to_process = 0;
                        """
                    , query_id='zv9J4K0r', verbose=False)

            # Build list for updates in edge tables
            list_of_tables = [
                (glbcfg.schema_graph_cache_test, 'Edges_N_Object_N_Object_T_ParentChildSymmetric')
            ]

            # Print status
            sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' parent-child tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    for d1,d2 in [('from', 'to'), ('to', 'from')]:
                        db.execute_query_in_shell(engine_name = 'xaas_coresrv', 
                            query = f"""UPDATE {schema_name}.{table_name} p
                                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged AS fc
                                            ON ( p.{d1}_object_type,  p.{d1}_object_id, p.{d2}_object_type, p.{d2}_object_id)
                                             = (fc.from_object_type, fc.from_object_id,  fc.to_object_type,  fc.to_object_id)
                                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                            ON ( p.{d1}_object_type, p.{d2}_object_type)
                                             = (tf.from_object_type,  tf.to_object_type)
                                           SET  p.to_process = 1
                                         WHERE fc.to_process = 1
                                           AND tf.to_process = 1
                                           AND  p.to_process = 0;
                            """
                        , query_id='ct6y8Gz2', verbose=False)

           # Build list for updates in edge tables
            list_of_tables = [
                (glbcfg.schema_graph_cache_test, 'Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS'),
                (glbcfg.schema_graph_cache_test, 'Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS'),
                (glbcfg.schema_graph_cache_test, 'Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC'),
                (glbcfg.schema_graph_cache_test, 'Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC')
            ]

            # Print status
            sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' score matrix tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    for d in ['from', 'to']:
                        db.execute_query_in_shell(engine_name = 'xaas_coresrv', 
                            query = f"""UPDATE {schema_name}.{table_name} p
                                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired AS se
                                            ON (p.{d}_object_type, p.{d}_object_id) = (se.object_type, se.object_id)
                                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                            ON ( p.from_object_type,  p.to_object_type)
                                             = (tf.from_object_type, tf.to_object_type)
                                           SET  p.to_process = 1
                                         WHERE se.to_process = 1
                                           AND tf.to_process = 1
                                           AND  p.to_process = 0;
                            """
                        , query_id='yzm93BqQ', verbose=False)

            # Print status
            sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' IndexBuildup Doc tables ...")

            # Fetch list from index config
            list_of_doc_types = idxcfg.settings['doc_types']

            # Propagate flags on index buildup tables
            with tqdm(list_of_doc_types, unit='doc type') as pb:
                for dummy, doc_type in pb:
                    pb.set_description(f"⚙️  Doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    db.execute_query_in_shell(engine_name = 'xaas_coresrv',
                        query = f"""UPDATE {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{doc_type} p
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged fc
                                        ON (p.doc_type, p.doc_id) = (fc.object_type, fc.object_id)
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                                     USING (object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process = 1
                                       AND tf.to_process = 1
                                       AND  p.to_process = 0;
                        """)

            # Print status
            sysmsg.trace(f"Processing '{glbcfg.schema_graph_cache_test}' IndexBuildup Doc-Link tables ...")

            # Fetch list from index config
            list_of_p2c_doclink_types = list(set([sorted([d, l])
                for d in idxcfg.settings['graphsearch']['fields']['links']['parent_child']
                for l in idxcfg.settings['graphsearch']['fields']['links']['parent_child'][d]]))

            # Propagate flags on index buildup tables
            with tqdm(list_of_p2c_doclink_types, unit='doc-link type') as pb:
                for source_doc_type, target_doc_type in pb:
                    pb.set_description(f"⚙️  Doc-link type: {source_doc_type}-{target_doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    db.execute_query_in_shell(engine_name='xaas_coresrv',
                        query = f"""UPDATE {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Links_ParentChild_{source_doc_type}_{target_doc_type} p
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged AS fc
                                        ON (p.doc_type, p.doc_id, p.link_type, p.link_id)
                                         = (fc.from_object_type, fc.from_object_id, fc.to_object_type, fc.to_object_id)
                                INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                        ON (p.doc_type, p.link_type)
                                         = (tf.from_object_type, tf.to_object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process = 1
                                       AND tf.to_process = 1
                                       AND  p.to_process = 0;
                        """
                    , query_id='J4Djz3fW', verbose=False)

            # # Truncate table: Operations/ Object / ToProcess
            # db.execute_query_in_shell(engine_name='xaas_coresrv', query=f"TRUNCATE TABLE {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_ToProcess;")

            # Print status
            sysmsg.success("⛳️ ✅ All 'to_process' flags have been propagated throughout cache.\n")

        # Sync new objects to operations table
        def sync(self, to_process=1, verbose=False):
            self.fieldschanged.sync(to_process=to_process, verbose=verbose)
            self.scoresexpired.sync(to_process=to_process, verbose=verbose)

        # Randomize airflow fields [OPTIONAL: For testing purposes]
        def randomize(self, doc_type=None, time_period=182, verbose=False):
            self.fieldschanged.randomize(doc_type=doc_type, time_period=time_period, verbose=verbose)
            self.scoresexpired.randomize(doc_type=doc_type, time_period=time_period, verbose=verbose)

        # Set expiration dates
        def expire(self, doc_type=None, older_than=None, limit_per_type=None, count_only=False, verbose=False):

            # Apply defaults
            older_than = older_than if older_than!=None else 90
            limit_per_type = limit_per_type if limit_per_type!=None else 100

            # Call expire functions for both 'fields changed' and 'scores expired' flag types
            self.fieldschanged.expire(doc_type=doc_type, older_than=older_than, limit_per_type=limit_per_type, count_only=count_only, verbose=verbose)
            self.scoresexpired.expire(doc_type=doc_type, older_than=older_than, limit_per_type=limit_per_type, count_only=count_only, verbose=verbose)

        # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
        def refresh(self, doc_type=None, refresh_checksums=False, limit_per_type=None, verbose=False):
            self.fieldschanged.refresh(doc_type=doc_type, refresh_checksums=refresh_checksums, limit_per_type=limit_per_type, verbose=verbose)
            self.scoresexpired.refresh(doc_type=doc_type, limit_per_type=limit_per_type, verbose=verbose)

        # Rollover checksums (replace previous one with current)
        def rollover(self, doc_type=None, actions=('eval',)):
            self.fieldschanged.rollover(doc_type=doc_type, actions=actions)

        # Update last_date_cached values
        def update_dates(self, doc_type=None, actions=('eval',)):
            self.fieldschanged.update_dates(doc_type=doc_type, actions=actions)
            self.scoresexpired.update_dates(doc_type=doc_type, actions=actions)

        # Update object checksums based on typeflag activation
        def update_checksums_v2(self, actions=('commit',), verbose=False):

            # Print status
            sysmsg.info("🧩 📝 Update object checksums based on typeflag activation.")

            # Get typeflags to process
            obj_types_to_process, obj2obj_types_to_process = self.typeflags.get_types_to_process(fields_or_scores='fields')

            # Serialize the latest
            obj2obj_types_to_process_serial = {x for pair in obj2obj_types_to_process for x in pair}

            #========================#
            # Node related checksums #
            #========================#

            #--------------------------#
            # General object checksums #
            #--------------------------#

            # Loop over registry and lectures schemas
            for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures]:

                # Check if there's something to process based on typeflags
                if len(set(obj_types_to_process) & set(glbcfg.schema_to_object_types[schema_name]))==0:
                    sysmsg.trace(f"➡️ Skipping calculation: Object > General registry > {schema_name}")
                    continue

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object > General registry > {schema_name} ...")

                # Generate SQL query (Object checksum > All non-ontology types)
                sql_query = f"""
                              SELECT object_type, object_id,
                                     MD5(CONCAT(MD5(COALESCE(object_type, "__null__")), MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(object_title, "__null__")), MD5(COALESCE(text_source, "__null__")), MD5(COALESCE(raw_text, "__null__")))) AS checksum_val
                                 FROM {schema_name}.Nodes_N_Object o
                           INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                                USING (object_type)
                                WHERE object_type NOT IN ('Slide', 'Transcript')
                                  AND t.flag_type = 'fields'
                                  AND t.to_process = 1
                 """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='VBk3hp3Z')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_T_ChecksumsObject',
                    query=sql_query,
                    key_column_names=['object_type', 'object_id'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='VBk3hp3Z'
                )

            # Ontology tables exception
            # Check if there's something to process based on typeflags
            if len(set(obj_types_to_process) & set(glbcfg.schema_to_object_types[glbcfg.schema_ontology]))==0:
                sysmsg.trace(f"➡️ Skipping calculation: Object > General registry > {glbcfg.schema_ontology}")
            else:

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object > General registry > {glbcfg.schema_ontology} ...")

                # Generate SQL query (Object checksum > Concept)
                sql_query = f"""
                          SELECT object_type, object_id,
                                 MD5(CONCAT(MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(name, "__null__")), MD5(COALESCE(is_ontology_category, "__null__")), MD5(COALESCE(is_ontology_concept, "__null__")), MD5(COALESCE(is_ontology_neighbour, "__null__")), MD5(COALESCE(is_noise, "__null__")), MD5(COALESCE(is_unused, "__null__")))) AS checksum_val
                            FROM {glbcfg.schema_ontology}.Nodes_N_Concept o
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                           WHERE t.flag_type = 'fields'
                             AND t.to_process = 1
                 """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='CmNTYc97')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_T_ChecksumsObject',
                    query=sql_query,
                    key_column_names=['object_type', 'object_id'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='CmNTYc97'
                )

                # Generate SQL query (Object checksum > Category)
                sql_query = f"""
                          SELECT object_type, object_id,
                                 MD5(CONCAT(MD5(COALESCE(object_id, "__null__")), MD5(COALESCE(name, "__null__")), MD5(COALESCE(depth, "__null__")), MD5(COALESCE(reference_page_id, "__null__")), MD5(COALESCE(reference_page_key, "__null__")), MD5(COALESCE(reference_page_url, "__null__")))) AS checksum_val
                            FROM {glbcfg.schema_ontology}.Nodes_N_Category o
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                           WHERE t.flag_type = 'fields'
                             AND t.to_process = 1
                 """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='XUiwHdd6')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_T_ChecksumsObject',
                    query=sql_query,
                    key_column_names=['object_type', 'object_id'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='XUiwHdd6'
                )

            #------------------------#
            # Page profile checksums #
            #------------------------#

            # Loop over registry, lectures, and ontology schemas
            for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                # Check if there's something to process based on typeflags
                if len(set(obj_types_to_process) & set(glbcfg.schema_to_object_types[schema_name]))==0:
                    sysmsg.trace(f"➡️ Skipping calculation: Object > Page profile > {schema_name}")
                    continue

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object > Page profile > {schema_name} ...")

                # Generate SQL query (Page profile checksum > All types)
                sql_query = f"""
                              SELECT object_type, object_id,
                                     MD5(CONCAT(MD5(COALESCE(numeric_id_en, "__null__")), MD5(COALESCE(numeric_id_fr, "__null__")), MD5(COALESCE(numeric_id_de, "__null__")), MD5(COALESCE(numeric_id_it, "__null__")), MD5(COALESCE(short_code, "__null__")), MD5(COALESCE(subtype_en, "__null__")), MD5(COALESCE(subtype_fr, "__null__")), MD5(COALESCE(subtype_de, "__null__")), MD5(COALESCE(subtype_it, "__null__")), MD5(COALESCE(name_en_is_auto_generated, "__null__")), MD5(COALESCE(name_en_is_auto_corrected, "__null__")), MD5(COALESCE(name_en_is_auto_translated, "__null__")), MD5(COALESCE(name_en_translated_from, "__null__")), MD5(COALESCE(name_en_value, "__null__")), MD5(COALESCE(name_fr_is_auto_generated, "__null__")), MD5(COALESCE(name_fr_is_auto_corrected, "__null__")), MD5(COALESCE(name_fr_is_auto_translated, "__null__")), MD5(COALESCE(name_fr_translated_from, "__null__")), MD5(COALESCE(name_fr_value, "__null__")), MD5(COALESCE(name_de_is_auto_generated, "__null__")), MD5(COALESCE(name_de_is_auto_corrected, "__null__")), MD5(COALESCE(name_de_is_auto_translated, "__null__")), MD5(COALESCE(name_de_translated_from, "__null__")), MD5(COALESCE(name_de_value, "__null__")), MD5(COALESCE(name_it_is_auto_generated, "__null__")), MD5(COALESCE(name_it_is_auto_corrected, "__null__")), MD5(COALESCE(name_it_is_auto_translated, "__null__")), MD5(COALESCE(name_it_translated_from, "__null__")), MD5(COALESCE(name_it_value, "__null__")), MD5(COALESCE(description_short_en_is_auto_generated, "__null__")), MD5(COALESCE(description_short_en_is_auto_corrected, "__null__")), MD5(COALESCE(description_short_en_is_auto_translated, "__null__")), MD5(COALESCE(description_short_en_translated_from, "__null__")), MD5(COALESCE(description_short_en_value, "__null__")), MD5(COALESCE(description_short_fr_is_auto_generated, "__null__")), MD5(COALESCE(description_short_fr_is_auto_corrected, "__null__")), MD5(COALESCE(description_short_fr_is_auto_translated, "__null__")), MD5(COALESCE(description_short_fr_translated_from, "__null__")), MD5(COALESCE(description_short_fr_value, "__null__")), MD5(COALESCE(description_short_de_is_auto_generated, "__null__")), MD5(COALESCE(description_short_de_is_auto_corrected, "__null__")), MD5(COALESCE(description_short_de_is_auto_translated, "__null__")), MD5(COALESCE(description_short_de_translated_from, "__null__")), MD5(COALESCE(description_short_de_value, "__null__")), MD5(COALESCE(description_short_it_is_auto_generated, "__null__")), MD5(COALESCE(description_short_it_is_auto_corrected, "__null__")), MD5(COALESCE(description_short_it_is_auto_translated, "__null__")), MD5(COALESCE(description_short_it_translated_from, "__null__")), MD5(COALESCE(description_short_it_value, "__null__")), MD5(COALESCE(description_medium_en_is_auto_generated, "__null__")), MD5(COALESCE(description_medium_en_is_auto_corrected, "__null__")), MD5(COALESCE(description_medium_en_is_auto_translated, "__null__")), MD5(COALESCE(description_medium_en_translated_from, "__null__")), MD5(COALESCE(description_medium_en_value, "__null__")), MD5(COALESCE(description_medium_fr_is_auto_generated, "__null__")), MD5(COALESCE(description_medium_fr_is_auto_corrected, "__null__")), MD5(COALESCE(description_medium_fr_is_auto_translated, "__null__")), MD5(COALESCE(description_medium_fr_translated_from, "__null__")), MD5(COALESCE(description_medium_fr_value, "__null__")), MD5(COALESCE(description_medium_de_is_auto_generated, "__null__")), MD5(COALESCE(description_medium_de_is_auto_corrected, "__null__")), MD5(COALESCE(description_medium_de_is_auto_translated, "__null__")), MD5(COALESCE(description_medium_de_translated_from, "__null__")), MD5(COALESCE(description_medium_de_value, "__null__")), MD5(COALESCE(description_medium_it_is_auto_generated, "__null__")), MD5(COALESCE(description_medium_it_is_auto_corrected, "__null__")), MD5(COALESCE(description_medium_it_is_auto_translated, "__null__")), MD5(COALESCE(description_medium_it_translated_from, "__null__")), MD5(COALESCE(description_medium_it_value, "__null__")), MD5(COALESCE(description_long_en_is_auto_generated, "__null__")), MD5(COALESCE(description_long_en_is_auto_corrected, "__null__")), MD5(COALESCE(description_long_en_is_auto_translated, "__null__")), MD5(COALESCE(description_long_en_translated_from, "__null__")), MD5(COALESCE(description_long_en_value, "__null__")), MD5(COALESCE(description_long_fr_is_auto_generated, "__null__")), MD5(COALESCE(description_long_fr_is_auto_corrected, "__null__")), MD5(COALESCE(description_long_fr_is_auto_translated, "__null__")), MD5(COALESCE(description_long_fr_translated_from, "__null__")), MD5(COALESCE(description_long_fr_value, "__null__")), MD5(COALESCE(description_long_de_is_auto_generated, "__null__")), MD5(COALESCE(description_long_de_is_auto_corrected, "__null__")), MD5(COALESCE(description_long_de_is_auto_translated, "__null__")), MD5(COALESCE(description_long_de_translated_from, "__null__")), MD5(COALESCE(description_long_de_value, "__null__")), MD5(COALESCE(description_long_it_is_auto_generated, "__null__")), MD5(COALESCE(description_long_it_is_auto_corrected, "__null__")), MD5(COALESCE(description_long_it_is_auto_translated, "__null__")), MD5(COALESCE(description_long_it_translated_from, "__null__")), MD5(COALESCE(description_long_it_value, "__null__")), MD5(COALESCE(external_key_en, "__null__")), MD5(COALESCE(external_key_fr, "__null__")), MD5(COALESCE(external_key_de, "__null__")), MD5(COALESCE(external_key_it, "__null__")), MD5(COALESCE(external_url_en, "__null__")), MD5(COALESCE(external_url_fr, "__null__")), MD5(COALESCE(external_url_de, "__null__")), MD5(COALESCE(external_url_it, "__null__")), MD5(COALESCE(is_visible, "__null__")))) AS checksum_val
                                FROM {schema_name}.Data_N_Object_T_PageProfile p
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                               USING (object_type)
                               WHERE object_type NOT IN ('Slide', 'Transcript')
                                 AND t.flag_type = 'fields'
                                 AND t.to_process = 1
                 """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='5nVWX6nk')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_T_ChecksumsPageProfile',
                    query=sql_query,
                    key_column_names=['object_type', 'object_id'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='5nVWX6nk'
                )

            #-------------------------#
            # Custom fields checksums #
            #-------------------------#

            # Loop over registry, lectures, and ontology schemas
            for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                # Check if there's something to process based on typeflags
                if len(set(obj_types_to_process) & set(glbcfg.schema_to_object_types[schema_name]))==0:
                    sysmsg.trace(f"➡️ Skipping calculation: Object > Custom fields > {schema_name}")
                    continue

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object > Custom fields > {schema_name} ...")

                # Generate SQL query (Custom fields checksum > All types)
                sql_query = f"""
                          SELECT object_type, object_id,
                                 MD5(GROUP_CONCAT(MD5(CONCAT(
                                    MD5(COALESCE(field_language, "__null__")), MD5(COALESCE(field_name, "__null__")), MD5(COALESCE(field_value, "__null__"))
                                 )) ORDER BY field_language, field_name, field_value)) AS checksum_val
                            FROM {schema_name}.Data_N_Object_T_CustomFields c
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                           WHERE object_type NOT IN ('Slide', 'Transcript')
                             AND t.flag_type = 'fields'
                             AND t.to_process = 1
                         GROUP BY object_type, object_id
                 """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='oTWu6bBL')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_T_ChecksumsCustomFields',
                    query=sql_query,
                    key_column_names=['object_type', 'object_id'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='oTWu6bBL'
                )

            #------------------------#
            # Final object checksums #
            #------------------------#

            # Print status
            sysmsg.trace(f"⚙️ Processing checksums: Object > Final checksums ...")

            # Generate SQL query (Final checksum > All types)
            sql_query = f"""
                      SELECT object_type, o.object_id,
                             MD5(CONCAT(COALESCE(o.checksum_val, "__null__"), COALESCE(p.checksum_val, "__null__"), COALESCE(c.checksum_val, "__null__"))) AS checksum_val
                        FROM {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_ChecksumsObject o
                   LEFT JOIN {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_ChecksumsPageProfile p
                       USING (object_type, object_id)
                   LEFT JOIN {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_ChecksumsCustomFields c
                       USING (object_type, object_id)
                   INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                        USING (object_type)
                        WHERE t.flag_type = 'fields'
                          AND t.to_process = 1
             """

            # Upsert computed checksums into target table
            db.execute_query_as_safe_inserts(
                engine_name='xaas_coresrv',
                schema_name=glbcfg.schema_graph_cache_test,
                table_name='Operations_N_Object_T_Checksums',
                query=sql_query,
                key_column_names=['object_type', 'object_id'],
                upd_column_names=['checksum_val'],
                eval_column_names=['object_type'],
                actions=actions,
                verbose=verbose,
                query_id='y0yFAafh'
            )

            #----------------------------------#
            # Apply checksums to Airflow table #
            #----------------------------------#

            if 'commit' in actions:

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object > Applying to Airflow ...")

                # Generate SQL query
                sql_query = f"""
                          UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged f
                      INNER JOIN {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_Checksums c
                           USING (object_type, object_id)
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                           USING (object_type)
                             SET f.checksum_current = c.checksum_val
                           WHERE t.flag_type = 'fields'
                             AND t.to_process = 1
                """

                # Execute query in shell
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='j65waWD2')

            # Print status
            sysmsg.trace(f"☑️ Done processing checksums for Object.")

            #=========================#
            # Edges related checksums #
            #=========================#

            #------------------------------------#
            # General object-to-object checksums #
            #------------------------------------#

            # Loop over registry and lectures schemas
            for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                # Check if there's something to process based on typeflags
                if len(set(obj2obj_types_to_process_serial) & set(glbcfg.schema_to_object_types[schema_name]))==0:
                    sysmsg.trace(f"➡️ Skipping calculation: Object-to-Object > General registry > {schema_name}")
                    continue

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > General registry > {schema_name} ...")

                # Generate SQL query (Object-to-object checksum > All types)
                sql_query = f"""
                          SELECT from_object_type, from_object_id, to_object_type, to_object_id, context,
                                 MD5(CONCAT(
                                    MD5(COALESCE(from_object_type, "__null__")), MD5(COALESCE(from_object_id, "__null__")),
                                    MD5(COALESCE(  to_object_type, "__null__")), MD5(COALESCE(  to_object_id, "__null__")),
                                    MD5(COALESCE(`context`, "__null__"))
                                 )) AS checksum_val
                            FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent e
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                           WHERE to_process = 1
                """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='LnzeNnx1')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_N_Object_T_ChecksumsObject',
                    query=sql_query,
                    key_column_names=['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['from_object_type', 'to_object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='LnzeNnx1'
                )

            #---------------------------------------------#
            # Custom fields in object-to-object checksums #
            #---------------------------------------------#

            # Loop over registry and lectures schemas
            for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                # Check if there's something to process based on typeflags
                if len(set(obj2obj_types_to_process_serial) & set(glbcfg.schema_to_object_types[schema_name]))==0:
                    sysmsg.trace(f"➡️ Skipping calculation: Object-to-Object > Custom fields > {schema_name}")
                    continue

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > Custom fields > {schema_name} ...")

                # Generate SQL query (Object-to-object custom fields checksum > All types)
                sql_query = f"""
                          SELECT from_object_type, from_object_id, to_object_type, to_object_id, context,
                                 MD5(GROUP_CONCAT(MD5(CONCAT(
                                    MD5(COALESCE(field_language, "__null__")), MD5(COALESCE(field_name, "__null__")), MD5(COALESCE(field_value, "__null__"))
                                 )) ORDER BY field_language, field_name, field_value)) AS checksum_val
                            FROM {schema_name}.Data_N_Object_N_Object_T_CustomFields c
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                           WHERE to_process = 1
                         GROUP BY from_object_type, from_object_id, to_object_type, to_object_id, context
                """

                # Print query if verbose
                if verbose:
                    print_sql(sql_query, title='WZ4gEw01')

                # Upsert computed checksums into target table
                db.execute_query_as_safe_inserts(
                    engine_name='xaas_coresrv',
                    schema_name=glbcfg.schema_graph_cache_test,
                    table_name='Operations_N_Object_N_Object_T_ChecksumsCustomFields',
                    query=sql_query,
                    key_column_names=['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                    upd_column_names=['checksum_val'],
                    eval_column_names=['from_object_type', 'to_object_type'],
                    actions=actions,
                    verbose=verbose,
                    query_id='WZ4gEw01'
                )

            #----------------------------------#
            # Final object-to-object checksums #
            #----------------------------------#

            # Print status
            sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > Final checksums ...")

            # Generate SQL query (Final checksum > All types)
            sql_query = f"""
                      SELECT o.from_object_type, o.from_object_id, o.to_object_type, o.to_object_id, o.context,
                             MD5(CONCAT(COALESCE(o.checksum_val, "__null__"), COALESCE(c.checksum_val, "__null__"))) AS checksum_val
                        FROM {glbcfg.schema_graph_cache_test}.Operations_N_Object_N_Object_T_ChecksumsObject o
                   LEFT JOIN {glbcfg.schema_graph_cache_test}.Operations_N_Object_N_Object_T_ChecksumsCustomFields c
                       USING (from_object_type, from_object_id, to_object_type, to_object_id, context)
                  INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags t
                       USING (from_object_type, to_object_type)
                       WHERE to_process = 1
            """

            # Upsert computed checksums into target table
            db.execute_query_as_safe_inserts(
                engine_name='xaas_coresrv',
                schema_name=glbcfg.schema_graph_cache_test,
                table_name='Operations_N_Object_N_Object_T_Checksums',
                query=sql_query,
                key_column_names=['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context'],
                upd_column_names=['checksum_val'],
                eval_column_names=['from_object_type', 'to_object_type'],
                actions=actions,
                verbose=verbose,
                query_id='JJQ2pj3y'
            )

            #----------------------------------#
            # Apply checksums to Airflow table #
            #----------------------------------#

            if 'commit' in actions:

                # Print status
                sysmsg.trace(f"⚙️ Processing checksums: Object-to-Object > Applying to Airflow ...")

                # Generate SQL query
                sql_query = f"""
                          UPDATE {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged f
                      INNER JOIN {glbcfg.schema_graph_cache_test}.Operations_N_Object_N_Object_T_Checksums c
                           USING (from_object_type, from_object_id, to_object_type, to_object_id, context)
                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags t
                           USING (from_object_type, to_object_type)
                             SET f.checksum_current = c.checksum_val
                           WHERE t.to_process = 1
                """

                # Execute query in shell
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='Fpas6ysH')

            # Print status
            sysmsg.trace(f"☑️ Done processing checksums for Object-to-Object.")

            #----------------------------------#

            # Print status
            sysmsg.info("🧩 ✅ Done updating object checksums.")

        # === Object Type Flags ===
        class TypeFlags():

            # Class constructor
            def __init__(self):
                pass
                # db = GraphDB()

            # Print type flags
            def status(self):

                # Print object type flags
                out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                    SELECT object_type, flag_type, to_process
                      FROM {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags
                     WHERE to_process = 1
                  ORDER BY object_type, flag_type;
                """, query_id='Ghx1Go9x')
                df = pd.DataFrame(out, columns=['object_type', 'flag_type', 'to_process'])
                if not df.empty:
                    print_dataframe(df, title='⛳️ TYPE FLAGS: Object')

                # Print object-to-object type flags
                out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                    SELECT from_object_type, to_object_type, to_process
                      FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags
                     WHERE to_process = 1
                  ORDER BY from_object_type, to_object_type;
                """, query_id='NRWbEw5o')
                df = pd.DataFrame(out, columns=['from_object_type', 'to_object_type', 'to_process'])
                if not df.empty:
                    print_dataframe(df, title='⛳️ TYPE FLAGS: Object-to-Object')

            # Set type flags (1 key only)
            def set(self, object_type_key, flag_type=None, to_process=None, verbose=False):

                # Check object_type_key input
                if not isinstance(object_type_key, tuple) or len(object_type_key) not in [1, 2]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 1 or 2.")
                    return

                # Check flag_type input
                if flag_type is not None and flag_type not in ['fields', 'scores']:
                    sysmsg.error("Invalid flag_type. It should be either 'fields' or 'scores'.")
                    return

                # Check to_process input
                if to_process is not None and to_process not in [0, 1]:
                    sysmsg.error("Invalid to_process. It should be 0 or 1.")
                    return

                # Set object type flags
                db.set_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = f"Operations_N_Object{'_N_Object' if len(object_type_key)==2 else ''}_T_TypeFlags",
                    set         = [('to_process', to_process)],
                    where       = [
                                    ('object_type'   , object_type_key[0]),
                                    ('flag_type'     , flag_type)
                                  ] if len(object_type_key)==1 else [
                                    ('from_object_type'   , object_type_key[0]),
                                    ('to_object_type'     , object_type_key[1])
                                  ],
                    verbose = verbose)

            # Get type flags (1 key only)
            def get(self, object_type_key, flag_type=False, verbose=False):

                # Check object_type_key input
                if not isinstance(object_type_key, tuple) or len(object_type_key) not in [1, 2]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 1 or 2.")
                    return

                # Check flag_type input
                if flag_type is not None and flag_type not in ['fields', 'scores']:
                    sysmsg.error("Invalid flag_type. It should be either 'fields' or 'scores'.")
                    return

                # Get object type flags
                output = db.get_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = f"Operations_N_Object{'_N_Object' if len(object_type_key)==2 else ''}_T_TypeFlags",
                    select      = ['to_process'],
                    where       = [
                                    ('object_type'   , object_type_key[0]),
                                    ('flag_type'     , flag_type)
                                  ] if len(object_type_key)==1 else [
                                    ('from_object_type'   , object_type_key[0]),
                                    ('to_object_type'     , object_type_key[1])
                                  ],
                    verbose = verbose)

                # Print warning if no output
                if len(output) == 0:
                    sysmsg.warning(f"No flags found for object type key: {object_type_key} with flag type: {flag_type}.")
                    return None

                # Return output as tuples or dict
                return output[0][0]

            # Reset type flags
            def reset(self):

                # Loop over airflow tables and reset to_process flags
                for table_name in ['Operations_N_Object_T_TypeFlags', 'Operations_N_Object_N_Object_T_TypeFlags']:

                    # Execute query to reset to_process flags
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=f"""
                        UPDATE {glbcfg.schema_airflow}.{table_name}
                           SET to_process = 0
                         WHERE to_process = 1
                    """, query_id='AUzikHX5')

            # Quick configuration for input list of node and edge types
            def config(self, config_json):
                """
                    Format:
                        config_json = {
                            'nodes': [['node_type', process_fields, process_scores], ...],
                            'edges': [['from_node_type', 'to_node_type'], ...],
                        }
                    Example:
                        config_json = {
                            'nodes': [['Course', True, False], ['Category', True, True], ['Publication', False, True]],
                            'edges': [['Concept', 'Lecture'], ['MOOC', 'Person'], ['Publication', 'Unit']]
                        }
                """

                # Reset airflow flags
                self.reset()

                # Node types
                if 'nodes' in config_json:
                    for d in config_json['nodes']:
                        node_type, process_fields, process_scores = d
                        if process_fields:
                            self.set(object_type_key=(node_type,), flag_type='fields', to_process=1)
                        if process_scores:
                            self.set(object_type_key=(node_type,), flag_type='scores', to_process=1)

                # Edge types
                if 'edges' in config_json:
                    for d in config_json['edges']:
                        from_node_type, to_node_type, process_fields = d
                        if process_fields:
                            self.set(object_type_key=(from_node_type, to_node_type), to_process=1)
                            self.set(object_type_key=(to_node_type, from_node_type), to_process=1)

            # Get airflow typeflags config JSON
            def get_config_json(self):

                # Initialize config JSON
                config_json = {'nodes': [], 'edges': []}

                # Define SQL query for fetching nodes config
                sql_query = f"""
                     SELECT t1.object_type, t1.to_process AS process_fields, t2.to_process AS process_scores
                       FROM {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t1
                 INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t2
                      USING (object_type)
                      WHERE t1.flag_type = 'fields'
                        AND t2.flag_type = 'scores'
                        AND (t1.to_process = 1 OR t2.to_process = 1)
                """

                # Execute the query
                config_json['nodes'] = [[row[0], row[1]>0.5, row[2]>0.5] for row in db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='4bcoW1KT')]

                # Define SQL query for fetching edges config
                sql_query = f"""
                    SELECT DISTINCT    LEAST(from_object_type, to_object_type) AS from_object_type,
                                    GREATEST(from_object_type, to_object_type) AS to_object_type
                               FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags
                              WHERE to_process = 1
                """

                # Execute the query
                config_json['edges'] = [[row[0], row[1], True] for row in db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='9K34TTeQ')]

                # Return the config JSON
                return config_json

            # Get node and edge types to process
            def get_types_to_process(self, fields_or_scores, return_symmetric=False):

                # Fetch typeflags config JSON
                config_json = self.get_config_json()

                # Processing fields?
                if fields_or_scores=='fields':

                    # Get node types to process
                    node_types_to_process = [node_type for node_type, process_fields, _ in config_json['nodes'] if process_fields]

                    # Get edges to process directly from config json
                    edge_types_to_process = set([tuple(sorted([from_node_type, to_node_type])) for from_node_type, to_node_type, process_fields in config_json['edges'] if process_fields is True])

                    # Filter by available edge types in index config
                    edge_types_available = set([
                        tuple(sorted([d,l]))
                        for d in idxcfg.settings['graphsearch']['fields' ]['links']['parent_child']
                        for l in idxcfg.settings['graphsearch']['fields' ]['links']['parent_child'][d]
                    ])

                    # Calculate set intersection
                    edge_types_to_process = edge_types_to_process.intersection(edge_types_available)

                # Processing scores?
                elif fields_or_scores=='scores':

                    # Get node types to process
                    node_types_to_process = [node_type for node_type, _, process_scores in config_json['nodes'] if process_scores]

                    # Generate all unique edge types
                    edge_types_to_process = list(combinations_with_replacement(sorted(set(node_types_to_process), key=str.casefold), 2))

                # Include symmetric edges?
                if return_symmetric:
                    edge_types_to_process = list(set(list(edge_types_to_process) + [(to_node_type, from_node_type) for from_node_type, to_node_type in edge_types_to_process]))

                # Return results
                return node_types_to_process, edge_types_to_process

        # === Fields Changed Flags ===
        class FieldsChanged():

            # Class constructor
            def __init__(self):
                pass
                # db = GraphDB()

            # Print current settings
            def status(self, object_key=None):
                if object_key is not None:

                    if len(object_key) == 2:
                        sql_query = f"""
                            SELECT object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged
                             WHERE (object_type)
                                 = ("{object_key[0]}", "{object_key[1]}")
                        """

                    elif len(object_key) == 3:
                        sql_query = f"""
                            SELECT object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged
                             WHERE (object_type, object_id)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")
                        """

                    elif len(object_key) == 4:
                        sql_query = f"""
                            SELECT from_object_type, to_object_type, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged
                             WHERE (from_object_type, to_object_type)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}")
                        """

                    elif len(object_key) == 6:
                        sql_query = f"""
                            SELECT from_object_type, from_object_id, to_object_type, to_object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged
                             WHERE (from_object_type, from_object_id, to_object_type, to_object_id)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}", "{object_key[4]}", "{object_key[5]}")
                        """

                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return

                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='UeM1cM6K')
                    df = pd.DataFrame(out, columns=['object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object [by type or key]')

                else:

                    out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                        SELECT object_type, COUNT(*) AS n_to_process
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged
                         WHERE to_process = 1
                      GROUP BY object_type
                    """, query_id='TDgw7fYz')
                    df = pd.DataFrame(out, columns=['object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object [stats]')

                    out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                        SELECT from_object_type, to_object_type, COUNT(*) AS n_to_process
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged
                         WHERE to_process = 1
                      GROUP BY from_object_type, to_object_type
                    """, query_id='EqpDtL34')
                    df = pd.DataFrame(out, columns=['from_object_type', 'to_object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object-to-Object [stats]')

            # Set fields for input object type or id
            def set(self, object_key, checksum_current=None, checksum_previous=None, has_changed=None, last_date_cached=None, has_expired=None, to_process=None, verbose=False):

                # Check object_type_key input
                if not isinstance(object_key, tuple) or len(object_key) not in [2, 3, 4, 6]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 2, 3, 4, or 6.")
                    return

                # Check input parameters
                if (checksum_current  is None and
                    checksum_previous is None and
                    has_changed       is None and
                    last_date_cached  is None and
                    has_expired       is None and
                    to_process        is None
                ):
                    sysmsg.error("Invalid input. One of the following must be provided: checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process.")
                    return

                # Generate WHERE condition
                if len(object_key) == 1:
                    where_conditions = [
                        ('object_type'   , object_key[0])
                    ]
                elif len(object_key) == 2:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        ('object_id'     , object_key[1])
                    ]
                # elif len(object_key) == 2:
                #     where_conditions = [
                #         ('from_object_type'   , object_key[0]),
                #         ('to_object_type'     , object_key[1])
                #     ]
                elif len(object_key) == 4:
                    where_conditions = [
                        ('from_object_type'   , object_key[0]),
                        ('from_object_id'     , object_key[1]),
                        ('to_object_type'     , object_key[2]),
                        ('to_object_id'       , object_key[3])
                    ]

                # Generate SET clause list
                set_clause_list = [(k, v) for k, v in {'checksum_current': checksum_current, 'checksum_previous': checksum_previous, 'has_changed': has_changed, 'last_date_cached': last_date_cached, 'has_expired': has_expired, 'to_process': to_process}.items() if v is not None]

                # Set object type flags
                db.set_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = f"Operations_N_Object{'_N_Object' if len(object_key) in [4,6] else ''}_T_FieldsChanged",
                    set         = set_clause_list,
                    where       = where_conditions,
                    verbose     = verbose)

            # Get fields for input object id
            def get(self, object_key, older_than=None, has_expired=None, verbose=False):

                # Check object_type_key input
                if not isinstance(object_key, tuple) or len(object_key) not in [2, 3, 4, 6]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 2, 3, 4, or 6.")
                    return

                # Check input parameters
                if (older_than  is None and
                    has_expired is None
                ):
                    sysmsg.error("Invalid input. One of the following must be provided: older_than, has_expired.")
                    return

                # Generate time period condition (only rows where last_date_cached is older than 'older_than' (in days) with respect to current date)
                time_condition = f"last_date_cached < CURDATE() - INTERVAL {older_than} DAY" if older_than is not None else "TRUE"

                # Generate has_expired condition (only rows where has_expired is True)
                has_expired_condition = f"has_expired = {has_expired}" if has_expired is not None else "TRUE"

                # Generate WHERE condition
                if len(object_key) == 1:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        (None            , time_condition),
                        (None            , has_expired_condition)
                    ]
                elif len(object_key) == 2:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        ('object_id'     , object_key[1]),
                        (None            , time_condition),
                        (None            , has_expired_condition)
                    ]
                # elif len(object_key) == 4:
                #     where_conditions = [
                #         ('from_object_type'   , object_key[1]),
                #         ('to_object_type'     , object_key[3]),
                #         (None                 , time_condition),
                #         (None                 , has_expired_condition)
                #     ]
                elif len(object_key) == 4:
                    where_conditions = [
                        ('from_object_type'   , object_key[0]),
                        ('from_object_id'     , object_key[1]),
                        ('to_object_type'     , object_key[2]),
                        ('to_object_id'       , object_key[3]),
                        (None                 , time_condition),
                        (None                 , has_expired_condition)
                    ]

                # Get object type flags
                output = db.get_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = f"Operations_N_Object{'_N_Object' if len(object_key) in [2,4] else ''}_T_FieldsChanged",
                    select      = ['object_type', 'object_id', 'checksum_current', 'checksum_previous', 'has_changed', 'last_date_cached', 'has_expired', 'to_process'] if len(object_key) in [1,2] else
                                  ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context', 'checksum_current', 'checksum_previous', 'has_changed', 'last_date_cached', 'has_expired', 'to_process'],
                    where       = where_conditions,
                    verbose     = verbose)

                # Return output as tuples
                return output

            # Sync new objects to operations table
            def sync(self, to_process=1, verbose=False):

                # Print status
                sysmsg.info("♻️  📝 Synching new objects added to the registry with 'FieldsChanged' airflow tables.")

                # Loop over registry data schemas
                for schema_name in [glbcfg.schema_lectures, glbcfg.schema_registry, glbcfg.schema_ontology]:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing nodes on schema '{schema_name}' ...")

                    # Count new object nodes to sync
                    sql_query = f"""
                              SELECT cp.object_type, COUNT(*) AS n
                                FROM {schema_name}.Nodes_N_Object cp
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged fc
                               USING (object_type, object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                               USING (object_type)
                               WHERE fc.object_id IS NULL
                                 AND cp.object_type NOT IN ('Slide', 'Transcript')
                                 AND tf.flag_type = 'fields'
                                 AND tf.to_process = 1
                             GROUP BY cp.object_type
                    """
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='DY3x5PC8')

                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged
                                    (object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process)
                              SELECT cp.object_type, cp.object_id, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Nodes_N_Object cp
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged fc
                               USING (object_type, object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                               USING (object_type)
                               WHERE fc.object_id IS NULL
                                 AND cp.object_type NOT IN ('Slide', 'Transcript')
                                 AND tf.flag_type = 'fields'
                                 AND tf.to_process = 1
                    ON DUPLICATE KEY UPDATE to_process = VALUES(to_process);
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='2PbejfUm')

                    # Print status
                    sysmsg.trace(f"Done. New objects synched: {out}'")

                    # Print status
                    sysmsg.trace(f"⚙️  Updating type flags for new objects on schema '{schema_name}' ...")

                    # Execute object sync @@@@@@@@
                    sql_query = f"""
                                INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags
                                           (object_type, flag_type, to_process)
                            SELECT DISTINCT object_type, 'fields' AS flag_type, 0 AS to_process
                                       FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged
                    ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_T_TypeFlags.to_process;
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='x5BdjGfN')

                    # Print status
                    sysmsg.trace(f"⚙️  Processing edges on schema '{schema_name}' ...")

                    # Count new object-to-object edges to sync
                    sql_query = f"""
                              SELECT cp.from_object_type, cp.to_object_type, COUNT(*) AS n
                                FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent cp
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged fc
                               USING (from_object_type, from_object_id, to_object_type, to_object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                               USING (from_object_type, to_object_type)
                               WHERE fc.from_object_id IS NULL
                                 AND cp.from_object_type NOT IN ('Slide', 'Transcript')
                                 AND cp.to_object_type   NOT IN ('Slide', 'Transcript')
                                 AND NOT (cp.from_object_type = 'Concept' AND cp.to_object_type = 'Concept')
                                 AND tf.to_process = 1
                             GROUP BY cp.from_object_type, cp.to_object_type
                    """
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='Gk7dDRC0')

                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged
                                    (from_object_type, from_object_id, to_object_type, to_object_id, context, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process)
                              SELECT cp.from_object_type, cp.from_object_id, cp.to_object_type, cp.to_object_id, cp.context, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent cp
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged fc
                               USING (from_object_type, from_object_id, to_object_type, to_object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                               USING (from_object_type, to_object_type)
                               WHERE fc.from_object_id IS NULL
                                 AND cp.from_object_type NOT IN ('Slide', 'Transcript')
                                 AND cp.to_object_type   NOT IN ('Slide', 'Transcript')
                                 AND NOT (cp.from_object_type = 'Concept' AND cp.to_object_type = 'Concept')
                                 AND tf.to_process = 1
                    ON DUPLICATE KEY UPDATE to_process = VALUES(to_process);
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='s1gXyPYb')

                    # Print status
                    sysmsg.trace(f"Done. New object tuples synched: {out}'")

                    # Print status
                    sysmsg.trace(f"⚙️  Updating type flags for new edges on schema '{schema_name}' ...")

                    # Execute object sync @@@@@@@@
                    sql_query = f"""
                                INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags
                                           (from_object_type, to_object_type, to_process)
                            SELECT DISTINCT from_object_type, to_object_type, 0 AS to_process
                                       FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged
                    ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_N_Object_T_TypeFlags.to_process;
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='dEE3eDPD')

                # Print status
                sysmsg.success("♻️  ✅ Done synching new objects between registry and 'FieldsChanged' airflow tables.\n")

            # Reset current settings
            def reset(self, doc_type=None, verbose=False):

                # Print status
                sysmsg.info("🧹 📝 Reset 'to_process' flags in 'FieldsChanged' airflow tables.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Loop over airflow tables and reset to_process flags
                    for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET to_process = 0
                             WHERE to_process = 1
                               AND {where_conditions[table_name]}
                        """

                        # Execute query to reset to_process flags
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='RWCE1vkr')

                # Print status
                sysmsg.success("🧹 ✅ Done resetting flags in 'FieldsChanged' airflow tables.\n")

            # Randomize airflow fields [OPTIONAL: For testing purposes]
            def randomize(self, doc_type=None, time_period=182, verbose=False):

                # Print status
                sysmsg.info("🎲 📝 Randomize 'last_date_cached' field in 'FieldsChanged' airflow tables.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Loop over airflow tables
                    for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET last_date_cached = CURDATE() - INTERVAL FLOOR(RAND() * {time_period}) DAY
                             WHERE {where_conditions[table_name]}
                        """

                        # Print query if verbose
                        if verbose:
                            print(f"\nExecuting query:\n{sql_query}\n")

                        # Set random date for "last_date_cached" column.
                        # chunk_filter scopes boundary discovery to rows actually touched by the UPDATE.
                        db.execute_query_in_chunks(
                            engine_name = 'xaas_coresrv',
                            schema_name = glbcfg.schema_airflow,
                            table_name  = table_name,
                            query       = sql_query,
                            chunk_filter = where_conditions[table_name],
                            chunk_size  = 1000000,
                            verbose     = verbose,
                            query_id    = '1GVbHk4y'
                        )

                # Print status
                sysmsg.success("🎲 ✅ Done randomizing dates in 'FieldsChanged' airflow tables.\n")

            # Set expiration dates
            def expire(self, doc_type=None, older_than=None, limit_per_type=None, count_only=False, verbose=False):

                # Apply defaults
                older_than = older_than if older_than!=None else 90
                limit_per_type = limit_per_type if limit_per_type!=None else 100

                # Print status
                sysmsg.info("⌛️ 📝 Set 'has_expired' flag to 1 for expired dates in 'FieldsChanged' airflow tables.")

                # Print parameters
                sysmsg.trace(f"Input parameters: older_than={older_than} (days), limit_per_type={limit_per_type} (rows).")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Print where conditions
                sysmsg.trace(f"Input WHERE conditions: {where_conditions}")

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Loop over airflow tables
                    for u, table_name in [('n', 'Operations_N_Object_T_FieldsChanged'), ('e', 'Operations_N_Object_N_Object_T_FieldsChanged')]:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' - resetting all 'has_expired' flags ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET has_expired = 0
                             WHERE has_expired = 1
                               AND {where_conditions[table_name]}
                        """

                        # Print sql query if verbose
                        if verbose:
                            print_sql(sql_query, title='MY52N1XY')

                        # Reset all expiration flags
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='MY52N1XY')

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' - setting 'has_expired' flags to 1 ...")

                        #----------------------------#
                        # Count or execute operation #
                        #----------------------------#

                        # Execute operation?
                        if not count_only:

                            sql_query = f"""
                              UPDATE {glbcfg.schema_airflow}.{table_name} t
                                JOIN (SELECT row_id
                                        FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY {'object_type' if u=='n' else 'from_object_type, to_object_type'} ORDER BY row_id) AS rn
                                                FROM {glbcfg.schema_airflow}.{table_name}
                                               WHERE ({where_conditions[table_name]})
                                                 AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than} DAY
                                             ) ranked
                                       WHERE rn <= {limit_per_type}
                                     ) ranked_rows
                                  ON t.row_id = ranked_rows.row_id
                                 SET t.has_expired = 1
                               WHERE {where_conditions[table_name]}
                            """

                            # Set has_expired=1 for dates older than time_period (and NULL dates if include_new=True)
                            db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='10PxduJu')

                        # Else, only count number of rows affected
                        else:

                            # Generate evaluation SQL query (direct drop-in; no CTE)
                            sql_query = f"""
                                SELECT {'object_type' if u=='n' else 'from_object_type, to_object_type'},
                                       COUNT(*) AS rows_to_be_set
                                  FROM {glbcfg.schema_airflow}.{table_name} t
                                  JOIN (SELECT row_id
                                          FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY {'object_type' if u=='n' else 'from_object_type, to_object_type'} ORDER BY row_id) AS rn
                                                  FROM {glbcfg.schema_airflow}.{table_name}
                                                 WHERE ({where_conditions[table_name]})
                                                   AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than} DAY
                                               ) ranked
                                         WHERE rn <= {limit_per_type}
                                       ) ranked_rows
                                    ON ranked_rows.row_id = t.row_id
                                 WHERE {where_conditions[table_name]}
                              GROUP BY {'object_type' if u=='n' else 'from_object_type, to_object_type'}
                            """

                            # Print query of verbose
                            if verbose:
                                print(f"\nExecuting query:\n{sql_query}\n")

                            # Set has_expired=1 for dates older than time_period (and NULL dates if include_new=True)
                            out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='d5GKbPVP')

                            # Print as data frame
                            df = pd.DataFrame(out, columns=['object_type', 'rows_to_be_set'] if u=='n' else ['from_object_type', 'to_object_type', 'rows_to_be_set'])
                            if not df.empty:
                                print_dataframe(df, title=f"🪪  FIELDS CHANGED: Table '{table_name}' - Rows that will be set as expired")
                            else:
                                sysmsg.warning(f"No rows will be set as expired in table '{table_name}'.")

                # Print status
                sysmsg.success("⌛️ ✅ Done updating 'has_expired' flags in 'FieldsChanged' airflow tables.\n")

            # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
            def refresh(self, doc_type=None, refresh_checksums=False, limit_per_type=None, verbose=False):

                # Apply defaults
                limit_per_type = limit_per_type if limit_per_type!=None else 100

                # Print status
                sysmsg.info("🧩 🏁 📝 Refresh checksums and set 'to_process' flags to 1 in 'FieldsChanged' airflow tables.")

                # Print parameters
                sysmsg.trace(f"Input parameters: limit_per_type={limit_per_type} (rows).")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        sysmsg.trace(f"Processing doc type: {doc_type}")
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    #-------------------------#
                    # Refresh checksums flags #
                    #-------------------------#

                    # Generate SQL query for objects
                    sql_query = f"""
                         UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged f
                     INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags t
                          USING (object_type)
                            SET has_changed = (f.checksum_current != f.checksum_previous)
                          WHERE t.to_process = 1
                    """

                    # Execute query in shell
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='iGojjBW7')

                    # Generate SQL query for object-to-objects
                    sql_query = f"""
                         UPDATE {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged f
                     INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags t
                          USING (from_object_type, to_object_type)
                            SET has_changed = (f.checksum_current != f.checksum_previous)
                          WHERE t.to_process = 1
                    """

                    # Execute query in shell
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='Hy3LQ6tJ')

                    #------------------------------------------#
                    # Update 'to_process' flags in both tables #
                    #------------------------------------------#

                    # Print status
                    sysmsg.trace(f"Set 'to_process' flags to 1.")

                    # Loop over airflow tables
                    # for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:
                    for u, table_name in [('n', 'Operations_N_Object_T_FieldsChanged'), ('e', 'Operations_N_Object_N_Object_T_FieldsChanged')]:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL query (reset to_process flags before setting again)
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET to_process = 0
                             WHERE to_process = 1
                               AND {where_conditions[table_name]}
                        """

                        # Reset to_process flags for all nodes
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='MsnEuv05')

                        # Generate SQL query
                        # sql_query = f"""
                        #     UPDATE {glbcfg.schema_airflow}.{table_name}
                        #        SET to_process = 1
                        #      WHERE (has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL)
                        #        AND {where_conditions[table_name]}
                        # """

                        # Table key definitions
                        table_type_key = "object_type" if u=='n' else "from_object_type, to_object_type, context"
                        table_full_key = "object_type, object_id" if u=='n' else "from_object_type, from_object_id, to_object_type, to_object_id, context"

                        # Generate SQL query
                        sql_query = f"""
                                  UPDATE {glbcfg.schema_airflow}.{table_name} t2u
                              INNER JOIN (SELECT {table_full_key}
                                            FROM (SELECT {table_full_key}, ROW_NUMBER() OVER (PARTITION BY {table_type_key}) AS row_to_process
                                                    FROM {glbcfg.schema_airflow}.{table_name}
                                                   WHERE (has_changed = 1 OR has_expired = 1 OR last_date_cached IS NULL)
                                                     AND ({where_conditions[table_name]})
                                                 ) tA
                                           WHERE row_to_process <= {limit_per_type}
                                         ) tB
                                   USING ({table_full_key})
                                     SET t2u.to_process = 1
                        """

                        # Update to_process flags for nodes
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='ye472zFQ')

                    #--------------------------------#
                    # Fetch stats on what to process #
                    #--------------------------------#

                    # Print status
                    sysmsg.trace(f"Fetch stats on what to process.")

                    # Loop over airflow tables
                    for u, table_name in [('n', 'Operations_N_Object_T_FieldsChanged'), ('e', 'Operations_N_Object_N_Object_T_FieldsChanged')]:

                        # Generate evaluation query
                        sql_query_eval = f"""
                            SELECT {'object_type' if u=='n' else 'from_object_type, to_object_type'},
                                   SUM(    ISNULL(last_date_cached)                                    ) AS new_or_never_cached,
                                   SUM(NOT ISNULL(last_date_cached) AND     has_changed                ) AS checksum_changed,
                                   SUM(NOT ISNULL(last_date_cached) AND NOT has_changed AND has_expired) AS cache_expired,
                                   SUM(to_process)                                                       AS to_process
                              FROM {glbcfg.schema_airflow}.{table_name}
                          GROUP BY {'object_type' if u=='n' else 'from_object_type, to_object_type'}
                            HAVING new_or_never_cached + checksum_changed + cache_expired > 0
                        """

                        # Execute evaluation query
                        out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='4QF4Lh4y')
                        df = pd.DataFrame(out, columns=[['object_type'] if u=='n' else ['from_object_type', 'to_object_type']][0]+['new_or_never_cached', 'checksum_changed', 'cache_expired', 'to_process'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for table: "{table_name}"')

                        # Generate evaluation query
                        sql_query_eval = f"""
                            SELECT 'Total' AS c,
                                   SUM(    ISNULL(last_date_cached)                                    ) AS new_or_never_cached,
                                   SUM(NOT ISNULL(last_date_cached) AND     has_changed                ) AS checksum_changed,
                                   SUM(NOT ISNULL(last_date_cached) AND NOT has_changed AND has_expired) AS cache_expired,
                                   SUM(to_process)                                                       AS to_process
                              FROM {glbcfg.schema_airflow}.{table_name}
                        """

                        # Execute evaluation query
                        out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='CUytG62p')
                        df = pd.DataFrame(out, columns=['TOTAL', 'new_or_never_cached', 'checksum_changed', 'cache_expired', 'to_process'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for table: "{table_name}"')

                # Print status
                sysmsg.success("🧩 🏁 ✅ Done refreshing checksums and setting 'to_process' flags in 'FieldsChanged' airflow tables.\n")

            # Rollover checksums (replace previous one with current)
            def rollover(self, doc_type=None, actions=('eval',)):

                # Print status
                sysmsg.info("⬅️  📝 Rollover checksums (make previous checksum equal to current) in 'FieldsChanged' airflow tables.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if 'eval' in actions or 'print' in actions:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Loop over airflow tables
                    for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL evaluation query
                        sql_query_eval = f"""
                            SELECT {'object_type' if table_name == 'Operations_N_Object_T_FieldsChanged' else 'from_object_type, to_object_type'}, COUNT(*) AS n_to_rollover
                              FROM {glbcfg.schema_airflow}.{table_name}
                             WHERE (   COALESCE(checksum_previous, '__null__') != COALESCE(checksum_current, '__null__')
                                    OR has_changed > 0.5
                                    OR (has_changed IS NULL AND checksum_current IS NOT NULL)
                                   )
                               AND {where_conditions[table_name]}
                               AND to_process = 1
                          GROUP BY {'object_type' if table_name == 'Operations_N_Object_T_FieldsChanged' else 'from_object_type, to_object_type'}
                        """

                        # Evaluate
                        if 'eval' in actions:

                            # Print evaluation query
                            if 'print' in actions:
                                print('\nSQL evaluation query:\n\n')
                                print_sql(sql_query_eval, title='D3YbxeVt')
                                print('\n')

                            # Execute evaluation query
                            out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='D3YbxeVt')
                            if len(out) > 0:
                                df = pd.DataFrame(out, columns=[['object_type'] if table_name == 'Operations_N_Object_T_FieldsChanged' else ['from_object_type', 'to_object_type']][0]+['n_to_rollover'])
                                print_dataframe(df, title=f'\n🔍 Evaluation results for table: "{table_name}"')

                        # Generate SQL commit query
                        sql_query_commit = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET checksum_previous = checksum_current, has_changed = 0
                             WHERE (   COALESCE(checksum_previous, '__null__') != COALESCE(checksum_current, '__null__')
                                    OR has_changed > 0.5
                                    OR (has_changed IS NULL AND checksum_current IS NOT NULL)
                                   )
                               AND {where_conditions[table_name]}
                               AND to_process = 1
                        """

                        # Reset all expiration flags
                        if 'commit' in actions:
                            db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, verbose='print' in actions, query_id='ht5AZcsE')
                        elif 'print' in actions:
                            print('\nSQL commit query:\n\n')
                            print_sql(sql_query_commit, title='ht5AZcsE')
                            print('\n')

                # Print status
                sysmsg.success("⬅️  ✅ Done rolling over checksums.\n")

            # Update last_date_cached values
            def update_dates(self, doc_type=None, actions=('eval',)):

                # Print status
                sysmsg.info("⬅️  📝 Update last_date_cached values in 'FieldsChanged' airflow tables.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if 'eval' in actions or 'print' in actions:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Loop over airflow tables
                    for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                        # Check if something to do before continuing
                        if where_conditions[table_name] == "FALSE":
                            sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                            continue

                        # Generate SQL evaluation query
                        sql_query_eval = f"""
                            SELECT {'object_type' if table_name == 'Operations_N_Object_T_FieldsChanged' else 'from_object_type, to_object_type'}, COUNT(*) AS n_to_update
                              FROM {glbcfg.schema_airflow}.{table_name}
                             WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                               AND {where_conditions[table_name]}
                               AND to_process = 1
                          GROUP BY {'object_type' if table_name == 'Operations_N_Object_T_FieldsChanged' else 'from_object_type, to_object_type'}
                        """

                        # Evaluate
                        if 'eval' in actions:

                            # Print evaluation query
                            if 'print' in actions:
                                print('\nSQL evaluation query:\n\n')
                                print_sql(sql_query_eval, title='kpAX4Cft')

                            # Execute evaluation query
                            out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='kpAX4Cft')
                            if len(out) > 0:
                                df = pd.DataFrame(out, columns=[['object_type'] if table_name == 'Operations_N_Object_T_FieldsChanged' else ['from_object_type', 'to_object_type']][0]+['n_to_update'])
                                print_dataframe(df, title=f'\n🔍 Evaluation results for table: "{table_name}"')

                        # Generate SQL commit query
                        sql_query_commit = f"""
                            UPDATE {glbcfg.schema_airflow}.{table_name}
                               SET last_date_cached = DATE(NOW()), has_expired = 0
                             WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                               AND {where_conditions[table_name]}
                               AND to_process = 1
                        """

                        # Reset all expiration flags
                        if 'commit' in actions:
                            db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, verbose='print' in actions, query_id='Q2dracb0')
                        elif 'print' in actions:
                            print('\nSQL commit query:\n\n')
                            print_sql(sql_query_commit, title='Q2dracb0')
                            print('\n')

                # Print status
                sysmsg.success("⬅️  ✅ Done updating last_date_cached values in 'FieldsChanged' airflow tables.\n")

        # === Scores Expired Flags ===
        class ScoresExpired():

            # Class constructor
            def __init__(self):
                pass
                # db = GraphDB()

            # Print current settings
            def status(self, object_key=None):
                if object_key is not None:
                    sql_query = f"""
                        SELECT object_type, object_id, last_date_cached, has_expired, to_process
                        FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                    """
                    if len(object_key) == 2:
                        sql_query += f"""WHERE (object_type) = ("{object_key[0]}", "{object_key[1]}")"""
                    elif len(object_key) == 3:
                        sql_query += f"""WHERE (object_type, object_id) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")"""
                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='xGQc0t7t')
                    df = pd.DataFrame(out, columns=['object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🧮 SCORES EXPIRED: Object [by key or id]')
                else:
                    out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                        SELECT object_type, COUNT(*) AS n_to_process
                        FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                        WHERE to_process = 1
                     GROUP BY object_type
                    """, query_id='ts8NQExF')
                    df = pd.DataFrame(out, columns=['object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🧮 SCORES EXPIRED: Object [stats]')

            # Set fields for input object type or id
            def set(self, object_key, last_date_cached=None, has_expired=None, to_process=None, verbose=False):

                # Check object_type_key input
                if not isinstance(object_key, tuple) or len(object_key) not in [2, 3]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 2 or 3.")
                    return

                # Check input parameters
                if (last_date_cached  is None and
                    has_expired       is None and
                    to_process        is None
                ):
                    sysmsg.error("Invalid input. One of the following must be provided: last_date_cached, has_expired, to_process.")
                    return

                # Generate WHERE condition
                if len(object_key) == 1:
                    where_conditions = [
                        ('object_type'   , object_key[0])
                    ]
                elif len(object_key) == 2:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        ('object_id'     , object_key[1])
                    ]

                # Generate SET clause list
                set_clause_list = [(k, v) for k, v in {'last_date_cached': last_date_cached, 'has_expired': has_expired, 'to_process': to_process}.items() if v is not None]

                # Set object type flags
                db.set_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = 'Operations_N_Object_T_ScoresExpired',
                    set         = set_clause_list,
                    where       = where_conditions,
                    verbose     = verbose)

            # Get fields for input object id
            def get(self, object_key, older_than=None, has_expired=None, verbose=False):

                # Check object_type_key input
                if not isinstance(object_key, tuple) or len(object_key) not in [2, 3]:
                    sysmsg.error("Invalid object_type_key. It should be a tuple of length 2 or 3.")
                    return

                # Check input parameters
                if (older_than  is None and
                    has_expired is None
                ):
                    sysmsg.error("Invalid input. One of the following must be provided: older_than, has_expired.")
                    return

                # Generate time period condition (only rows where last_date_cached is older than 'older_than' (in days) with respect to current date)
                time_condition = f"last_date_cached < CURDATE() - INTERVAL {older_than} DAY" if older_than is not None else "TRUE"

                # Generate has_expired condition (only rows where has_expired is True)
                has_expired_condition = f"has_expired = {has_expired}" if has_expired is not None else "TRUE"

                # Generate WHERE condition
                if len(object_key) == 1:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        (None            , time_condition),
                        (None            , has_expired_condition)
                    ]
                elif len(object_key) == 2:
                    where_conditions = [
                        ('object_type'   , object_key[0]),
                        ('object_id'     , object_key[1]),
                        (None            , time_condition),
                        (None            , has_expired_condition)
                    ]

                # Get object type flags
                output = db.get_cells(
                    engine_name = 'xaas_coresrv',
                    schema_name = glbcfg.schema_airflow,
                    table_name  = 'Operations_N_Object_T_ScoresExpired',
                    select      = ['object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process'],
                    where       = where_conditions,
                    verbose     = verbose)

                # Return output as tuples
                return output

            # Sync new objects to operations table -> TODO: optimise queries and include graph_lectures (done?)
            def sync(self, to_process=1, verbose=False):

                # Print status
                sysmsg.info("♻️  📝 Synching new objects added to the registry with 'ScoresExpired' airflow tables.")

                # Loop over registry data schemas
                for schema_name in [glbcfg.schema_lectures, glbcfg.schema_registry, glbcfg.schema_ontology]:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing nodes on schema '{schema_name}' ...")

                    # Count new object nodes to sync
                    sql_query = f"""
                              SELECT n.object_type, COUNT(*) AS n
                                FROM {schema_name}.Nodes_N_Object n
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired o
                               USING (object_type, object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                               USING (object_type)
                               WHERE o.object_id IS NULL
                                 AND n.object_type != 'Transcript'
                                 AND n.object_type != 'Slide'
                                 AND tf.flag_type = 'fields'
                                 AND tf.to_process = 1
                             GROUP BY n.object_type
                    """
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='7RNfE1fF')

                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                    (object_type, object_id, last_date_cached, has_expired, to_process)
                              SELECT n.object_type, n.object_id, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Nodes_N_Object n
                           LEFT JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired o
                               USING (object_type, object_id)
                          INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                               USING (object_type)
                               WHERE o.object_id IS NULL
                                 AND n.object_type NOT IN ('Slide', 'Transcript')
                                 AND tf.flag_type = 'fields'
                                 AND tf.to_process = 1
                    ON DUPLICATE KEY UPDATE to_process = VALUES(to_process);
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='5mhz4Uwr')

                    # Print status
                    sysmsg.trace(f"Done. New objects synched: {out}'")

                    # Print status
                    sysmsg.trace(f"⚙️  Updating type flags for new objects on schema '{schema_name}' ...")

                    # Execute object sync @@@@@@@
                    sql_query = f"""
                                INSERT INTO {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags
                                           (object_type, flag_type, to_process)
                            SELECT DISTINCT object_type, 'scores' AS flag_type, 0 AS to_process
                                       FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                    ON DUPLICATE KEY UPDATE to_process = Operations_N_Object_T_TypeFlags.to_process;
                    """
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='n2TKRWNV')

                # Print status
                sysmsg.success("♻️  ✅ Done synching new objects between registry and 'ScoresExpired' airflow tables.\n")

            # Reset current settings
            def reset(self, doc_type=None, verbose=False):

                # Print status
                sysmsg.info("🧹 📝 Reset 'to_process' flags in 'ScoresExpired' airflow table.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' ...")

                    # Check if something to do before continuing
                    if where_conditions['Operations_N_Object_T_ScoresExpired'] == "FALSE":
                        sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                        pass

                    # If WHERE conditions were generated, continue
                    else:

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                               SET to_process = 0
                             WHERE to_process = 1
                               AND {where_conditions['Operations_N_Object_T_ScoresExpired']}
                        """

                        # Execute query to reset to_process flags
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='AhJepYi8')

                # Print status
                sysmsg.success("🧹 ✅ Done resetting flags in 'ScoresExpired' airflow table.\n")

            # Randomize airflow fields [OPTIONAL: For testing purposes]
            def randomize(self, doc_type=None, time_period=182, verbose=False):

                # Print status
                sysmsg.info("🎲 📝 Randomize 'last_date_cached' field in 'ScoresExpired' airflow table.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' ...")

                    # Check if something to do before continuing
                    if where_conditions['Operations_N_Object_T_ScoresExpired'] == "FALSE":
                        sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                        pass

                    # If WHERE conditions were generated, continue
                    else:

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                               SET last_date_cached = CURDATE() - INTERVAL FLOOR(RAND() * {time_period}) DAY
                             WHERE {where_conditions['Operations_N_Object_T_ScoresExpired']}
                        """

                        # Print query if verbose
                        if verbose:
                            print(f"\nExecuting query:\n{sql_query}\n")

                        # Set random date for "last_date_cached" column.
                        # chunk_filter scopes boundary discovery to rows actually touched by the UPDATE.
                        db.execute_query_in_chunks(
                            engine_name = 'xaas_coresrv',
                            schema_name = glbcfg.schema_airflow,
                            table_name  = 'Operations_N_Object_T_ScoresExpired',
                            query       = sql_query,
                            chunk_filter = where_conditions['Operations_N_Object_T_ScoresExpired'],
                            chunk_size  = 100000,
                            verbose     = verbose,
                            query_id    = 'q1kQA2gx'
                        )

                # Print status
                sysmsg.success("🎲 ✅ Done randomizing dates in 'ScoresExpired' airflow table.\n")

            # Set expiration dates
            def expire(self, doc_type=None, older_than=None, limit_per_type=None, count_only=False, verbose=False):

                # Apply defaults
                older_than = older_than if older_than!=None else 90
                limit_per_type = limit_per_type if limit_per_type!=None else 100

                # Print status
                sysmsg.info("⌛️ 📝 Set 'has_expired' flag to 1 for expired dates in 'ScoresExpired' airflow table.")

                # Print parameters
                sysmsg.trace(f"Input parameters: older_than={older_than} (days), limit_per_type={limit_per_type} (rows).")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Print where conditions
                sysmsg.trace(f"Input WHERE conditions: {where_conditions}")

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Check if something to do before continuing
                    if where_conditions['Operations_N_Object_T_ScoresExpired'] == "FALSE":
                        sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                        pass

                    # If WHERE conditions were generated, continue
                    else:

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' - resetting all 'has_expired' flags ...")

                        # Generate SQL query
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                               SET has_expired = 0
                             WHERE has_expired = 1
                               AND {where_conditions['Operations_N_Object_T_ScoresExpired']}
                        """

                        # Reset all expiration flags
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='Zsz9iF13')

                        # Print status
                        sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' - setting 'has_expired' flags to 1 ...")

                        #----------------------------#
                        # Count or execute operation #
                        #----------------------------#

                        # Execute operation?
                        if not count_only:

                            # Generate SQL query (direct drop-in; no CTE)
                            sql_query = f"""
                                UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired t
                                  JOIN (SELECT row_id
                                          FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY row_id) AS rn
                                                  FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                                 WHERE ({where_conditions['Operations_N_Object_T_ScoresExpired']})
                                                   AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than} DAY
                                               ) ranked
                                         WHERE rn <= {limit_per_type}
                                       ) ranked_rows
                                    ON t.row_id = ranked_rows.row_id
                                   SET t.has_expired = 1
                                 WHERE {where_conditions['Operations_N_Object_T_ScoresExpired']}
                            """

                            # Set has_expired=1 for dates older than time_period
                            db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='6nKcLVme')

                        # Else, only count number of rows affected
                        else:

                            # Generate SQL query (direct drop-in; no CTE)
                            sql_query = f"""
                                SELECT object_type, COUNT(*) AS rows_to_be_set
                                  FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired t
                                  JOIN (SELECT row_id
                                          FROM (SELECT row_id, ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY row_id) AS rn
                                                  FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                                 WHERE ({where_conditions['Operations_N_Object_T_ScoresExpired']})
                                                   AND COALESCE(last_date_cached, DATE('1900-01-01')) < CURDATE() - INTERVAL {older_than} DAY
                                               ) ranked
                                         WHERE rn <= {limit_per_type}
                                       ) ranked_rows
                                    ON ranked_rows.row_id = t.row_id
                                 WHERE {where_conditions['Operations_N_Object_T_ScoresExpired']}
                              GROUP BY object_type
                            """

                            # Print query of verbose
                            if verbose:
                                print(f"\nExecuting query:\n{sql_query}\n")

                            # Set has_expired=1 for dates older than time_period
                            out = db.execute_query(engine_name='xaas_coresrv', query=sql_query, query_id='46PNmQvh')

                            # Print as data frame
                            df = pd.DataFrame(out, columns=['object_type', 'rows_to_be_set'])
                            if not df.empty:
                                print_dataframe(df, title=f"🧮 SCORES EXPIRED: Table 'Operations_N_Object_T_ScoresExpired' - Rows that will be set as expired")
                            else:
                                sysmsg.warning(f"No rows will be set as expired in table 'Operations_N_Object_T_ScoresExpired'.")

                # Print status
                sysmsg.success("⌛️ ✅ Done updating 'has_expired' flags in 'ScoresExpired' airflow table.\n")

            # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
            def refresh(self, doc_type=None, limit_per_type=None, verbose=False):

                # Apply defaults
                limit_per_type = limit_per_type if limit_per_type!=None else 100

                # Print status
                sysmsg.info("🏁 📝 Set 'to_process' flags to 1 in 'ScoresExpired' airflow tables.")

                # Print parameters
                sysmsg.trace(f"Input parameters: limit_per_type={limit_per_type} (rows).")

                #------------------------------------------#
                # Update 'to_process' flags in both tables #
                #------------------------------------------#

                # Print status
                sysmsg.trace(f"⚙️  Processing 'Operations_N_Object_T_ScoresExpired' table ...")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if verbose:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' ...")

                    # Check if something to do before continuing
                    if where_conditions['Operations_N_Object_T_ScoresExpired'] == "FALSE":
                        sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                        pass

                    # If WHERE conditions were generated, continue
                    else:

                        # Generate SQL query (reset to_process flags before setting again)
                        sql_query = f"""
                            UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                               SET to_process = 0
                             WHERE to_process = 1
                               AND {where_conditions['Operations_N_Object_T_ScoresExpired']}
                        """

                        # Reset to_process flags for all nodes
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='q4L84LJy')

                        # # Generate SQL query
                        # sql_query = f"""
                        #     UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                        #        SET to_process = 1
                        #      WHERE (has_expired = 1 OR last_date_cached IS NULL)
                        #        AND {where_conditions['Operations_N_Object_T_ScoresExpired']}
                        # """

                        # Generate SQL query
                        sql_query = f"""
                                  UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired t2u
		                      INNER JOIN (SELECT object_type, object_id
					                        FROM (SELECT object_type, object_id, ROW_NUMBER() OVER (PARTITION BY object_type) AS row_to_process
                                                    FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                                   WHERE (has_expired = 1 OR last_date_cached IS NULL)
                                                     AND ({where_conditions['Operations_N_Object_T_ScoresExpired']})
                                                 ) tA
                                           WHERE row_to_process <= {limit_per_type}
                                         ) tB
		                           USING (object_type, object_id)
                                     SET t2u.to_process = 1
                        """

                        # Update to_process flags for nodes
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose=verbose, query_id='tBKyps8J')

                        #--------------------------------#
                        # Fetch stats on what to process #
                        #--------------------------------#

                        # Print status
                        sysmsg.trace(f"Fetch stats on what to process.")

                        # Generate evaluation query
                        sql_query_eval = f"""
                            SELECT object_type,
                                   SUM(    ISNULL(last_date_cached)                ) AS new_or_never_cached,
                                   SUM(NOT ISNULL(last_date_cached) AND has_expired) AS cache_expired
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                          GROUP BY object_type
                            HAVING new_or_never_cached + cache_expired > 0
                        """

                        # Execute evaluation query
                        out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='JH9iFxCF')
                        df = pd.DataFrame(out, columns=['object_type', 'new_or_never_cached', 'cache_expired'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for table: "Operations_N_Object_T_ScoresExpired"')

                        # Generate evaluation query
                        sql_query_eval = f"""
                            SELECT 'Total' AS c,
                                   SUM(    ISNULL(last_date_cached)                ) AS new_or_never_cached,
                                   SUM(NOT ISNULL(last_date_cached) AND has_expired) AS cache_expired
                              FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                        """

                        # Execute evaluation query
                        out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='ZK89fhUA')
                        df = pd.DataFrame(out, columns=['TOTAL', 'new_or_never_cached', 'cache_expired'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for table: "Operations_N_Object_T_ScoresExpired"')

                # Print status
                sysmsg.success("🏁 ✅ Done setting 'to_process' flags in 'ScoresExpired' airflow tables.\n")

            # Update last_date_cached values
            def update_dates(self, doc_type=None, actions=('eval',)):

                # Print status
                sysmsg.info("⬅️  📝 Update last_date_cached values in 'ScoresExpired' airflow tables.")

                # Generate Airflow WHERE conditions
                where_conditions = generate_airflow_where_conditions(doc_type=doc_type)

                # Check if something to do
                if where_conditions is None:
                    sysmsg.warning("Nothing to do. Check input 'doc_type' or typeflags config.")

                # If WHERE conditions were generated, continue
                else:

                    # Print conditions if verbose
                    if 'eval' in actions or 'print' in actions:
                        print("\nAirflow WHERE conditions:")
                        rich.print_json(data=where_conditions)
                        print('')

                    # Only one table to process
                    table_name = 'Operations_N_Object_T_ScoresExpired'

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                    # Check if something to do before continuing
                    if where_conditions[table_name] == "FALSE":
                        sysmsg.trace("Nothing to do. Check input 'doc_type' or typeflags config.")
                        return

                    # Generate SQL evaluation query
                    sql_query_eval = f"""
                        SELECT object_type, COUNT(*) AS n_to_update
                          FROM {glbcfg.schema_airflow}.{table_name}
                         WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                           AND {where_conditions[table_name]}
                           AND to_process = 1
                      GROUP BY object_type
                    """

                    # Evaluate
                    if 'eval' in actions:

                        # Print evaluation query
                        if 'print' in actions:
                            print('\nSQL evaluation query:\n\n')
                            print_sql(sql_query_eval, title='y9GdvZ4W')

                        # Execute evaluation query
                        out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='y9GdvZ4W')
                        if len(out) > 0:
                            df = pd.DataFrame(out, columns=['object_type', 'n_to_update'])
                            print_dataframe(df, title=f'\n🔍 Evaluation results for table: "{table_name}"')

                    # Generate SQL commit query
                    sql_query_commit = f"""
                        UPDATE {glbcfg.schema_airflow}.{table_name}
                           SET last_date_cached = DATE(NOW()), has_expired = 0
                         WHERE COALESCE(last_date_cached, DATE('0000-00-00')) != DATE(NOW())
                           AND {where_conditions[table_name]}
                           AND to_process = 1
                    """

                    # Reset all expiration flags
                    if 'commit' in actions:
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, verbose='print' in actions)
                    elif 'print' in actions:
                        print('\nSQL commit query:\n\n')
                        print_sql(sql_query_commit, title='ERWG42')
                        print('\n')

                # Print status
                sysmsg.success("⬅️  ✅ Done updating last_date_cached values in 'ScoresExpired' airflow tables.\n")

    #-----------------------------------------------------#
    # Subclass definition: GraphRegistry Cache Management #
    #-----------------------------------------------------#
    class CacheManagement():

        # Class constructor
        def __init__(self):
            pass
            # db = GraphDB()

        # Commit for all views [calls 'cache_update_from_view']
        def materialize_views(self, actions=()):

            # Print status
            sysmsg.info(f"👀 📝 Materialize views and commit updated data to '{glbcfg.schema_graph_cache_test}' [actions: {actions}].")

            # Print action specific status
            if len(actions) == 0:
                sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                sysmsg.info(f"🚀 📝 Nothing to do.")
                return
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # List of views to execute
            list_of_views = [
                'obj2obj: all fields symmetric',
                'obj: page profile',
                'obj: all fields',
                'obj2obj: parent-child symmetric'
                # 'obj2obj: parent-child symmetric (ontology)'
            ]

            # Execute and commit all views
            for view_name in list_of_views:
                self.cache_update_from_view(view_name, actions=actions)

            # Print status
            sysmsg.success(f"👀 ✅ Done materializing views.\n")

        # Compute and cache scores [calls 'cache_update_from_batch_formula']
        def apply_formulas(self, formula_type=None, verbose=False):

            # Print status
            sysmsg.info(f"🚀 📝 Apply formulas commit updated data to '{glbcfg.schema_graph_cache_test}' [verbose: {verbose}].")

            #---------------------------------#
            # Formula type: Calculated fields #
            #---------------------------------#

            # Process formula type?
            if formula_type is None or formula_type == 'calculated fields':

                # Fetch list of calculated field formulas to execute
                list_of_calcfield_formulas = []
                for d in ['obj', 'obj2obj']:

                    # Fetch list of calculated field formulas to execute
                    list_of_unparsed_names = [re.findall(r'\/formula\.(.*)\.sql$', f)[0] for f in sorted(glob.glob(f'{SQL_FORMULAS_PATH}/calculated_fields/{d}/formula.*.sql'))]

                    # Extract list of object type keys and field names
                    list_of_calcfield_formulas += [tuple(f.split('.')) if d=='obj' else (tuple(f.split('.')[:2]), f.split('.')[2]) for f in list_of_unparsed_names]

                # Execute and commit all formulas
                for object_type_key, field_name in list_of_calcfield_formulas:
                    self.cache_update_from_calculated_field(object_type_key=object_type_key, field_name=field_name, verbose=verbose)

            #---------------------------------#
            # Formula type: Calculated fields #
            #---------------------------------#

            # Process formula type?
            if formula_type is None or formula_type == 'batch':

                # Fetch list of batch formulas to execute
                list_of_batch_formulas = [re.findall(r'\/formula\.(.*)\.sql$', f)[0] for f in sorted(glob.glob(f'{SQL_FORMULAS_PATH}/batch/formula.*.sql'))]

                # Execute and commit all formulas
                for formula_name in list_of_batch_formulas:
                    self.cache_update_from_batch_formula(formula_name, verbose=verbose)

            # Print status
            sysmsg.success(f"🚀 ✅ Done applying formulas and committing updated data to '{glbcfg.schema_graph_cache_test}'.\n")

        # Batch apply formulas: calculated fields only
        def apply_calculated_field_formulas(self, verbose=False):
            for local_path in [
                'calculated_fields/obj',
                'calculated_fields/obj2obj'
            ]:
                self.apply_formulas_from_folder(local_path=local_path, verbose=verbose)

        # Batch apply formulas: traversal and scoring
        def apply_traversal_and_scoring_formulas(self, verbose=False):
            self.apply_traversals(verbose=verbose)
            self.apply_scoring_formulas(verbose=verbose)

        # Batch apply formulas: traversal and scoring
        def apply_traversals(self, verbose=False):
            for local_path in [
                'graph_traversals',
            ]:
                self.apply_formulas_from_folder(local_path=local_path, verbose=verbose)

        # Batch apply formulas: traversal and scoring
        def apply_scoring_formulas(self, verbose=False):
            for local_path in [
                'calculated_scores/obj2ontology/concepts',
                'calculated_scores/obj2ontology/concepts_union',
                'calculated_scores/obj2ontology/categories',
                'calculated_scores/obj2ontology/categories_union',
                'calculated_scores/degree_scores'
            ]:
                self.apply_formulas_from_folder(local_path=local_path, verbose=verbose)

        # Update all scores
        def update_scores_matrix(self, score_thr=0.1, actions=()):

            # Print status
            sysmsg.info(f"🧮 📝 Calculate and consolidate scores matrices.")

            # Get ontology-related edges to process
            _, tmp_list = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='scores')
            ontology_edges = [[d,l] for d,l in tmp_list if d in ('Category', 'Concept') or l in ('Category', 'Concept')]

            # Build list of edge types to process
            edge_types_to_process = scrcfg.settings['scored_edge_tuples']['education'] + scrcfg.settings['scored_edge_tuples']['research'] + ontology_edges
            edge_types_to_process = sorted([(d,l) for d,l in edge_types_to_process])

            # Fetch typeflags config JSON
            _, doclink_types_to_process_from_config = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='scores')

            # Filter out edge types not in config
            edge_types_to_process = [ (d,l) for d,l in edge_types_to_process if (d,l) in doclink_types_to_process_from_config ]

            # Check if any edge types remain after filtering
            if len(edge_types_to_process) == 0:
                sysmsg.warning(f"No object-to-object edge types to process for scores matrix calculation. Check TypeFlags configuration.")
                sysmsg.info(f"🧮 Nothing to do.")
                return

            # Print status
            sysmsg.trace(f"⚙️  Calculating scores matrix for object-to-object edge combinations ...")

            # Print list of affected tables
            print('\n[🐬 GraphSearch DB] [SM-DB] The following edges will be (re)scored:')
            for t in edge_types_to_process:
                print(f" - {t[0]} --> {t[1]}")
            print('')

            # Loop over edge types
            with tqdm(edge_types_to_process, unit='edge type') as pb:
                for n1, n2 in pb:

                    # Print status
                    pb.set_description(f"⚙️  Processing edge type: {n1} --> {n2}".ljust(PBWIDTH)[:PBWIDTH])

                    # Calculate scores matrix
                    self.calculate_scores_matrix(from_object_type=n1, to_object_type=n2, actions=actions)

            # Print status
            sysmsg.trace(f"⚙️  Consolidating scores matrix (normalising scores and inserting Category/Concept edges) ...")

            # Loop over edge types
            with tqdm(edge_types_to_process, unit='edge type') as pb:
                for n1, n2 in pb:

                    # Print status
                    pb.set_description(f"⚙️  Processing edge type: {n1} --> {n2}".ljust(PBWIDTH)[:PBWIDTH])

                    # Consolidate scores matrix
                    self.consolidate_scores_matrix(from_object_type=n1, to_object_type=n2, update_averages=True, score_thr=score_thr, actions=actions)

            # Print status
            sysmsg.success(f"🧮 ✅ Done updating scores matrices.\n")

        # Update cache table from registry view
        def cache_update_from_view(self, view_name, actions=()):

            # Initialize variables with default values
            query_has_filters = None

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            if view_name == 'obj2obj: all fields symmetric':

                # Target cache table
                target_table = 'Data_N_Object_N_Object_T_AllFieldsSymmetric'

                # List of evaluation columns
                eval_columns = ['from_object_type', 'to_object_type', 'field_name']

                # Initialise query stack
                sql_query_stack = []

                # Query template
                sql_query_template = """
                    SELECT cf.from_object_type,
                           cf.from_object_id,
                           cf.to_object_type,
                           cf.to_object_id,
                           cf.context,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM %s.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_N_Object_T_%s cf
                     USING (from_object_type, from_object_id, to_object_type, to_object_id)
                INNER JOIN %s.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_object_type, to_object_type)
                     WHERE tp.to_process = 1
                       AND tf.to_process = 1
                       AND cf.from_object_type NOT IN ('Slide')
                       AND   cf.to_object_type NOT IN ('Slide')

                 UNION ALL

                    SELECT cf.to_object_type      AS from_object_type,
                           cf.to_object_id        AS from_object_id,
                           cf.from_object_type    AS to_object_type,
                           cf.from_object_id      AS to_object_id,
                           cf.context             AS context,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM %s.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_N_Object_T_%s cf
                     USING (from_object_type, from_object_id, to_object_type, to_object_id)
                INNER JOIN %s.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_object_type, to_object_type)
                     WHERE tp.to_process = 1
                       AND tf.to_process = 1
                       AND cf.from_object_type NOT IN ('Slide')
                       AND   cf.to_object_type NOT IN ('Slide')
                """

                # Append queries for custom fields
                for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:
                    sql_query_stack += [sql_query_template % (
                        glbcfg.schema_airflow,
                        schema_name, 'CustomFields',
                        glbcfg.schema_airflow,
                        glbcfg.schema_airflow,
                        schema_name, 'CustomFields',
                        glbcfg.schema_airflow
                    )]

                # Append query for cached calculated fields
                sql_query_stack += [sql_query_template % (
                    glbcfg.schema_airflow,
                    glbcfg.schema_graph_cache_test, 'CalculatedFields',
                    glbcfg.schema_airflow,
                    glbcfg.schema_airflow,
                    glbcfg.schema_graph_cache_test, 'CalculatedFields',
                    glbcfg.schema_airflow)]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj: page profile':

                # Target cache table
                target_table = 'Data_N_Object_T_PageProfile'

                # List of evaluation columns
                eval_columns = ['object_type']

                # Initialise query stack
                sql_query_stack = []

                # Loop over schemas
                for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                    # Append query
                    sql_query_stack += [f"""
                        SELECT pp.object_type, pp.object_id, pp.numeric_id_en, pp.numeric_id_fr, pp.numeric_id_de, pp.numeric_id_it, pp.short_code, pp.subtype_en, pp.subtype_fr, pp.subtype_de, pp.subtype_it, pp.name_en_is_auto_generated, pp.name_en_is_auto_corrected, pp.name_en_is_auto_translated, pp.name_en_translated_from, pp.name_en_value, pp.name_fr_is_auto_generated, pp.name_fr_is_auto_corrected, pp.name_fr_is_auto_translated, pp.name_fr_translated_from, pp.name_fr_value, pp.name_de_is_auto_generated, pp.name_de_is_auto_corrected, pp.name_de_is_auto_translated, pp.name_de_translated_from, pp.name_de_value, pp.name_it_is_auto_generated, pp.name_it_is_auto_corrected, pp.name_it_is_auto_translated, pp.name_it_translated_from, pp.name_it_value, pp.description_short_en_is_auto_generated, pp.description_short_en_is_auto_corrected, pp.description_short_en_is_auto_translated, pp.description_short_en_translated_from, pp.description_short_en_value, pp.description_short_fr_is_auto_generated, pp.description_short_fr_is_auto_corrected, pp.description_short_fr_is_auto_translated, pp.description_short_fr_translated_from, pp.description_short_fr_value, pp.description_short_de_is_auto_generated, pp.description_short_de_is_auto_corrected, pp.description_short_de_is_auto_translated, pp.description_short_de_translated_from, pp.description_short_de_value, pp.description_short_it_is_auto_generated, pp.description_short_it_is_auto_corrected, pp.description_short_it_is_auto_translated, pp.description_short_it_translated_from, pp.description_short_it_value, pp.description_medium_en_is_auto_generated, pp.description_medium_en_is_auto_corrected, pp.description_medium_en_is_auto_translated, pp.description_medium_en_translated_from, pp.description_medium_en_value, pp.description_medium_fr_is_auto_generated, pp.description_medium_fr_is_auto_corrected, pp.description_medium_fr_is_auto_translated, pp.description_medium_fr_translated_from, pp.description_medium_fr_value, pp.description_medium_de_is_auto_generated, pp.description_medium_de_is_auto_corrected, pp.description_medium_de_is_auto_translated, pp.description_medium_de_translated_from, pp.description_medium_de_value, pp.description_medium_it_is_auto_generated, pp.description_medium_it_is_auto_corrected, pp.description_medium_it_is_auto_translated, pp.description_medium_it_translated_from, pp.description_medium_it_value, pp.description_long_en_is_auto_generated, pp.description_long_en_is_auto_corrected, pp.description_long_en_is_auto_translated, pp.description_long_en_translated_from, pp.description_long_en_value, pp.description_long_fr_is_auto_generated, pp.description_long_fr_is_auto_corrected, pp.description_long_fr_is_auto_translated, pp.description_long_fr_translated_from, pp.description_long_fr_value, pp.description_long_de_is_auto_generated, pp.description_long_de_is_auto_corrected, pp.description_long_de_is_auto_translated, pp.description_long_de_translated_from, pp.description_long_de_value, pp.description_long_it_is_auto_generated, pp.description_long_it_is_auto_corrected, pp.description_long_it_is_auto_translated, pp.description_long_it_translated_from, pp.description_long_it_value, pp.external_key_en, pp.external_key_fr, pp.external_key_de, pp.external_key_it, pp.external_url_en, pp.external_url_fr, pp.external_url_de, pp.external_url_it, pp.is_visible, 1 AS to_process, 0 AS deleted
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Data_N_Object_T_PageProfile pp
                         USING (object_type, object_id)
                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                         USING (object_type)
                         WHERE tp.to_process = 1
                           AND tf.flag_type = 'fields'
                           AND tf.to_process = 1
                    """]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj: all fields':

                # Target cache table
                target_table = 'Data_N_Object_T_AllFields'

                # List of evaluation columns
                eval_columns = ['object_type', 'field_name']

                # Initialise query stack
                sql_query_stack = []

                # Query template
                sql_query_template = """
                    SELECT cf.object_type, cf.object_id,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM %s.Operations_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_T_%s cf
                     USING (object_type, object_id)
                INNER JOIN %s.Operations_N_Object_T_TypeFlags tf
                     USING (object_type)
                     WHERE tp.to_process = 1
                       AND tf.flag_type = 'fields'
                       AND tf.to_process = 1
                       AND cf.object_type NOT IN ('Slide')
                """

                # Append queries for custom fields
                for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:
                    sql_query_stack += [sql_query_template % (
                        glbcfg.schema_airflow,
                        schema_name, 'CustomFields',
                        glbcfg.schema_airflow
                    )]

                # Append query for cached calculated fields
                sql_query_stack += [sql_query_template % (
                    glbcfg.schema_airflow,
                    glbcfg.schema_graph_cache_test, 'CalculatedFields',
                    glbcfg.schema_airflow)]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj2obj: parent-child symmetric':

                # Target cache table
                target_table = 'Edges_N_Object_N_Object_T_ParentChildSymmetric'

                # List of evaluation columns
                eval_columns = ['from_object_type', 'to_object_type']

                # Initialise query stack
                sql_query_stack = []

                # Loop over schemas
                for schema_name in [glbcfg.schema_registry, glbcfg.schema_lectures, glbcfg.schema_ontology]:

                    # Append query
                    sql_query_stack += [f"""
                        SELECT 'Child-to-Parent' AS edge_type,
                               c2p.from_object_type,
                               c2p.from_object_id,
                               c2p.to_object_type,
                               c2p.to_object_id,
                               c2p.context,
                               1 AS to_process
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_object_type, from_object_id, to_object_type, to_object_id)
                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_object_type, to_object_type)
                         WHERE tp.to_process = 1
                           AND tf.to_process = 1

                     UNION ALL

                        SELECT 'Parent-to-Child' AS edge_type,
                               c2p.to_object_type      AS from_object_type,
                               c2p.to_object_id        AS from_object_id,
                               c2p.from_object_type    AS to_object_type,
                               c2p.from_object_id      AS to_object_id,
                               c2p.context             AS context,
                               1 AS to_process
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_object_type, from_object_id, to_object_type, to_object_id)
                    INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_object_type, to_object_type)
                         WHERE tp.to_process = 1
                           AND tf.to_process = 1
                    """]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'template':

                # Target cache table
                target_table = 'template'

                # List of evaluation columns
                eval_columns = ['from_object_type', 'to_object_type']

                # Build query (base)
                sql_query = f"""
                """

            #-------------------------#
            # Process resulting query #
            #-------------------------#

            # Evaluate query
            if 'eval' in actions:

                # Build evaluation query
                sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"

                # Print evaluation query
                if 'print' in actions:
                    print_sql(sql_query_eval, title='8nZVFGbc')

                # Execute evaluation query
                out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='8nZVFGbc')
                df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                if len(df) > 0:
                    print_dataframe(df, title=f'\n🔍 Evaluation results for view: "{view_name}"')

            # Execute commit
            if 'commit' in actions:

                # Print status
                sysmsg.trace(f"⚙️  Processing view: '{view_name}' ...")

                # Fetch target table column names
                target_table_columns = db.get_column_names(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=target_table)

                # Remove row_id (if exists)
                if 'row_id' in target_table_columns:
                    target_table_columns.remove('row_id')

                # Build commit query
                sql_query_commit = f"\tREPLACE INTO {glbcfg.schema_graph_cache_test}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"

                # Print commit query
                if 'print' in actions:
                    print_sql(sql_query_commit, title='Mn0to7TQ')

                # Execute commit query in shell
                # Note: 'execute_query_in_chunks' doesn't work with UNIONs
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, verbose=False, query_id='Mn0to7TQ')

        # Apply formula from SQL file
        def apply_formulas_from_folder(self, local_path, verbose=False):

            # Print status
            sysmsg.info(f"🧪 📝 Apply formulas of type '{local_path}'.")

            # Fetch list of batch formulas to execute
            list_of_files = sorted(glob.glob(f'{SQL_FORMULAS_PATH}/{local_path}/formula.*.sql'))

            # Load active node/edge types from Airflow config for type-specific folders
            active_node_types = None
            active_edge_types = None
            active_scores_node_types = None
            if local_path in (
                'calculated_fields/obj',
                'graph_traversals',
                'calculated_fields/obj2obj',
                'calculated_scores/obj2ontology/categories',
                'calculated_scores/obj2ontology/concepts',
                'calculated_scores/obj2ontology/concepts_union',
                'calculated_scores/obj2ontology/categories_union',
                'calculated_scores/degree_scores',
            ):
                config_json = GraphRegistry.Orchestration.TypeFlags().get_config_json()
                if local_path in ('calculated_fields/obj', 'graph_traversals'):
                    active_node_types = {
                        node_type.lower()
                        for node_type, process_fields, _ in config_json['nodes']
                        if process_fields
                    }
                    sysmsg.info(f"   Active node types for fields: {sorted(active_node_types)}")
                if local_path.startswith('calculated_scores/'):
                    active_scores_node_types = {
                        node_type.lower()
                        for node_type, _, process_scores in config_json['nodes']
                        if process_scores
                    }
                    sysmsg.info(f"   Active node types for scores: {sorted(active_scores_node_types)}")
                active_edge_types = {
                    tuple(sorted([src.lower(), dst.lower()]))
                    for src, dst, is_active in config_json['edges']
                    if is_active
                }
                sysmsg.info(f"   Active edge types for fields: {sorted(active_edge_types)}")

            # Loop over and execute all formulas
            for file_path in list_of_files:

                # Extract formula name from file path
                formula_name = re.findall(r'formula\.(.*)\.sql$', file_path)[0]

                # Skip formulas that target inactive node/edge types
                if local_path == 'calculated_fields/obj':
                    formula_node_type = self._get_node_type_from_obj_formula(formula_name)
                    if formula_node_type is not None and formula_node_type not in active_node_types:
                        sysmsg.info(
                            f"⏭️  Skipping obj formula '{formula_name}': "
                            f"node type '{formula_node_type}' is not in active fields node types."
                        )
                        continue
                elif local_path == 'graph_traversals':
                    # For traversals, require every non-ontology object type in the path to be
                    # active for fields. Ontology types (concept/category) are ignored because
                    # their activation is implicit via the object side.
                    formula_node_types = self._get_node_types_from_traversal_formula(formula_name)
                    non_ontology_types = [
                        t for t in formula_node_types
                        if t not in ('concept', 'category')
                    ]
                    inactive_types = [t for t in non_ontology_types if t not in active_node_types]
                    if inactive_types:
                        sysmsg.info(
                            f"⏭️  Skipping traversal formula '{formula_name}': "
                            f"object type(s) {inactive_types} are not in active fields node types."
                        )
                        continue
                elif local_path == 'calculated_fields/obj2obj':
                    formula_edge = self._get_edge_type_from_obj2obj_formula(formula_name)
                    if formula_edge is not None and formula_edge not in active_edge_types:
                        sysmsg.info(
                            f"⏭️  Skipping obj2obj formula '{formula_name}': "
                            f"edge {formula_edge} is not in active fields edge types."
                        )
                        continue
                elif local_path.startswith('calculated_scores/'):
                    scores_filter = self._get_scores_filter_for_calculated_scores_formula(local_path, formula_name)
                    if scores_filter is not None:
                        filter_type, filter_value = scores_filter
                        if filter_type == 'specific_node':
                            if filter_value not in active_scores_node_types:
                                sysmsg.info(
                                    f"⏭️  Skipping calculated-scores formula '{formula_name}': "
                                    f"node type '{filter_value}' is not in active scores node types."
                                )
                                continue
                        elif filter_type == 'any_scores':
                            if not active_scores_node_types:
                                sysmsg.info(
                                    f"⏭️  Skipping calculated-scores formula '{formula_name}': "
                                    f"no active scores node types in the Airflow config."
                                )
                                continue

                self.apply_formula_from_file(file_path=file_path, verbose=verbose)

            # Print status
            sysmsg.success(f"🧪 ✅ Done applying formulas.\n")

        # Helper: extract edge types from a traversal formula filename
        def _get_edge_types_from_traversal_formula(self, formula_name):
            """
            Parses a traversal formula filename and returns the edge types involved.

            Examples:
                '001.unit-person.affiliation'
                    -> [('person', 'unit')]
                '004.person-publication-concept.concept_detection'
                    -> [('person', 'publication'), ('concept', 'publication')]
            """
            parts = formula_name.split('.')
            if len(parts) < 2 or '-' not in parts[1]:
                return []

            path_types = parts[1].split('-')
            edges = []
            for i in range(len(path_types) - 1):
                edges.append(tuple(sorted([path_types[i].lower(), path_types[i + 1].lower()])))
            return edges

        # Helper: extract node types from a traversal formula filename
        def _get_node_types_from_traversal_formula(self, formula_name):
            """
            Parses a traversal formula filename and returns the node/object types in the path.

            Examples:
                '003.publication-concept.concept_detection'
                    -> ['publication', 'concept']
                '004.person-publication-concept.concept_detection'
                    -> ['person', 'publication', 'concept']
            """
            parts = formula_name.split('.')
            if len(parts) < 2 or '-' not in parts[1]:
                return []

            return [t.lower() for t in parts[1].split('-')]

        # Helper: extract edge type from an obj2obj calculated field formula filename
        def _get_edge_type_from_obj2obj_formula(self, formula_name):
            """
            Parses an obj2obj calculated field formula filename and returns the edge type.

            Example:
                'course.person.latest_teaching_assignment_year'
                    -> ('course', 'person')
            """
            parts = formula_name.split('.')
            if len(parts) < 2:
                return None
            return tuple(sorted([parts[0].lower(), parts[1].lower()]))

        # Helper: extract node type from an obj calculated field formula filename
        def _get_node_type_from_obj_formula(self, formula_name):
            """
            Parses an obj calculated field formula filename and returns the node type.

            Example:
                'concept.node_degree'
                    -> 'concept'
            """
            parts = formula_name.split('.')
            if not parts:
                return None
            return parts[0].lower()

        # Helper: determine scores filter for a calculated_scores formula
        def _get_scores_filter_for_calculated_scores_formula(self, local_path, formula_name):
            """
            Returns a scores filter for calculated_scores formulas.

            Scores formulas are governed by the node's process_scores flag in the
            Airflow config (not the edge's process_fields flag).

            Returns:
                ('specific_node', node_type)  -> skip if node_type is not active for scores
                ('any_scores', None)          -> skip if no node is active for scores
                None                          -> cannot determine, run anyway
            """
            if local_path == 'calculated_scores/obj2ontology/categories':
                # formula.{node_type}.concept_sum-scores_aggregation.sql
                parts = formula_name.split('.')
                if len(parts) >= 2:
                    return ('specific_node', parts[0].lower())
                return None

            if local_path == 'calculated_scores/obj2ontology/concepts':
                # Known patterns: formula.{number}.{node_type}.{calculation}.sql
                # The second token is the target node type for the scores calculation.
                parts = formula_name.split('.')
                if len(parts) >= 2:
                    return ('specific_node', parts[1].lower())
                return None

            # Union formulas aggregate concept/category scores produced upstream.
            # Run them whenever any node is active for scores.
            if local_path == 'calculated_scores/obj2ontology/concepts_union':
                return ('any_scores', None)

            if local_path == 'calculated_scores/obj2ontology/categories_union':
                return ('any_scores', None)

            if local_path == 'calculated_scores/degree_scores':
                # Degree scores are global score computations. Run them whenever any
                # node is active for scores (there is no per-edge scores flag).
                return ('any_scores', None)

            return None

        # Apply a single SQL formula identified by a path relative to SQL_FORMULAS_PATH
        def apply_formula_by_path(self, formula_path, actions=('eval',)):
            """
            Resolve a formula path relative to database/formulas and apply it.

            Args:
                formula_path: Path relative to database/formulas, e.g.
                    'traversals/formula.007.course-lecture-slide-concept.concept_detection'.
                actions: Tuple of actions to perform: print, eval, commit.
            """
            # Allow omitting the .sql extension
            if not formula_path.endswith('.sql'):
                formula_path = f"{formula_path}.sql"

            # Resolve folder aliases (e.g. traversals -> graph_traversals)
            parts = formula_path.split('/')
            if parts and parts[0] in SQL_FORMULAS_FOLDER_ALIASES:
                parts[0] = SQL_FORMULAS_FOLDER_ALIASES[parts[0]]
            formula_path = '/'.join(parts)

            full_path = SQL_FORMULAS_PATH / formula_path
            if not full_path.is_file():
                sysmsg.error(f"Formula file not found: {full_path}")
                return
            self.apply_formula_from_file(file_path=str(full_path), actions=actions)

        # Apply formula from SQL file
        def apply_formula_from_file(self, file_path, verbose=False, actions=None):

            # Backward-compatible default actions when called via the legacy verbose API
            if actions is None:
                actions = ('print', 'commit') if verbose else ('commit',)
            actions = tuple(actions)

            # Extract formula name from file path
            formula_name = re.findall(r'formula\.(.*)\.sql$', file_path)[0]

            # Extract formula type from file path
            formula_type = re.findall(r'(.*)\/formula\..*\.sql$', file_path.replace(f'{SQL_FORMULAS_PATH}/', ''))[0]

            # Print status
            if 'print' in actions or 'eval' in actions or verbose:
                sysmsg.trace(f"⚙️  Applying formula: '{formula_name}' ...")

            # Read the SQL formula
            with open(file_path, 'r') as file:
                sql_formula = file.read()

            # Fill in the template variables
            for db_schema_name in glbcfg.mysql_schema_names['xaas_coresrv']:
                sql_formula = sql_formula.replace(f'[[{db_schema_name}]]', glbcfg.mysql_schema_names['xaas_coresrv'][db_schema_name])

            # Determine type of formula (safe inserts vs direct execution)
            if (
                   'INSERT'  in sql_formula
                or 'REPLACE' in sql_formula
                or 'UPDATE'  in sql_formula
                or 'DELETE'  in sql_formula
                or 'CREATE'  in sql_formula
                or 'DROP'    in sql_formula
                or 'ALTER'   in sql_formula
            ):
                execution_type = 'direct execution'
            elif 'SELECT' in sql_formula:
                execution_type = 'safe inserts'
            else:
                sysmsg.warning(f"Could not determine type of formula (safe inserts vs direct execution).")
                return

            #-------------------------------#
            # Execute based on formula type #
            #-------------------------------#

            # Execute as safe inserts?
            if execution_type == 'safe inserts':

                # Define key and update column names, and target table (object calculated fields)
                if formula_type == 'calculated_fields/obj':
                    target_table      = 'Data_N_Object_T_CalculatedFields'
                    key_column_names  = ['object_type', 'object_id']
                    upd_column_names  = ['field_language', 'field_name', 'field_value']
                    eval_column_names = ['object_type']

                # Define key and update column names, and target table (object-to-object calculated fields)
                elif formula_type == 'calculated_fields/obj2obj':
                    target_table      = 'Data_N_Object_N_Object_T_CalculatedFields'
                    key_column_names  = ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context']
                    upd_column_names  = ['field_language', 'field_name', 'field_value']
                    eval_column_names = ['from_object_type', 'to_object_type']

                # Execute SQL formula as safe inserts
                db.execute_query_as_safe_inserts(
                    engine_name       = 'xaas_coresrv',
                    schema_name       = glbcfg.schema_graph_cache_test,
                    table_name        = target_table,
                    query             = sql_formula,
                    key_column_names  = key_column_names,
                    upd_column_names  = upd_column_names,
                    eval_column_names = eval_column_names,
                    actions           = actions,
                    verbose           = 'print' in actions,
                    query_id          = 'D9NxAGY2'
                )

            # Execute as direct execution?
            elif execution_type == 'direct execution':

                # Print the formula when requested (avoid double-printing during commit)
                if 'print' in actions:
                    print_sql(sql_formula, title='Neg00cQJ')

                # Execute the SQL formula when requested
                if 'commit' in actions:
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_formula, verbose=False, query_id='Neg00cQJ')

            # Unknown execution type
            else:
                sysmsg.warning(f"Could not determine type of formula (safe inserts vs direct execution).")
                return

        # Update cache table from SQL batch formula
        def cache_update_from_batch_formula(self, formula_name, verbose=False):

            # Print status
            sysmsg.trace(f"⚙️  Applying batch formula: '{formula_name.split('.')[1]}' ...")

            # Read the SQL formula
            with open(f'{SQL_FORMULAS_PATH}/batch/formula.{formula_name}.sql', 'r') as file:
                sql_formula = file.read()

            # Fill in the template variables
            for db_schema_name in glbcfg.mysql_schema_names['xaas_coresrv']:
                sql_formula = sql_formula.replace(f'[[{db_schema_name}]]', glbcfg.mysql_schema_names['xaas_coresrv'][db_schema_name])

            # Execute the SQL formula
            db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_formula, verbose=verbose, query_id='N5ABPyWm')

        # Update cache table from SQL calculated field formula
        def cache_update_from_calculated_field(self, object_type_key, field_name, verbose=False):

            # Print status
            sysmsg.trace(f"⚙️  Applying calculated field formula: {object_type_key} / {field_name} ...")

            # Does object type key refer to node or edge?
            if type(object_type_key) is str: # Node

                # Define target cache table
                target_table = 'Data_N_Object_T_CalculatedFields'

                # Define key and update column names
                key_column_names = ['object_type', 'object_id']
                upd_column_names = ['field_language', 'field_name', 'field_value']

                # Read the SQL formula
                with open(f'{SQL_FORMULAS_PATH}/calculated_fields/obj/formula.{object_type_key}.{field_name}.sql', 'r') as file:
                    sql_formula = file.read()

            elif type(object_type_key) is tuple and len(object_type_key)==2: # Edge

                # Define target cache table
                target_table = 'Data_N_Object_N_Object_T_CalculatedFields'

                # Define key and update column names
                key_column_names = ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context']
                upd_column_names = ['field_language', 'field_name', 'field_value']

                # Read the SQL formula
                with open(f'{SQL_FORMULAS_PATH}/calculated_fields/obj2obj/formula.{object_type_key[0]}.{object_type_key[1]}.{field_name}.sql', 'r') as file:
                    sql_formula = file.read()

            else:
                sysmsg.error(f"Invalid object_type_key: {object_type_key}. Must be string (node) or tuple of two strings (edge).")
                return

            # Check if SQL formula is valid (return otherwise)
            if 'SELECT' not in sql_formula.upper():
                sysmsg.warning(f"Invalid SQL formula.")
                return

            # Fill in the template variables
            for db_schema_name in glbcfg.mysql_schema_names['xaas_coresrv']:
                sql_formula = sql_formula.replace(f'[[{db_schema_name}]]', glbcfg.mysql_schema_names['xaas_coresrv'][db_schema_name])

            # Execute SQL formula as safe inserts
            db.execute_query_as_safe_inserts(
                engine_name       = 'xaas_coresrv',
                schema_name       = glbcfg.schema_graph_cache_test,
                table_name        = target_table,
                query             = sql_formula,
                key_column_names  = key_column_names,
                upd_column_names  = upd_column_names,
                eval_column_names = ['object_type'],
                actions           = ('print', 'commit') if verbose else ('commit'),
                verbose           = verbose,
                query_id          = 'ApR0YLT2'
            )

        # Update cached lecture timestamps
        def cache_lecture_timestamps(self):

            sql_query = f"""
          REPLACE INTO {glbcfg.schema_graph_cache_test}.Edges_N_Lecture_N_Concept_T_Timestamps AS

                SELECT t2.from_object_type       AS object_type,
                       t2.from_object_id         AS object_id,
                       t3.concept_id             AS concept_id,
                       MAX(t3.score)             AS detection_score,
                       t4.field_value            AS detection_time_hms,
                       t5.field_value            AS detection_timestamp

                  FROM {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged t1

            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
                    ON (   t1.object_type,    t1.object_id)
                     = (t2.to_object_type, t2.to_object_id)

            INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t3
                    ON (t2.from_object_type, t2.from_object_id)
                     = (     t3.object_type,      t3.object_id)

            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t4
                    ON (  t2.to_object_type,   t2.to_object_id, t2.from_object_type, t2.from_object_id)
                     = (t4.from_object_type, t4.from_object_id,   t4.to_object_type,   t4.to_object_id)

            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t5
                    ON (  t2.to_object_type,   t2.to_object_id, t2.from_object_type, t2.from_object_id)
                     = (t5.from_object_type, t5.from_object_id,   t5.to_object_type,   t5.to_object_id)

                 WHERE t1.object_type = 'Lecture'
                   AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
                   AND t3.object_type = 'Slide'
                   AND (t4.from_object_type, t4.to_object_type, t4.field_name) = ('Lecture', 'Slide', 'time_hms')
                   AND (t5.from_object_type, t5.to_object_type, t5.field_name) = ('Lecture', 'Slide', 'timestamp')
                   AND t1.to_process = 1

              GROUP BY t2.from_object_type,
                       t2.from_object_id,
                       t3.concept_id,
                       t5.field_value
            """

        # Core function that updates the object-to-object scores matrix
        # TODO: Widget-Concept/Catagory tables
        # Also, might need to avoid creating edges where from_id=to_id (self-edges). TBD...
        def calculate_scores_matrix(self, from_object_type, to_object_type, actions=()):

            # Print action specific status
            if len(actions) == 0:
                sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                sysmsg.info(f"🚀 📝 Nothing to do.")
                return
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # Re-arrange from/to object types alphabetically (since undirected scores)
            from_object_type, to_object_type = sorted([from_object_type, to_object_type])

            # Generate scores matrix table name
            scores_matrix_table_name_gbc = get_scores_matrix_table_name(from_object_type, to_object_type, gbc_or_as='GBC')

            # Initialise SQL queries
            sql_eval_query, sql_commit_query = None, None

            # Ignore edge types: Object-to-Concept/Category/Curated area (not including Category-to-Category)
            # These have already been calculated in Object-Concept and Object-Category tables, and can be used later
            if (from_object_type in ('Category','Concept','Curated area') or to_object_type in ('Category','Concept','Curated area')) and not (from_object_type, to_object_type) == ('Category', 'Category'):
                return

            # Calculate all other edge types, including Category-to-Category
            else:

                # Build evaluation query
                if from_object_type == to_object_type:
                    # Total count = from[to_process=1] x to[all]
                    sql_eval_query = f"""
                        SELECT object_type AS from_object_type, object_type AS to_object_type, SUM(to_process) * COUNT(*) AS estimated_n_to_process
                          FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                         WHERE object_type = '{from_object_type}'
                    """
                else:
                    # Total count = from[to_process=1] x to[all] + from[all] x to[to_process=1]
                    sql_eval_query = f"""
                        SELECT t1.object_type AS from_object_type, t2.object_type AS to_object_type,
                               t1.n_to_process * t2.n_count + t1.n_count * t2.n_to_process AS estimated_n_to_process
                          FROM (SELECT '_' AS id, object_type, SUM(to_process) AS n_to_process, COUNT(*) AS n_count
                                  FROM  {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                 WHERE object_type = '{from_object_type}') t1
                    INNER JOIN (SELECT '_' AS id, object_type, SUM(to_process) AS n_to_process, COUNT(*) AS n_count
                                  FROM {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired
                                 WHERE object_type = '{to_object_type}') t2
                         USING (id)
                    """

                # Define 'from' and 'to' object tables
                from_object_table = f"Nodes_N_{'Category' if from_object_type=='Category' else 'Object'}"
                to_object_table   = f"Nodes_N_{'Category' if   to_object_type=='Category' else 'Object'}"

                # Generate commit SQL query
                sql_commit_query = f"""
                     REPLACE INTO {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_gbc}
                                  (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process)

                           SELECT e1.object_type     AS from_object_type,
                                  e1.object_id       AS from_object_id,
                                  e2.object_type     AS to_object_type,
                                  e2.object_id       AS to_object_id,
                                  SUM(e1.score*e2.score) AS score, 1 AS to_process

                             FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Concept_T_FinalScores e1
                       INNER JOIN {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Concept_T_FinalScores e2
                      FORCE INDEX (idx_concept_type_proc_score)
                            USING (concept_id)

                            WHERE e1.object_type = "{from_object_type}"
                              AND e2.object_type = "{  to_object_type}"

                              AND e1.to_process = 1
                              AND e2.to_process = 1

                              AND e1.score >= 0.1
                              AND e2.score >= 0.1

                              AND ((e1.object_type = e2.object_type AND e1.object_id < e2.object_id) OR (e1.object_type != e2.object_type))

                         GROUP BY e1.object_type, e1.object_id,
                                  e2.object_type, e2.object_id

                           HAVING COUNT(DISTINCT e1.concept_id) >= 4
                              AND SUM(e1.score*e2.score) >= 0.1
                    """

            # Evaluate query
            if 'eval' in actions and sql_eval_query is not None:

                # Check if evaluation query is available
                if sql_eval_query is None:
                    sysmsg.warning(f"No evaluation query available for ({from_object_type}, {to_object_type}).")
                else:
                    # Print evaluation query
                    if 'print' in actions:
                        print('\nExecuting query:')
                        print_sql(sql_eval_query, title='f3LmCRzV')

                    # Execute evaluation query
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_eval_query, query_id='f3LmCRzV')
                    df = pd.DataFrame(out, columns=['from_object_type', 'to_object_type', 'n_to_process'])
                    if len(df) > 0:
                        print_dataframe(df, title=f'\n🔍 Evaluation results for ({from_object_type}, {to_object_type})')

            # Print commit query
            if 'print' in actions:
                print_sql(sql_commit_query, title='wAbL4D8i')

            # Commit query
            if 'commit' in actions and sql_commit_query is not None:

                # Execute commit query
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_commit_query, query_id='wAbL4D8i')

        # Core function that consolidates the object-to-object scores matrix (adjusted/bounded scores)
        def consolidate_scores_matrix(self, from_object_type, to_object_type, update_averages=False, score_thr=0.1, actions=()):

            # Re-arrange from/to object types alphabetically (since undirected scores)
            from_object_type, to_object_type = sorted([from_object_type, to_object_type])

            # Generate scores matrix table names (both group-by-concept and adjusted-scores)
            scores_matrix_table_name_gbc = get_scores_matrix_table_name(from_object_type, to_object_type, gbc_or_as='GBC')
            scores_matrix_table_name_as  = get_scores_matrix_table_name(from_object_type, to_object_type, gbc_or_as='AS')

            # Print action specific status
            if len(actions) == 0:
                sysmsg.warning(f"No actions specified. Nothing to do.")
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # If edge types: Object-to-Concept/Category/Curated area (not including Category-to-Category),
            # Copy from pre-calculated tables
            if (from_object_type in ('Category', 'Concept', 'Curated area') or to_object_type in ('Category', 'Concept', 'Curated area')) and not (from_object_type, to_object_type) in (('Category', 'Category'), ('Concept', 'Concept'), ('Curated area', 'Curated area')):

                # Get ontology type and object type
                ontology_type = from_object_type if from_object_type in ('Category', 'Concept', 'Curated area') else to_object_type

                # Generate SQL query
                sql_query = f"""
                    REPLACE INTO {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process)
                          SELECT object_type    AS from_object_type,
                                 object_id      AS from_object_id,
                                 '{ontology_type}' AS to_object_type,
                                 {ontology_type.lower().replace(' ','_')}_id AS to_object_id,
                                 score, to_process
                            FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_{ontology_type.title().replace(' ','')}_T_FinalScores
                           WHERE to_process = 1
                             AND score >= {score_thr}
                """

            # Concept-to-concept tables
            elif (from_object_type, to_object_type) == ('Concept', 'Concept'):

                # Generate SQL query
                sql_query = f"""
                    REPLACE INTO {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process)
                          SELECT 'Concept'        AS from_object_type,
                                 from_id          AS from_object_id,
                                 'Concept'        AS to_object_type,
                                 to_id            AS to_object_id,
                                 normalised_score AS score,
                                 s1.to_process OR s2.to_process AS to_process
                            FROM {glbcfg.schema_ontology}.Edges_N_Concept_N_Concept_T_Undirected c

                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired s1
                              ON s1.object_id = c.from_id

                      INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired s2
                              ON s2.object_id = c.to_id

                           WHERE s1.object_type = 'Concept'
                             AND s2.object_type = 'Concept'
                             AND (s1.to_process = 1 OR s2.to_process = 1)
                             AND normalised_score >= {score_thr}
                """

            # Calculate all other edge types, including Category-to-Category (to fetch from GBC table)
            else:

                # Check if update averages is requested
                if update_averages:

                    # Generate SQL query for average score calculation (if needed)
                    sql_query_avg = f"""
                    REPLACE INTO {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                                (from_object_type, to_object_type, avg_score, n_rows)
                          SELECT from_object_type, to_object_type,
                                 AVG(score) AS avg_score, COUNT(*) AS n_rows
                            FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_gbc}
                           WHERE from_object_type = '{from_object_type}'
                             AND to_object_type   = '{to_object_type}'
                        GROUP BY from_object_type, to_object_type
                    """

                    # Print average score calculation query
                    if 'print' in actions:
                        print_sql(sql_query_avg, title='gs1ieZYM')

                    # Execute average score calculation
                    if 'commit' in actions:
                        # pass # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
                        # TODO: Add --no-avg-recalc flag
                        db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_avg, verbose='print' in actions, query_id='gs1ieZYM')

                # Check first if an average score is available, return otherwise
                sql_query_check = f"""
                    SELECT * FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                     WHERE (from_object_type, to_object_type)
                         = ('{from_object_type}', '{to_object_type}');
                """
                out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_check, query_id='fTaH8sTj')
                if len(out) == 0:
                    sysmsg.warning(f'\nNo average score calculation available for ({from_object_type}, {to_object_type})')
                    return

                # Generate SQL query for adjusted scores calculation
                sql_query = f"""
                    REPLACE INTO {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process)
                          SELECT t.from_object_type, t.from_object_id, t.to_object_type, t.to_object_id, t.score, t.to_process
                            FROM (SELECT gb.from_object_type, gb.from_object_id,
                                           gb.to_object_type,   gb.to_object_id,
                                         (2/(1 + EXP(-gb.score/(4 * av.avg_score))) - 1) AS score, gb.to_process
                                    FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_gbc} gb
                              INNER JOIN {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG av
                                      ON gb.from_object_type = av.from_object_type
                                     AND   gb.to_object_type = av.to_object_type
                                   WHERE gb.to_process = 1
                                     AND gb.from_object_type = '{from_object_type}'
                                     AND gb.to_object_type   = '{to_object_type}'
                                 ) t
                           WHERE t.score >= {score_thr}
                """

            # If commit action is requested, execute the query
            if 'commit' in actions:

                # Print the commit query
                if 'print' in actions:
                    print_sql(sql_query, title='qHE7tP6J')

                # Execute commit query
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query, verbose='print' in actions, query_id='qHE7tP6J')

    #-----------------------------------------------------------#
    # Subclass definition: GraphIndex Management (SQL Database) #
    #-----------------------------------------------------------#
    class IndexDB():

        # Class constructor
        def __init__(self, engine_name='xaas_coresrv'):
            # db = GraphDB()
            self.engine_name = engine_name
            self.cachebuilder = self.CacheBuildup()
            self.pageprofile = self.PageProfile()
            self.idocs = {}
            self.idoclinks = {}
            self.list_of_index_tables = db.get_tables_in_schema(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graphsearch'])


            # Get all doc types available
            doc_types_available = dynsql.doc_types

            # Initialize IndexDoc objects for all doc types
            # for doc_type in [t[0] for t in [re.findall(r'Index_D_([^_]*)$', table_name) for table_name in self.list_of_index_tables] if len(t)>0]:
            # for doc_type in [t[0] for t in [re.findall(r'IndexBuildup_Fields_Docs_([^_]*)$', table_name) for table_name in db.get_tables_in_schema(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'])] if len(t)>0]:
            for doc_type in doc_types_available:
                self.idocs[doc_type] = self.IndexDocs(doc_type=doc_type)

            # Get all doc-link types available
            doclink_types_available = [(t[0],t[1],'SEM') for t in dynsql.doclink_types_sem] + [(t[0],t[1],'ORG') for t in dynsql.doclink_types_org]

            # Initialize IndexDocLinks objects for all doc-link types
            # for doc_type, link_type, link_subtype in [t[0] for t in [re.findall(r'Index_D_([^_]*)_L_([^_]*)_T_([^_]*)$', table_name) for table_name in self.list_of_index_tables] if len(t)>0]:
            for doc_type, link_type, link_subtype in doclink_types_available:
                if doc_type not in self.idoclinks:
                    self.idoclinks[doc_type] = {}
                if link_type not in self.idoclinks[doc_type]:
                    self.idoclinks[doc_type][link_type] = {}
                self.idoclinks[doc_type][link_type][link_subtype] = self.IndexDocLinks(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype)

        # Apply cache builder methods
        def build(self, actions=()):
            self.cachebuilder.build_all(actions=actions)

        # Apply all patching methods
        def patch(self, actions=()):
            self.pageprofile.patch(actions=actions)
            self.docs_patch_all(actions=actions)
            self.doclinks_vertical_patch_all(actions=actions)
            self.doclinks_horizontal_patch_all(actions=actions)

        # Patch all index doc tables on graphsearch test
        def docs_patch_all(self, actions=()):

            # Print status
            sysmsg.info(f"🚜 📝 Vertical patch of doc index tables [actions: {actions}].")

            # Print action specific status
            if len(actions)==0 and actions!=('settle',):
                sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit', 'settle'.")
                sysmsg.info(f"🚜 📝 Nothing to do.")
                return
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # Fetch typeflags config JSON
            doc_types_in_config, _ = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='fields')

            # Get all doc types available
            doc_types_available = dynsql.doc_types

            # Keep only intersection of available and to-process types
            doc_types_to_process = [t for t in doc_types_available if t in doc_types_in_config]

            # Append doclinks for which links equal doc types to be processed
            doc_types_to_process += [t for t in doc_types_available if t in doc_types_in_config]

            # Process links in both directions
            doc_types_to_process += [t for t in doc_types_to_process if t in doc_types_available]

            # Clean and sort list
            doc_types_to_process = sorted(list(set(doc_types_to_process)))

            # Check if empty
            if len(doc_types_to_process)==0:
                sysmsg.warning(f"No type flags found for 'docs'. Nothing to do.")

            # If not empty, proceed
            else:

                # Print status
                sysmsg.trace(f"Patch tables in '{glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}' and '{glbcfg.mysql_schema_names[self.engine_name]['es_cache']}' schemas.")

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [D-P-DB] The following tables will be affected:')
                for t in doc_types_to_process:
                    print(f" - {glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}.Index_D_{t}")
                print('')

                # Loop over doc types
                with tqdm(doc_types_to_process, unit='doc type') as pb:
                    for doc_type in pb:

                        # Print status
                        pb.set_description(f"⚙️  [🐬 GraphSearch DB] [D-P-DB] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc table (graphsearch tables)
                        self.idocs[doc_type].patch(actions=actions)

                        # Print status
                        pb.set_description(f"⚙️  [⚡️ ElasticSearch] [D-P-ES] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc table (elasticsearch cache)
                        self.idocs[doc_type].patch_elasticsearch(actions=actions)

                        # Print status
                        pb.set_description(f"⚙️  [♻️ Airflow] [D-P-AF] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Update Airflow 'Operations_N_Object_T_FieldsChanged' table
                        if 'settle' in actions:
                            self.idocs[doc_type].airflow_update(verbose=('print' in actions))

            # Print status
            sysmsg.success(f"🚜 ✅ Done vertical patching of doc index tables.\n")

        # Patch all index doc-link tables on graphsearch test
        def doclinks_vertical_patch_all(self, actions=()):

            # Print status
            sysmsg.info(f"🚜 📝 Vertical patch of doc-link index tables [actions: {actions}].")

            # Print action specific status
            if len(actions) == 0:
                sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                sysmsg.info(f"🚜 📝 Nothing to do.")
                return
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # Fetch typeflags config JSON
            doc_types_in_config, doclink_types_in_config = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='fields', return_symmetric=True)

            # Check if empty
            if len(doc_types_in_config)==0 and len(doclink_types_in_config)==0:
                sysmsg.warning(f"No type flags found for 'docs' nor 'doc-links'.")
                sysmsg.info(f"🚜 Nothing to do.\n")
                return

            # If not empty, proceed
            else:

                # Get all doc-link types available
                doclink_types_available = [(t[0],t[1],'SEM') for t in dynsql.doclink_types_sem] + [(t[0],t[1],'ORG') for t in dynsql.doclink_types_org]

                # Keep only intersection of available and to-process types
                doclink_types_to_process = [t for t in doclink_types_available if t[:2] in doclink_types_in_config and t[2]=='ORG']

                # Append doclinks for which links equal doc types to be processed
                doclink_types_to_process += [t for t in doclink_types_available if t[1] in doc_types_in_config]

                # Process links in both directions
                doclink_types_to_process += [(t[1], t[0], t[2]) for t in doclink_types_to_process if (t[1], t[0], t[2]) in doclink_types_available]

                # Clean and sort list
                doclink_types_to_process = sorted(list(set(doclink_types_to_process)))

                # Print status
                sysmsg.trace(f"Patch tables in '{glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}' schema.")

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [DL-VP-DB] The following tables will be affected:')
                for t in doclink_types_to_process:
                    print(f" - {glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}.Index_D_{t[0]}_L_{t[1]}_T_{t[2]}")
                print('')

                # Loop over doc-link types
                with tqdm(doclink_types_to_process, unit='doc-link type') as pb:
                    for doc_type, link_type, link_subtype in pb:

                        # Check if table type exists (continue otherwise)
                        if link_subtype not in self.idoclinks[doc_type][link_type]:
                            sysmsg.warning(f"Doc-link type not found: {doc_type} --> {link_type} [{link_subtype}]. Skipping.")
                            continue

                        # Print status
                        pb.set_description(f"⚙️  [🐬 GraphSearch DB] [DL-VP-DB] Processing doc-link type: {doc_type} --> {link_type} [{link_subtype}]".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc-link table (mysql)
                        if link_subtype == 'SEM':
                            self.idoclinks[doc_type][link_type][link_subtype].vertical_patch(actions=actions)
                        elif link_subtype == 'ORG':
                            # Process links in both directions
                            self.idoclinks[doc_type][link_type][link_subtype].vertical_patch_parentchild(actions=actions)

                # Extract only ElasticSearch tuples (no distinction between SEM and ORG)
                doclink_types_to_process_es = sorted(list(set([t[:2] for t in doclink_types_to_process])))

                # Print status
                sysmsg.trace(f"Patch tables in '{glbcfg.mysql_schema_names[self.engine_name]['es_cache']}' schema.")

                # Print list of affected tables
                print('\n[⚡️ ElasticSearch] [DL-VP-ES] The following tables will be affected:')
                for t in doclink_types_to_process_es:
                    print(f" - {glbcfg.mysql_schema_names[self.engine_name]['es_cache']}.Index_D_{t[0]}_L_{t[1]}")
                print('')

                # Loop over doc-link types
                with tqdm(doclink_types_to_process, unit='doc-link type') as pb:
                    for doc_type, link_type, _ in pb:

                        # Check if table type exists (continue otherwise)
                        link_subtype = 'SEM' if 'SEM' in self.idoclinks[doc_type][link_type] else 'ORG'
                        if link_subtype not in self.idoclinks[doc_type][link_type]:
                            continue

                        # Print status
                        # TODO: For some reason elasticsearch_cache.Index_D_Lecture_L_Course is being processed twice 
                        pb.set_description(f"⚙️  [⚡️ ElasticSearch] [DL-VP-ES] Processing doc-link type: {doc_type} --> {link_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc-link table (elasticsearch)
                        self.idoclinks[doc_type][link_type][link_subtype].vertical_patch_elasticsearch(actions=actions)

            # Print status
            sysmsg.success(f"🚜 ✅ Done vertical patching of doc-link index tables.\n")

        # Patch all index doc-link tables on graphsearch test
        def doclinks_horizontal_patch_all(self, actions=()):

            # Print status
            sysmsg.info(f"🚜 📝 Horizontal patch of doc-link index tables [actions: {actions}].")

            # Print action specific status
            if len(actions) == 0:
                sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                sysmsg.info(f"🚜 📝 Nothing to do.")
                return
            elif 'eval' in actions and 'commit' not in actions:
                sysmsg.warning(f"Executing in evaluation mode only.")

            # Fetch typeflags config JSON
            doc_types_in_config_FIELDS, doclink_types_in_config_FIELDS = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='fields', return_symmetric=True)
            doc_types_in_config_SCORES, doclink_types_in_config_SCORES = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='scores', return_symmetric=True)

            # Combine both
            # TODO: Note this is not optimal, as it's including other types
            doc_types_in_config = sorted(list(set(doc_types_in_config_FIELDS + doc_types_in_config_SCORES)))
            doclink_types_in_config = sorted(list(set(doclink_types_in_config_FIELDS + doclink_types_in_config_SCORES)))

            # Check if empty
            if len(doc_types_in_config)==0 and len(doclink_types_in_config)==0:
                sysmsg.warning(f"No type flags found for 'docs' nor 'doc-links'")
                sysmsg.info(f"🚜 Nothing to do.\n")
                return

            # If not empty, proceed
            else:

                # Get all doc-link types available $$$$$$$$$$$$$$$$$$$$
                doclink_types_available = [(t[0],t[1],'SEM') for t in dynsql.doclink_types_sem] + [(t[0],t[1],'ORG') for t in dynsql.doclink_types_org]

                # Keep only intersection of available and to-process types
                doclink_types_to_process = [t for t in doclink_types_available if t[:2] in doclink_types_in_config and t[2]=='ORG']

                # Append doclinks for which links equal doc types to be processed
                doclink_types_to_process += [t for t in doclink_types_available if t[1] in doc_types_in_config]

                # Process links in both directions
                doclink_types_to_process += [(t[1], t[0], t[2]) for t in doclink_types_to_process if (t[1], t[0], t[2]) in doclink_types_available]

                # Clean and sort list
                doclink_types_to_process = sorted(list(set(doclink_types_to_process)))

                # Print status
                sysmsg.trace(f"Patch tables in '{glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}' schema.")

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [DL-HP-DB] The following tables will be affected:')
                for t in doclink_types_to_process:
                    print(f" - {glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}.Index_D_{t[0]}_L_{t[1]}_T_{t[2]}")
                print('')

                # Loop over doc-link types
                with tqdm(doclink_types_to_process, unit='doc-link type') as pb:
                    for doc_type, link_type, link_subtype in pb:

                        # Check if table type exists (continue otherwise)
                        if link_subtype not in self.idoclinks[doc_type][link_type]:
                            continue

                        # Print status
                        pb.set_description(f"⚙️  [🐬 GraphSearch DB] [DL-HP-DB] Processing doc-link type: {doc_type} --> {link_type} [{link_subtype}]".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc-link table
                        self.idoclinks[doc_type][link_type][link_subtype].horizontal_patch(actions=actions)

                # Extract only ElasticSearch tuples (no distinction between SEM and ORG)
                doclink_types_to_process_es = sorted(list(set([t[:2] for t in doclink_types_to_process])))

                # Print status
                sysmsg.trace(f"Patch tables in '{glbcfg.mysql_schema_names[self.engine_name]['es_cache']}' schema.")

                # Print list of affected tables
                print('\n[⚡️ ElasticSearch] [DL-HP-ES] The following tables will be affected:')
                for t in doclink_types_to_process_es:
                    print(f" - {glbcfg.mysql_schema_names[self.engine_name]['es_cache']}.Index_D_{t[0]}_L_{t[1]}")
                print('')

                # Loop over doc-link types
                with tqdm(doclink_types_to_process_es, unit='doc-link type') as pb:
                    for doc_type, link_type in pb:

                        # Check if table type exists (continue otherwise)
                        link_subtype = 'SEM' if 'SEM' in self.idoclinks[doc_type][link_type] else 'ORG'
                        if link_subtype not in self.idoclinks[doc_type][link_type]:
                            continue

                        # Print status
                        pb.set_description(f"⚙️  [⚡️ ElasticSearch] [DL-HP-ES] Processing doc-link type: {doc_type} --> {link_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Patch index doc-link table (elasticsearch)
                        self.idoclinks[doc_type][link_type][link_subtype].horizontal_patch_elasticsearch(actions=actions)

            # Print status
            sysmsg.success(f"🚜 ✅ Done horizontal patching of doc-link index tables.\n")

        # Helper: create mixed (org+sem) view for a single doc-link type
        @staticmethod
        def _create_mixed_view_for_doclink(doc_type, link_type, test_mode=False):

            # Generate table names
            table_name_org = f"Index_D_{doc_type}_L_{link_type}_T_ORG"
            table_name_sem = f"Index_D_{doc_type}_L_{link_type}_T_SEM"
            table_name_mix = f"Index_D_{doc_type}_L_{link_type}_T_MIX"

            # Ignore concept search table (special case)
            if table_name_sem == 'Index_D_Lecture_L_Concept_T_SEM_Search':
                sysmsg.warning(f"Skipping Index_D_Lecture_L_Concept_T_SEM_Search table.")
                return

            # Get list of columns for SEM table
            list_of_columns_sem = db.get_column_names(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_sem)

            # Remove row_id
            if 'row_id' in list_of_columns_sem:
                list_of_columns_sem.remove('row_id')

            # Fix list of columns for ORG table
            list_of_columns_org = ['degree_score' if c == 'semantic_score' else c for c in list_of_columns_sem]

            # Generate SQL query
            SQLQuery = f"""
            CREATE OR REPLACE VIEW {glbcfg.schema_graphsearch_test}.{table_name_mix} AS

                            SELECT {', '.join(list_of_columns_org)}, (s.row_rank) AS adjusted_row_rank
                              FROM {glbcfg.schema_graphsearch_test}.{table_name_org} s
                        INNER JOIN (SELECT doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                      FROM {glbcfg.schema_graphsearch_test}.{table_name_org}
                                  GROUP BY doc_type, doc_id) o
                             USING (doc_type, doc_id)
                             WHERE doc_id IN (SELECT doc_id FROM {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{doc_type} WHERE to_process = 1)

                         UNION ALL

                            SELECT {', '.join(list_of_columns_sem)}, (s.row_rank + COALESCE(o.max_row_rank, 0)) AS adjusted_row_rank
                              FROM {glbcfg.schema_graphsearch_test}.{table_name_sem} s
                         LEFT JOIN (SELECT doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                      FROM {glbcfg.schema_graphsearch_test}.{table_name_org}
                                  GROUP BY doc_type, doc_id) o
                             USING (doc_type, doc_id)
                             WHERE (s.doc_type, s.doc_id, s.link_type, s.link_id)
                                      NOT IN (SELECT doc_type, doc_id, link_type, link_id FROM {glbcfg.schema_graphsearch_test}.{table_name_org})
                               AND doc_id IN (SELECT doc_id FROM {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{doc_type} WHERE to_process = 1)

                          ORDER BY doc_id ASC, adjusted_row_rank ASC;
            """

            if test_mode:
                print(SQLQuery)
            else:
                db.execute_query_in_shell(engine_name='xaas_coresrv', query=SQLQuery, query_id='tb1Vdfyq')

        # Helper: ensure mixed view exists for a single doc-link type
        @staticmethod
        def _ensure_mixed_view_exists(doc_type, link_type, test_mode=False):

            # Generate table names
            table_name_org = f"Index_D_{doc_type}_L_{link_type}_T_ORG"
            table_name_sem = f"Index_D_{doc_type}_L_{link_type}_T_SEM"
            table_name_mix = f"Index_D_{doc_type}_L_{link_type}_T_MIX"

            # Only create/use MIX views for pairs explicitly listed in mixed-scoring-tuples.
            # Otherwise a stale ORG table can cause an on-demand MIX view to be created for a
            # pair that should be SEM-only, leading to expensive and incorrect ES cache queries.
            configured_mix_pairs = set(dynsql.doclink_types_mix)
            if (doc_type, link_type) not in configured_mix_pairs:
                # If a stale MIX view already exists, drop it so horizontal_patch_elasticsearch
                # falls back to the correct ORG/SEM branch instead of querying the invalid view.
                table_exists_mix = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_mix, exclude_views=False)
                if table_exists_mix:
                    sysmsg.info(f"🗑️  Dropping stale MIX view for {doc_type} --> {link_type} (not in configured SEM∩ORG pairs).")
                    db.execute_query_in_shell(
                        engine_name='xaas_coresrv',
                        query=f"DROP VIEW IF EXISTS {glbcfg.schema_graphsearch_test}.{table_name_mix}",
                        query_id='xY7gHv2K'
                    )
                else:
                    # sysmsg.trace(
                    #     f"Skipping MIX view for {doc_type} --> {link_type}: not in configured SEM∩ORG pairs."
                    # )
                    pass
                return False

            # Generate 'table exists' flags
            table_exists_org = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_org)
            table_exists_sem = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_sem)
            table_exists_mix = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_mix, exclude_views=False)

            # Cannot create MIX if either source table is missing
            if not (table_exists_org and table_exists_sem):
                # sysmsg.trace(
                #     f"Cannot create MIX view for {doc_type} --> {link_type}. "
                #     f"ORG exists: {table_exists_org}, SEM exists: {table_exists_sem}."
                # )
                return False

            # Already exists
            if table_exists_mix:
                return True

            # Create it
            sysmsg.info(f"🛠️  Creating missing MIX view for {doc_type} --> {link_type}.")
            GraphRegistry.IndexDB._create_mixed_view_for_doclink(doc_type, link_type, test_mode=test_mode)
            return True

        # Create mixed (org+sem) views for ElasticSearch indexing
        def create_mixed_views(self, drop_existing=False, test_mode=False):

            # Get mixed doclink tuples from config_scores.json
            doclinks_to_process = sorted(list(set(dynsql.doclink_types_mix)))

            # Loop over all doclink tuples
            for doc_type, link_type in tqdm(doclinks_to_process):

                # Generate table names
                table_name_org = f"Index_D_{doc_type}_L_{link_type}_T_ORG"
                table_name_sem = f"Index_D_{doc_type}_L_{link_type}_T_SEM"
                table_name_mix = f"Index_D_{doc_type}_L_{link_type}_T_MIX"

                # Generate 'table exists' flags
                table_exists_org = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_org)
                table_exists_sem = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_sem)
                table_exists_mix = db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=table_name_mix)

                # Only process if both SEM and ORG tables exist
                if not (table_exists_org and table_exists_sem):
                    sysmsg.warning(f"Skipping doc-link type: {doc_type} --> {link_type}. SEM table exists: {table_exists_sem}. ORG table exists: {table_exists_org}. MIX table exists: {table_exists_mix}.")
                    continue

                # Drop existing if requested
                if table_exists_mix and not drop_existing:
                    sysmsg.trace(f"MIX view already exists for {doc_type} --> {link_type}, skipping.")
                    continue

                # Create the mixed view
                GraphRegistry.IndexDB._create_mixed_view_for_doclink(doc_type, link_type, test_mode=test_mode)

        # TODO: Copy patched data to production cache schema [NEEDS WORK]
        def copy_patches_to_prod(self):
            return

        # Delete loose ends from the Operations_N_Object_T_NoLooseEnds table and optionally update it
        def delete_loose_ends(self, engine_name='xaas_coresrv', update_loose_ends=False, include_scores_matrix=False, refresh_graph=False, actions=()):

            #---------------------------------------------------------#
            # Step 0: Remove orphaned row_rank = 99 placeholder rows  #
            #---------------------------------------------------------#

            index_tables = db.get_tables_in_schema(
                engine_name = engine_name,
                schema_name = glbcfg.schema_graphsearch_test,
                use_regex   = [r'^Index_D_[^_]*_L_[^_]*_T_SEM+$']
            )

            # Exclude private/internal backup tables that start with an underscore
            index_tables = [t for t in index_tables if not t.startswith('_')]

            # Print list of affected tables
            print('\n[🐬 GraphSearch DB] [CLEAN] The following tables will be affected:')
            for t in index_tables:
                print(f" - {glbcfg.schema_graphsearch_test}.{t}")
            print('')

            # Loop over index tables and check for orphaned row_rank=99 rows
            for table_name in index_tables:

                # Generate evaluation query
                sql_query_eval = f"""
                   SELECT doc_type, link_type, COUNT(*) AS n_to_delete
                     FROM {glbcfg.schema_graphsearch_test}.{table_name}
                    WHERE row_rank = 99
                 GROUP BY doc_type, link_type
                """

                # Execute evaluation query (if requested)
                if 'eval' in actions or 'commit' in actions:
                    out = db.execute_query(
                        engine_name=engine_name,
                        query=sql_query_eval,
                        query_id='rr99cnt'
                    )

                    # Create a DataFrame to display the results
                    df = pd.DataFrame(out, columns=['doc_type', 'link_type', 'n_to_delete'])


                    # Print the DataFrame if it contains any rows and delete loose ends from the current table if 'commit' action is specified
                    if len(df) > 0:

                        # Print the DataFrame with a title indicating the evaluation results for the current table
                        print_dataframe(df, title=f'🔍 Evaluation results for table: "{glbcfg.schema_graphsearch_test}.{table_name}"')

                        # Execute commit query to delete loose ends if 'commit' action is specified
                        if 'commit' in actions:

                            # Trace message
                            print(f"🔥 Deleting loose ends from table: '{glbcfg.schema_graphsearch_test}.{table_name}'.")

                            # Generate delete query to remove rows with row_rank=99 from the current table
                            sql_query_delete = f"DELETE FROM {glbcfg.schema_graphsearch_test}.{table_name} WHERE row_rank = 99"

                            # Execute the delete query in the database shell with the specified engine name, verbosity, and query ID
                            db.execute_query_in_shell(engine_name=engine_name, query=sql_query_delete, verbose='print' in actions, query_id='rr99del')

            #-----------------------------------#
            # Step 1: Calculate connected graph #
            #-----------------------------------#

            from sqlalchemy import text

            cache_table_path = f"{glbcfg.schema_graph_cache_test}.Operations_N_Object_T_LargestConnectedGraph"

            # Local helper to escape a SQL string literal by doubling single quotes.
            def _sql_string(value):
                return str(value).replace("'", "''")

            # Local helper to check whether the cached largest-component graph exists and is non-empty.
            def _graph_cache_exists():
                try:
                    tables = db.get_tables_in_schema(
                        engine_name = engine_name,
                        schema_name = glbcfg.schema_graph_cache_test,
                        use_regex   = [r'^Operations_N_Object_T_LargestConnectedGraph$']
                    )
                    if not tables:
                        return False
                    result = db.execute_query(
                        engine_name=engine_name,
                        query=f"SELECT 1 FROM {cache_table_path} LIMIT 1",
                        query_id='cache_chk'
                    )
                    return bool(result)
                except Exception:
                    return False

            # Local helper to write a list of (object_type, object_id) tuples to the
            # Operations_N_Object_T_LargestConnectedGraph table in chunks.
            def _write_largest_component(nodes, chunk_size=10000):
                lines = [f"TRUNCATE TABLE {cache_table_path};"]
                for i in range(0, len(nodes), chunk_size):
                    chunk = nodes[i:i + chunk_size]
                    values = ", ".join(
                        f"('{_sql_string(object_type)}', '{_sql_string(object_id)}')"
                        for object_type, object_id in chunk
                    )
                    lines.append(f"INSERT INTO {cache_table_path} (object_type, object_id) VALUES {values};")

                file_path = '/tmp/sql_query_upd_largest_component.sql'
                with open(file_path, 'w') as f:
                    f.write("\n".join(lines))

                db.execute_query_from_file(
                    engine_name=engine_name,
                    file_path=file_path,
                    verbose='print' in actions
                )

            if not refresh_graph and _graph_cache_exists():
                print(f"\n[🐬 GraphSearch DB] Using cached graph from {cache_table_path}.")
            else:
                if refresh_graph:
                    print(f"\n[🐬 GraphSearch DB] Refreshing cached graph in {cache_table_path}.")
                else:
                    print(f"\n[🐬 GraphSearch DB] Graph cache not found or empty; computing and storing in {cache_table_path}.")

                # Union-Find (Disjoint Set Union) structure for connected components.
                # Far more memory efficient than building a full NetworkX graph.
                class UnionFind:
                    __slots__ = ('parent', 'rank')
                    def __init__(self):
                        self.parent = []
                        self.rank = []

                    def add(self):
                        idx = len(self.parent)
                        self.parent.append(idx)
                        self.rank.append(0)
                        return idx

                    def find(self, x):
                        parent = self.parent
                        while parent[x] != x:
                            parent[x] = parent[parent[x]]  # path halving
                            x = parent[x]
                        return x

                    def union(self, x, y):
                        x_root = self.find(x)
                        y_root = self.find(y)
                        if x_root == y_root:
                            return
                        rank = self.rank
                        if rank[x_root] < rank[y_root]:
                            self.parent[x_root] = y_root
                        elif rank[x_root] > rank[y_root]:
                            self.parent[y_root] = x_root
                        else:
                            self.parent[y_root] = x_root
                            rank[x_root] += 1

                uf = UnionFind()
                node_to_index = {}
                nodes_list = []  # index -> (doc_type, doc_id)

                def _get_node_index(node):
                    idx = node_to_index.get(node)
                    if idx is None:
                        idx = uf.add()
                        node_to_index[node] = idx
                        nodes_list.append(node)
                    return idx

                # Local helper to stream rows from a SELECT without buffering the whole result set in RAM.
                # Uses SQLAlchemy's stream_results mode with server-side cursors.
                def _stream_rows(engine_name, query, fetch_size=10000, query_id=None):
                    engine = db.engine[engine_name]
                    connection = engine.connect()
                    try:
                        exec_conn = connection.execution_options(stream_results=True)
                        result = exec_conn.execute(text(query))
                        while True:
                            chunk = result.fetchmany(fetch_size)
                            if not chunk:
                                break
                            for row in chunk:
                                yield row
                    finally:
                        connection.close()

                # Get the list of tables in the schema using the defined regex mapping
                list_of_tables = db.get_tables_in_schema(
                    engine_name = engine_name,
                    schema_name = glbcfg.schema_graphsearch_test,
                    use_regex   = [r'^Index_D_[^_]+$']
                )

                # Exclude private/internal backup tables that start with an underscore
                list_of_tables = [t for t in list_of_tables if not t.startswith('_')]

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [EXTRACT NODES] The following tables will be searched:')
                for t in list_of_tables:
                    print(f" - {glbcfg.schema_graphsearch_test}.{t}")
                print('')

                # Loop over each table in the list of tables, in order to build nodes first
                for table_name in tqdm(list_of_tables, desc='Extract nodes', unit='table'):

                    # Generate SQL query to extract doc_type, doc_id from the current table
                    sql_query_extract = f"""
                        SELECT doc_type, doc_id
                            FROM {glbcfg.schema_graphsearch_test}.{table_name}
                    """

                    # Optionally count rows for a progress bar (small overhead, better UX)
                    try:
                        count_row = db.execute_query(
                            engine_name=engine_name,
                            query=f"SELECT COUNT(*) FROM {glbcfg.schema_graphsearch_test}.{table_name}",
                            query_id='jkwrt35c'
                        )
                        total_rows = count_row[0][0] if count_row else 0
                    except Exception:
                        total_rows = None

                    # Stream rows and register nodes
                    for row in tqdm(
                        _stream_rows(engine_name=engine_name, query=sql_query_extract, query_id='jkwrt35'),
                        total=total_rows,
                        desc=f'  {table_name}',
                        unit='row',
                        leave=False
                    ):
                        doc_type, doc_id = row
                        _get_node_index((doc_type, doc_id))

                # Get the list of tables in the schema using the defined regex mapping
                list_of_tables = db.get_tables_in_schema(
                    engine_name = engine_name,
                    schema_name = glbcfg.schema_graphsearch_test,
                    use_regex   = [r'^Index_D_[^_]+_L_.+']
                )

                # Exclude private/internal backup tables that start with an underscore
                list_of_tables = [t for t in list_of_tables if not t.startswith('_')]

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [EXTRACT EDGES] The following tables will be searched:')
                for t in list_of_tables:
                    print(f" - {glbcfg.schema_graphsearch_test}.{t}")
                print('')

                # Loop over each table in the list of tables, in order to build edges next
                for table_name in tqdm(list_of_tables, desc='Extract edges', unit='table'):

                    # Generate SQL query to extract doc_type, doc_id, link_type, and link_id from the current table
                    sql_query_extract = f"""
                        SELECT doc_type, doc_id, link_type, link_id
                          FROM {glbcfg.schema_graphsearch_test}.{table_name}
                    """

                    # Optionally count rows for a progress bar (small overhead, better UX)
                    try:
                        count_row = db.execute_query(
                            engine_name=engine_name,
                            query=f"SELECT COUNT(*) FROM {glbcfg.schema_graphsearch_test}.{table_name}",
                            query_id='GGg429c'
                        )
                        total_rows = count_row[0][0] if count_row else 0
                    except Exception:
                        total_rows = None

                    # Stream rows and union endpoints directly
                    for row in tqdm(
                        _stream_rows(engine_name=engine_name, query=sql_query_extract, query_id='GGg429'),
                        total=total_rows,
                        desc=f'  {table_name}',
                        unit='row',
                        leave=False
                    ):
                        doc_type, doc_id, link_type, link_id = row
                        from_idx = _get_node_index((doc_type, doc_id))
                        to_idx = _get_node_index((link_type, link_id))
                        uf.union(from_idx, to_idx)

                # Determine component sizes and find the largest component
                component_sizes = {}
                for idx in range(len(nodes_list)):
                    root = uf.find(idx)
                    component_sizes[root] = component_sizes.get(root, 0) + 1

                sorted_components = sorted(component_sizes.items(), key=lambda x: x[1], reverse=True)
                print(f"\n[🐬 GraphSearch DB] Found {len(sorted_components)} connected components with sizes: {[size for _, size in sorted_components]}")

                largest_root, largest_size = sorted_components[0]
                print(f"\n[🐬 GraphSearch DB] Largest connected component size: {largest_size}")

                # Extract node types and ids for largest connected component
                largest_component_nodes = [nodes_list[idx] for idx in range(len(nodes_list)) if uf.find(idx) == largest_root]

                # Write largest component to the cache table
                _write_largest_component(largest_component_nodes)

            # Generate SQL query to update loose ends in the Operations_N_Object_T_NoLooseEnds table
            sql_query_upd_loose_ends = f"""
            TRUNCATE TABLE {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_NoLooseEnds;
               INSERT INTO {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_NoLooseEnds (object_type, object_id)
                    SELECT object_type, object_id FROM {glbcfg.schema_registry}.Data_N_Object_T_PageProfile;
               INSERT INTO {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_NoLooseEnds (object_type, object_id)
                    SELECT object_type, object_id FROM {glbcfg.schema_lectures}.Data_N_Object_T_PageProfile;
               INSERT INTO {glbcfg.schema_graph_cache_test}.Operations_N_Object_T_NoLooseEnds (object_type, object_id)
                    SELECT object_type, object_id FROM {glbcfg.schema_ontology}.Data_N_Object_T_PageProfile;
            """

            # Execute SQL query to update loose ends
            if update_loose_ends:
                db.execute_query_in_shell(engine_name=engine_name, query=sql_query_upd_loose_ends, verbose='print' in actions, query_id='K42g42')

            # Define regex mapping for each schema to identify relevant tables
            regex_mapping = {
                'airflow'     : [r'^Operations_.*'],
                'graph_cache' : [r'^Data_.*', r'^IndexBuildup_.*', r'^Operations_N_Object_T_Checksums.*', r'^Operations_N_Object_N_Object_T_Checksums.*'],
                'graphsearch' : False,
                'es_cache'    : False,
            }

            # Include scores matrix tables in the graph_cache schema if specified
            if include_scores_matrix:
                regex_mapping['graph_cache'] += [r'^Nodes_N_Object_.*', r'^Edges_N_Object_.*']

            # Loop over each schema key to process tables
            for schema_key, allowed_nodes_table in [
                ('airflow'    , 'Operations_N_Object_T_NoLooseEnds'),
                ('graph_cache', 'Operations_N_Object_T_NoLooseEnds'),
                ('graphsearch', 'Operations_N_Object_T_LargestConnectedGraph'),
                ('es_cache'   , 'Operations_N_Object_T_LargestConnectedGraph')
            ]:

                # Get the schema name from the global configuration
                schema_name = glbcfg.mysql_schema_names['test'][schema_key]

                # Get the list of tables in the schema using the defined regex mapping
                list_of_tables = db.get_tables_in_schema(
                    engine_name = engine_name,
                    schema_name = schema_name,
                    use_regex   = regex_mapping[schema_key]
                )

                # Exclude private/internal backup tables that start with an underscore
                list_of_tables = [t for t in list_of_tables if not t.startswith('_')]

                # Exclude tables containing the string "ProcessingTokens" and "Checksums"
                list_of_tables = [t for t in list_of_tables if "ProcessingTokens" not in t and "Checksums" not in t]

                # Print list of affected tables
                print('\n[🐬 GraphSearch DB] [CLEAN] The following tables will be affected:')
                for t in list_of_tables:
                    print(f" - {schema_name}.{t}")
                print('')

                # Loop over each table in the list of tables
                for table_name in list_of_tables:

                    # Ignore tables that start with an underscore (private/internal tables)
                    if table_name.startswith('_'):
                        continue

                    # Get the list of columns for the current table
                    list_of_columns = db.get_column_names(
                        engine_name = engine_name,
                        schema_name = schema_name,
                        table_name  = table_name
                    )

                    # Define SQL query templates for different table structures (object tables and object-to-object tables)
                    sql_query_obj_template = """
                              {eval_or_commit}
                         FROM {schema_name}.{table_name} t
                    LEFT JOIN {graph_cache_test}.{allowed_nodes_table} n
                           ON t.{col_prefix}_type = n.object_type
                          AND t.{col_prefix}_id   = n.object_id
                        WHERE n.object_id IS NULL
                              {eval_group_by}
                    """

                    # Define SQL query template for object-to-object tables (edges/doclinks)
                    sql_query_obj2obj_template = """
                              {eval_or_commit}
                         FROM {schema_name}.{table_name} t
                    LEFT JOIN {graph_cache_test}.{allowed_nodes_table} n_from
                           ON n_from.object_type = t.{from_prefix}_type
                          AND n_from.object_id   = t.{from_prefix}_id
                    LEFT JOIN {graph_cache_test}.{allowed_nodes_table} n_to
                           ON n_to.object_type = t.{to_prefix}_type
                          AND n_to.object_id   = t.{to_prefix}_id
                        WHERE n_from.object_id IS NULL
                          AND n_to.object_id   IS NULL
                              {eval_group_by}
                    """

                    # Define SQL query templates for commit (single-table DELETE with NOT EXISTS)
                    # Outer table columns are fully qualified to avoid name resolution to the subquery table.
                    sql_query_obj_commit_template = """
                        DELETE FROM {schema_name}.{table_name}
                         WHERE NOT EXISTS (
                               SELECT 1 FROM {graph_cache_test}.{allowed_nodes_table} n
                                WHERE n.object_type = {schema_name}.{table_name}.{col_prefix}_type
                                  AND n.object_id   = {schema_name}.{table_name}.{col_prefix}_id
                         )
                    """

                    sql_query_obj2obj_commit_template = """
                        DELETE FROM {schema_name}.{table_name}
                         WHERE NOT EXISTS (
                               SELECT 1 FROM {graph_cache_test}.{allowed_nodes_table} n_from
                                WHERE n_from.object_type = {schema_name}.{table_name}.{from_prefix}_type
                                  AND n_from.object_id   = {schema_name}.{table_name}.{from_prefix}_id
                         )
                           AND NOT EXISTS (
                               SELECT 1 FROM {graph_cache_test}.{allowed_nodes_table} n_to
                                WHERE n_to.object_type = {schema_name}.{table_name}.{to_prefix}_type
                                  AND n_to.object_id   = {schema_name}.{table_name}.{to_prefix}_id
                         )
                    """

                    # Define SQL query templates for adding unique keys to object and object-to-object tables
                    sql_query_obj_addkey_template     = "ALTER TABLE {schema_name}.{table_name} ADD UNIQUE KEY IF NOT EXISTS object_type_and_id ({col_prefix}_type, {col_prefix}_id);"
                    sql_query_obj2obj_addkey_template = "ALTER TABLE {schema_name}.{table_name} ADD UNIQUE KEY IF NOT EXISTS object_type_and_id ({from_prefix}_type, {from_prefix}_id, {to_prefix}_type, {to_prefix}_id);"

                    # Determine the type of table based on its columns and generate appropriate SQL queries for evaluation, deletion, and adding unique keys
                    if len(set(['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id']) & set(list_of_columns))==4:
                        # print('edges    : ', f"{schema_name}.{table_name}")
                        from_prefix, to_prefix = 'from_object', 'to_object'
                        sql_query_eval   = sql_query_obj2obj_template.format(eval_or_commit="SELECT t.from_object_type, t.to_object_type, COUNT(*) AS n_to_delete", eval_group_by="GROUP BY t.from_object_type, t.to_object_type",
                                                                             schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, from_prefix=from_prefix, to_prefix=to_prefix)
                        sql_query_commit = sql_query_obj2obj_commit_template.format(
                                                                             schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, from_prefix=from_prefix, to_prefix=to_prefix)
                        sql_query_addkey = sql_query_obj2obj_addkey_template.format(
                                                                             schema_name=schema_name, table_name=table_name, from_prefix=from_prefix, to_prefix=to_prefix)

                    # Determine if the table is a doclink table based on its columns and generate appropriate SQL queries for evaluation, deletion, and adding unique keys
                    elif len(set(['doc_type', 'doc_id', 'link_type', 'link_id']) & set(list_of_columns))==4:
                        # print('doclinks : ', f"{schema_name}.{table_name}")
                        from_prefix, to_prefix = 'doc', 'link'
                        sql_query_eval   = sql_query_obj2obj_template.format(eval_or_commit="SELECT t.doc_type, t.link_type, COUNT(*) AS n_to_delete", eval_group_by="GROUP BY t.doc_type, t.link_type",
                                                                             schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, from_prefix=from_prefix, to_prefix=to_prefix)
                        sql_query_commit = sql_query_obj2obj_commit_template.format(
                                                                             schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, from_prefix=from_prefix, to_prefix=to_prefix)
                        sql_query_addkey = sql_query_obj2obj_addkey_template.format(
                                                                             schema_name=schema_name, table_name=table_name, from_prefix=from_prefix, to_prefix=to_prefix)

                    # Determine if the table is an object table based on its columns and generate appropriate SQL queries for evaluation, deletion, and adding unique keys
                    elif len(set(['object_type', 'object_id']) & set(list_of_columns))==2:
                        # print('nodes    : ', f"{schema_name}.{table_name}")
                        col_prefix = 'object'
                        sql_query_eval   = sql_query_obj_template.format(eval_or_commit="SELECT t.object_type, COUNT(*) AS n_to_delete", eval_group_by="GROUP BY t.object_type",
                                                                         schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, col_prefix=col_prefix)
                        sql_query_commit = sql_query_obj_commit_template.format(
                                                                         schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, col_prefix=col_prefix)
                        sql_query_addkey = sql_query_obj_addkey_template.format(
                                                                         schema_name=schema_name, table_name=table_name, col_prefix=col_prefix)

                    # Determine if the table is a doc table based on its columns and generate appropriate SQL queries for evaluation, deletion, and adding unique keys
                    elif len(set(['doc_type', 'doc_id']) & set(list_of_columns))==2:
                        # print('docs     : ', f"{schema_name}.{table_name}")
                        col_prefix = 'doc'
                        sql_query_eval   = sql_query_obj_template.format(eval_or_commit="SELECT t.doc_type, COUNT(*) AS n_to_delete", eval_group_by="GROUP BY t.doc_type",
                                                                         schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, col_prefix=col_prefix)
                        sql_query_commit = sql_query_obj_commit_template.format(
                                                                         schema_name=schema_name, table_name=table_name, graph_cache_test=glbcfg.schema_graph_cache_test, allowed_nodes_table=allowed_nodes_table, col_prefix=col_prefix)
                        sql_query_addkey = sql_query_obj_addkey_template.format(
                                                                         schema_name=schema_name, table_name=table_name, col_prefix=col_prefix)

                    # If none of the above conditions are met, continue to the next table without performing any actions
                    else:
                        continue

                    # Execute evaluation query to count the number of loose ends in the current table and print the results if 'eval' action is specified
                    if 'eval' in actions:

                        # Execute the evaluation query
                        out = db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='DFSHG4tf', verbose='print' in actions)

                        # Determine columns based on the type of table (object, object-to-object, doc, or doclink) and create a DataFrame to display the results
                        if len(set(['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id']) & set(list_of_columns))==4:
                            df = pd.DataFrame(out, columns=['from_object_type', 'to_object_type', 'n_to_delete'])
                        elif len(set(['doc_type', 'doc_id', 'link_type', 'link_id']) & set(list_of_columns))==4:
                            df = pd.DataFrame(out, columns=['doc_type', 'link_type', 'n_to_delete'])
                        elif len(set(['object_type', 'object_id']) & set(list_of_columns))==2:
                            df = pd.DataFrame(out, columns=['object_type', 'n_to_delete'])
                        elif len(set(['doc_type', 'doc_id']) & set(list_of_columns))==2:
                            df = pd.DataFrame(out, columns=['doc_type', 'n_to_delete'])
                        else:
                            df = pd.DataFrame(out, columns=['n_to_delete'])

                        # Print the DataFrame if it contains any rows and delete loose ends from the current table if 'commit' action is specified
                        if len(df) > 0:

                            # Print the DataFrame with a title indicating the evaluation results for the current table
                            print_dataframe(df, title=f'🔍 Evaluation results for table: "{schema_name}.{table_name}"')

                            # Execute commit query to delete loose ends if 'commit' action is specified
                            if 'commit' in actions:

                                # Trace message
                                print(f"🔥 Deleting loose ends from table: '{schema_name}.{table_name}'.")

                                # Execute the commit query
                                db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, query_id='s5DfH2Lk', verbose='print' in actions)

        #-----------------------------------------------------#
        # Sub-subclass definition: Index Cache Buildup Tables #
        #-----------------------------------------------------#
        class CacheBuildup():

            # Class constructor
            def __init__(self):
                pass
                # db = GraphDB()

            # info
            def info(self):
                list_of_tables = db.get_tables_in_schema(
                    engine_name   = 'xaas_coresrv',
                    schema_name   = glbcfg.schema_graph_cache_test,
                    include_views = False,
                    filter_by     = False,
                    use_regex     = [r'^IndexBuildup_Fields_Docs_[^_]*', r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*']
                )
                print('\nList of index buildup tables:')
                print(' - '+'\n - '.join(sorted(list_of_tables)))

            # Update index buildup tables (all)
            def build_all(self, actions=()):

                # Print status
                sysmsg.info(f"🚜 📝 Build up and/or update index field tables on '{glbcfg.schema_graph_cache_test}' [actions: {actions}].")

                # Print action specific status
                if len(actions) == 0:
                    sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                    sysmsg.info(f"🚜 📝 Nothing to do.")
                    return
                elif 'eval' in actions and 'commit' not in actions:
                    sysmsg.warning(f"Executing in evaluation mode only.")

                # Fetch doc types to process
                doc_types_to_process, doclink_types_to_process = GraphRegistry.Orchestration.TypeFlags().get_types_to_process(fields_or_scores='fields')

                # Check if empty
                if len(doc_types_to_process)==0 and len(doclink_types_to_process)==0:
                    sysmsg.warning(f"No type flags found. Nothing to do.")

                # If not empty, proceed
                else:

                    # Print status
                    sysmsg.trace(f"Build tables of type: 'IndexBuildup_Fields_Docs_*'")

                    # Print list of affected tables
                    print('\n[🐬 GraphSearch DB] [B-BD] The following tables will be affected:')
                    for t in doc_types_to_process:
                        print(f" - {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{t}")
                    for t,l in doclink_types_to_process:
                        print(f" - {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Links_ParentChild_{t}_{l}")
                    print('')

                    # Loop over doc types
                    with tqdm(doc_types_to_process, unit='doc type') as pb:
                        for doc_type in pb:

                            # Print status
                            pb.set_description(f"⚙️  [🐬 GraphSearch DB] [B-BD] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                            # Build docs fields
                            self.build_docs_fields(doc_type, actions)

                    # Print status
                    sysmsg.trace(f"Build tables of type: 'IndexBuildup_Fields_Links_ParentChild_*_*'")

                    # Loop over doc-link types
                    with tqdm(doclink_types_to_process, unit='doc-link type') as pb:
                        for doc_type, link_type in pb:

                            # Print status
                            pb.set_description(f"⚙️  [🐬 GraphSearch DB] [B-P2C] Processing doc-link type: '{doc_type} --> {link_type}'".ljust(PBWIDTH)[:PBWIDTH])

                            # Build doc-link fields
                            self.build_links_parentchild(doc_type, link_type, actions)

                # Print status
                sysmsg.success(f"🚜 ✅ Done building up and/or updating index field tables.\n")

            # Update index buildup tables: doc fields
            def build_docs_fields(self, doc_type, actions=()):

                #---------------------------------#
                # Fetch settings from JSON config #
                #---------------------------------#

                # Fetch doc options
                include_code_in_name = idxcfg.settings['options']['include_code_in_name'].get(doc_type, 0)

                # Fetch object's list of custom fields (raw format to preserve language/field split)
                list_of_fields = idxcfg.settings['graphsearch']['fields' ]['docs_raw'].get(doc_type, [])

                #----------------------------#
                # Generate SQL query helpers #
                #----------------------------#

                # Build (and transpose) query helper string matrix
                query_helpers = [list(r) for r in zip(*[
                    (
                        f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language],
                        f"t{k+1}.field_value AS {field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language],
                        f"{' '*6}LEFT JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_T_AllFields t{k+1} ON (t{k+1}.object_type, t{k+1}.object_id, t{k+1}.field_language, t{k+1}.field_name) = ('{doc_type}', p.object_id, '{field_language}', '{field_name}')"
                    )
                    for k, (field_language, field_name) in enumerate([tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields])
                ])]

                # Assign to specific query helper SQL slices
                cachebuildup_obj_fields         =           query_helpers[0]  if len(query_helpers)>0 else []
                sql_slice_field_names           = ', '.join(query_helpers[0]) if len(query_helpers)>0 else ''
                sql_slice_field_values_as_names = ', '.join(query_helpers[1]) if len(query_helpers)>0 else ''
                sql_slice_joins_obj             = '\n'.join(query_helpers[2]) if len(query_helpers)>0 else ''

                # Add trailing comma if necessary
                if len(sql_slice_field_names) > 0:
                    sql_slice_field_names += ', '
                if len(sql_slice_field_values_as_names) > 0:
                    sql_slice_field_values_as_names += ', '

                #----------------------------#

                # Generate SQL query for replacing scores and fields
                sql_query = f"""
                SELECT DISTINCT p.object_type AS doc_type, p.object_id AS doc_id,
                                {include_code_in_name} AS include_code_in_name,
                                {sql_slice_field_values_as_names}
                                COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score,
                                1 AS to_process
                           FROM {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p\n{sql_slice_joins_obj}
                      LEFT JOIN {glbcfg.schema_graph_cache_test}.Nodes_N_Object_T_DegreeScores d
                             ON (p.object_type, p.object_id) = (d.object_type, d.object_id)
                          WHERE p.object_type = '{doc_type}'
                            AND p.to_process = 1
                """

                # Target cache table
                target_table = f'IndexBuildup_Fields_Docs_{doc_type}'

                # List of evaluation columns
                eval_columns = ['doc_type']

                #-------------------------#
                # Process resulting query #
                #-------------------------#

                # Evaluate query
                if 'eval' in actions:

                    # Build evaluation query
                    sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"

                    # Print query
                    if 'print' in actions:
                        print("\nExecuting query:\n")
                        print_sql(sql_query_eval, title='hpFZ8RAT')

                    # Execute evaluation query
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='hpFZ8RAT') # TODO: add verbose
                    df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                    if len(df) > 0:
                        print_dataframe(df, title=f'\n🔍 Evaluation results for doc type: "{doc_type}"')

                # Execute commit
                if 'commit' in actions:

                    # Fetch target table column names
                    target_table_columns = ['doc_type', 'doc_id', 'include_code_in_name'] + cachebuildup_obj_fields + ['degree_score', 'to_process']

                    # Remove row_id (if exists)
                    if 'row_id' in target_table_columns:
                        target_table_columns.remove('row_id')

                    # Build commit query
                    sql_query_commit = f"\tREPLACE INTO {glbcfg.schema_graph_cache_test}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"

                    # Execute commit
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, verbose=('print' in actions), query_id='F1ArYGKd')

            # Update index buildup tables: link parent-child type
            def build_links_parentchild(self, doc_type, link_type, actions=()):

                #---------------------------------#
                # Fetch settings from JSON config #
                #---------------------------------#

                # Fetch organisational object-to-object list of custom fields (raw format to preserve language/field split)
                list_of_fields = idxcfg.settings['graphsearch']['fields' ]['links']['parent_child_raw'].get(doc_type, {}).get(link_type, [])

                # Flip doc-link direction if needed
                doc_type, link_type = sorted([doc_type, link_type])

                #----------------------------#
                # Generate SQL query helpers #
                #----------------------------#

                # Build (and transpose) query helper string matrix
                query_helpers = [list(r) for r in zip(*[
                    (
                        f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language],
                        f"t{k+1}.field_value AS {field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language],
                        f"{' '*6}LEFT JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_N_Object_T_AllFieldsSymmetric t{k+1} ON (t{k+1}.from_object_type, t{k+1}.from_object_id, t{k+1}.to_object_type, t{k+1}.to_object_id, t{k+1}.field_language, t{k+1}.field_name) = ('{doc_type}', s.from_object_id, '{link_type}',   s.to_object_id, '{field_language}', '{field_name}')"
                    )
                    for k, (field_language, field_name) in enumerate([tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields])
                ])]

                # Assign to specific query helper SQL slices
                cachebuildup_obj2obj_fields     =           query_helpers[0]  if len(query_helpers)>0 else []
                sql_slice_field_names           = ', '.join(query_helpers[0]) if len(query_helpers)>0 else ''
                sql_slice_field_values_as_names = ', '.join(query_helpers[1]) if len(query_helpers)>0 else ''
                sql_slice_joins_obj2obj         = '\n'.join(query_helpers[2]) if len(query_helpers)>0 else ''

                # Add trailing comma if necessary
                if len(sql_slice_field_names) > 0:
                    sql_slice_field_names += ', '
                if len(sql_slice_field_values_as_names) > 0:
                    sql_slice_field_values_as_names += ', '

                #----------------------------#

                # Generate SQL query for replacing scores and fields
                sql_query = f"""
                  SELECT s.from_object_type AS  doc_type, s.from_object_id AS doc_id,
                           s.to_object_type AS link_type, s.to_object_id AS link_id,
                         {sql_slice_field_values_as_names}
                         1 AS to_process
                    FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ParentChildSymmetric s\n{sql_slice_joins_obj2obj}
                   WHERE (s.from_object_type, s.to_object_type) = ('{doc_type}', '{link_type}')
                     AND s.to_process = 1
                """

                # Target cache table
                target_table = f'IndexBuildup_Fields_Links_ParentChild_{doc_type}_{link_type}'

                # Create target table if it doesn't exist
                create_table_if_not_exists(engine_name='xaas_coresrv', schema_name=glbcfg.schema_graph_cache_test, table_name=target_table)

                # List of evaluation columns
                eval_columns = ['doc_type', 'link_type']

                #-------------------------#
                # Process resulting query #
                #-------------------------#

                # Print query
                if 'print' in actions:
                    print_sql(sql_query, title='sfag24G')

                # Evaluate query
                if 'eval' in actions:
                    sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"
                    out = db.execute_query(engine_name='xaas_coresrv', query=sql_query_eval, query_id='6D05nXQL')
                    df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                    if len(df) > 0:
                        print_dataframe(df, title=f'\n🔍 Evaluation results for doc-link type: "{doc_type}-{link_type}"')

                # Execute commit
                if 'commit' in actions:

                    # Fetch target table column names
                    target_table_columns = ['doc_type', 'doc_id', 'link_type', 'link_id'] + cachebuildup_obj2obj_fields + ['to_process']

                    # Remove row_id (if exists)
                    if 'row_id' in target_table_columns:
                        target_table_columns.remove('row_id')

                    # Build commit query
                    sql_query_commit = f"\tREPLACE INTO {glbcfg.schema_graph_cache_test}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"

                    # Execute commit
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, query_id='gEzB7UwD')

        #----------------------------------------------#
        # Sub-subclass definition: Page Profiles Table #
        #----------------------------------------------#
        class PageProfile():

            # Class constructor
            def __init__(self, engine_name='xaas_coresrv'):

                # Assign DB pointer
                # db = GraphDB()

                # Define internal variables
                self.engine_name      = engine_name
                self.table_name       = 'Data_N_Object_T_PageProfile'
                self.key_column_names = ['object_type', 'object_id']

                # Fetch column names to update
                out = db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = glbcfg.mysql_schema_names[self.engine_name]['graph_cache'],
                    table_name  = self.table_name
                )
                self.upd_column_names = [c for c in out if c not in self.key_column_names+['row_id', 'to_process', 'deleted']]

            # ...
            def info(self):
                out = db.execute_query(engine_name='xaas_coresrv', query=f"""
                    SELECT object_type, COUNT(*) AS n_to_process
                    FROM {glbcfg.schema_graph_cache_test}.{self.table_name}
                    WHERE to_process = 1
                    GROUP BY object_type
                """, query_id='8ffjZPFr')
                df = pd.DataFrame(out, columns=['object_type', 'n_to_process'])
                print_dataframe(df, title=f'\n🔍 Evaluation results for page profile')

            # Index > Page Profile > Get engine
            def get_engine(self):
                return self.engine_name

            # Index > Page Profile > Set engine
            def set_engine(self, engine_name):
                self.engine_name = engine_name

            # Index > Page Profile > Create table on selected engine
            def create_table(self, actions=()):
                raise NotImplementedError
                if False:
                    pass
                    # sql_query_create_table = f"""
                    # CREATE TABLE IF NOT EXISTS {glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}.{self.table_name} (
                    #     row_id int NOT NULL AUTO_INCREMENT,
                    #     {', '.join([f'{c} VARCHAR(1)' for c in self.key_column_names])},
                    #     {', '.join([f'{c} VARCHAR(1)' for c in self.upd_column_names])},
                    #     UNIQUE KEY row_id (row_id)
                    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    # """

                    # # Get table type
                    # table_type = get_table_type_from_name(self.table_name)

                    # # Get datatypes
                    # datatypes_json = table_datatypes_json[table_type]
                    # datatypes_json.update(...idx...['data-types'])

                    # # Get keys
                    # keys_json = table_keys_json[table_type]
                    # keys_json.update(...idx...['data-keys'])

                    # if 'print' in actions:
                    #     print(sql_query_create_table)
                    #     rich.print_json(data=datatypes_json)
                    #     rich.print_json(data=keys_json)

                    # if 'commit' in actions:
                    # db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_create_table)
                    # db.apply_datatypes(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graphsearch'], table_name=self.index_table_name, datatypes_json=datatypes_json)
                    # db.apply_keys(     engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graphsearch'], table_name=self.index_table_name, keys_json=keys_json)

            #==================#
            # General patching #
            #==================#

            # Index > Page Profile > General patching > Generate snapshot
            def snapshot(self, rollback_date=False, actions=()):

                return
                # # Generate SQL query
                # SQLQuery = f"""
                # INSERT IGNORE INTO {glbcfg.schema_graph_cache_test}.IndexRollback_PageProfile
                #                 (rollback_date, {', '.join(column_names)})
                # SELECT DISTINCT '{rollback_date}' AS rollback_date, p.{', p.'.join(column_names)}
                #             FROM {glbcfg.schema_graphsearch_test}.Data_N_Object_T_PageProfile p
                #         INNER JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile c
                #             USING (object_type, object_id)
                #             WHERE c.to_process = 1
                #             AND ({' OR '.join([f'p.{c} != c.{c}' for c in column_names])});
                # """

            # Index > Page Profile > General patching > Insert new rows, update existing fields (graphsearch test)
            def patch(self, actions=()):

                # Print status
                sysmsg.info(f"🚜 📝 Patch page profile table on 'graphsearch_test' [actions: {actions}].")

                # Print action specific status
                if len(actions) == 0:
                    sysmsg.warning(f"No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
                    sysmsg.info(f"🚜 📝 Nothing to do.")
                    return
                elif 'eval' in actions and 'commit' not in actions:
                    sysmsg.warning(f"Executing in evaluation mode only.")

                # Generate SQL query
                sql_query = f"""
                    \t\t     SELECT {', '.join([f'p.{k}' for k in self.key_column_names])}{', ' if len(self.upd_column_names)>0 else ''}{', '.join(self.upd_column_names)}
                    \t\t       FROM {glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{self.table_name} p
                    \t\t INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged fc
                    \t\t      USING (object_type, object_id)
                    \t\t INNER JOIN {glbcfg.schema_airflow}.Operations_N_Object_T_TypeFlags tf
                    \t\t      USING (object_type)
                    \t\t      WHERE tf.flag_type  = 'fields'
                    \t\t        AND  p.to_process = 1
                    \t\t        AND fc.to_process = 1
                    \t\t        AND tf.to_process = 1
                """

                # Print status
                if 'commit' in actions:
                    sysmsg.trace(f"⚙️  Processing page profile ...")

                # Execute query
                db.execute_query_as_safe_inserts(
                    engine_name       = self.engine_name,
                    schema_name       = glbcfg.mysql_schema_names[self.engine_name]['graphsearch'],
                    table_name        = self.table_name,
                    query             = sql_query,
                    key_column_names  = self.key_column_names,
                    upd_column_names  = self.upd_column_names,
                    eval_column_names = ['object_type'],
                    actions           = actions,
                    query_id          = 'Mp7U7rMW'
                )

                # Print status
                sysmsg.success(f"🚜 ✅ Done patching page profile table.\n")

            # Index > Page Profile > General patching > Roll back to previous state
            def rollback(self, actions=()):
                pass

        #-------------------------------------------#
        # Sub-subclass definition: Index Docs Table #
        #-------------------------------------------#
        class IndexDocs():

            # Class constructor
            def __init__(self, doc_type, engine_name='xaas_coresrv'):

                # Assign DB pointer
                # db = GraphDB()

                # Define internal variables
                self.engine_name        = engine_name
                self.doc_type           = doc_type
                self.buildup_table_name = f'IndexBuildup_Fields_Docs_{doc_type}'
                self.index_table_name   = f'Index_D_{doc_type}'
                self.key_column_names   = ['doc_type', 'doc_id']

                # Create buildup and graphsearch tables if they don't exist
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_graph_cache_test, table_name=self.buildup_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_graphsearch_test, table_name=self.index_table_name)

                # Fetch column names to update
                out = db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = glbcfg.mysql_schema_names[self.engine_name]['graph_cache'],
                    table_name  = self.buildup_table_name
                )
                self.upd_column_names = [c for c in out if c not in self.key_column_names+['row_id', 'to_process']]

                # Fetch object fields and options from index config
                self.include_code_in_name     = idxcfg.settings['options' ]['include_code_in_name'].get(self.doc_type, 0)
                self.graphsearch_obj_fields   = idxcfg.settings['graphsearch'  ]['fields' ]['docs'].get(self.doc_type, [])
                self.elasticsearch_obj_fields = idxcfg.settings['elasticsearch']['fields' ]['docs'].get(self.doc_type, [])
                self.elasticsearch_filters    = idxcfg.settings['elasticsearch']['filters']['docs'].get(self.doc_type, [])

            # Index > Docs > Table info
            def info(self):
                print('\nSelected table:', self.index_table_name)
                out = db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = glbcfg.mysql_schema_names[self.engine_name]['graphsearch'],
                    table_name  = self.index_table_name
                )
                print('\nList of columns:')
                print(' - '+'\n - '.join(out))

            # Index > Docs > Get engine
            def get_engine(self):
                return self.engine_name

            # Index > Docs > Set engine
            def set_engine(self, engine_name):
                self.engine_name = engine_name

            #==================#
            # General patching #
            #==================#

            # Index > Docs > General patching > Generate snapshot
            def snapshot(self, rollback_date=False, actions=()):
                raise NotImplementedError

            # Index > Docs > General patching > Insert new rows, update existing fields (graphsearch test)
            def patch(self, actions=()):

                # Full table paths
                cache_schema_name  = glbcfg.mysql_schema_names[self.engine_name]['graph_cache']
                buildup_table_name = f"IndexBuildup_Fields_Docs_{self.doc_type}"
                target_schema_name = glbcfg.mysql_schema_names[self.engine_name]['graphsearch']
                target_table_name  = f"Index_D_{self.doc_type}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=cache_schema_name,  table_name=buildup_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name)

                # Generate evaluation query
                upd_column_compare = [
                    ('t.degree_score', 'n.degree_score')
                ] + [(f't.{c}', f'n.{c}') for c in self.graphsearch_obj_fields]

                # Build comparison conditions
                compare_conditions = '    '+'\n\t\t\t\t\t OR '.join([
                    f"""COALESCE({t_col}, "__null__") != COALESCE({src_expr}, "__null__")"""
                    for t_col, src_expr in upd_column_compare
                ])

                # Generate evaluation query
                sql_query_eval_1 = f"""
                      SELECT COUNT(*) AS n_total,
                             COALESCE(SUM(\n\t\t\t\t\t{compare_conditions}
                             ), 0) AS n_patch
                        FROM {cache_schema_name}.Data_N_Object_T_PageProfile p

                   LEFT JOIN {target_schema_name}.{target_table_name} t
                          ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)

                  INNER JOIN {cache_schema_name}.{buildup_table_name} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)

                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 1
                """

                # Generate evaluation query
                sql_query_eval_2 = f"""
                      SELECT COUNT(*) AS n_total,
                             COALESCE(SUM(\n\t\t\t\t\t{compare_conditions}
                             ), 0) AS n_patch
                        FROM {cache_schema_name}.Data_N_Object_T_PageProfile p

                   LEFT JOIN {target_schema_name}.{target_table_name} t
                          ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)

                  INNER JOIN {cache_schema_name}.{buildup_table_name} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)

                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 0
                         AND n.to_process = 1
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Execute and validate the evaluation query
                    out_1 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_1, query_id='Rj0R4w2q[1/2]')
                    out_1 = out_1 if type(out_1) is list else [[0,0]]

                    # Execute and validate the evaluation query
                    out_2 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_2, query_id='Rj0R4w2q[2/2]')
                    out_2 = out_2 if type(out_2) is list else [[0,0]]

                    # Combine the results of both evaluation queries
                    out = [[out_1[0][0] + out_2[0][0], out_1[0][1] + out_2[0][1]]]

                    # Extract evalutation parameters
                    rows_to_process, rows_to_patch = out[0]

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_process, rows_to_patch = 0, 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval_1)
                        print_sql(sql_query_eval_2)

                    # Print the evaluation results
                    if rows_to_process + rows_to_patch > 0:
                        df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_schema_name}.{target_table_name}:')
                    if rows_to_patch == 0 and 'print' in actions:
                        sysmsg.warning(f"No rows to patch in table '{target_schema_name}.{target_table_name}'.")

               # Update column names
                upd_column_names = [
                    'include_code_in_name',
                    'degree_score'
                ] + self.graphsearch_obj_fields

                # Update column values
                upd_column_values = [
                    'n.include_code_in_name',
                    'n.degree_score',
                ] + [f'n.{c}' for c in self.graphsearch_obj_fields]

                # Generate commit query
                sql_query_commit_1 = f"""
                     SELECT n.doc_type, n.doc_id, {', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])}
                       FROM {cache_schema_name}.Data_N_Object_T_PageProfile p
                 INNER JOIN {cache_schema_name}.{buildup_table_name} n
                         ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                      WHERE p.object_type = '{self.doc_type}'
                        AND p.to_process = 1
                """

                # Generate commit query
                sql_query_commit_2 = f"""
                     SELECT n.doc_type, n.doc_id, {', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])}
                       FROM {cache_schema_name}.Data_N_Object_T_PageProfile p
                 INNER JOIN {cache_schema_name}.{buildup_table_name} n
                         ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                      WHERE p.object_type = '{self.doc_type}'
                        AND p.to_process = 0
                        AND n.to_process = 1
                """

                # Print the commit query
                if 'print' in actions:
                    print_sql(sql_query_commit_1, title='T4VTvBv6[1/2]')
                    print_sql(sql_query_commit_2, title='T4VTvBv6[2/2]')

                # Execute the commit query
                if 'commit' in actions:

                    # Return if there are no rows to patch
                    if rows_to_patch == 0:
                        return
                    # Else, execute the query as safe inserts
                    else:
                        for qn, sql_query_commit in enumerate([sql_query_commit_1, sql_query_commit_2], start=1):
                            sysmsg.trace(f"⚙️  Processing page profile (commit query {qn}/2) ...")
                            sysmsg.trace(f"🔥 Executing commit query {qn}/2 on table: '{target_schema_name}.{target_table_name}' ...")

                            # Execute the commit query as safe inserts in chunks.
                            # chunk_filter scopes the boundary discovery to the rows of PageProfile that
                            # satisfy the p-table predicates, so chunks are dense over matching row_ids.
                            # The n.to_process=1 predicate in query 2 is still evaluated inside the query.
                            chunk_filter_by_query = {
                                1: f"object_type = '{self.doc_type}' AND to_process = 1",
                                2: f"object_type = '{self.doc_type}' AND to_process = 0",
                            }
                            db.execute_query_as_safe_inserts_in_chunks(
                                engine_name       = self.engine_name,
                                schema_name       = target_schema_name,
                                table_name        = target_table_name,
                                query             = sql_query_commit,
                                key_column_names  = ['doc_type', 'doc_id'],
                                upd_column_names  = upd_column_names,
                                eval_column_names = ['doc_type'],
                                actions           = actions,
                                table_to_chunk    = f"{cache_schema_name}.Data_N_Object_T_PageProfile",
                                chunk_filter      = chunk_filter_by_query[qn],
                                chunk_size        = 100000,
                                row_id_name       = 'p.row_id',
                                query_id          = f"T4VTvBv6[{qn}/2]"
                            )

            # Index > Docs > General patching > Insert new rows, update existing fields (elascticsearch cache)
            def patch_elasticsearch(self, actions=()):

                # Full table paths
                cache_schema_name       = glbcfg.mysql_schema_names[self.engine_name]['graph_cache']
                buildup_link_table_name = f"IndexBuildup_Fields_Docs_{self.doc_type}"
                target_schema_name      = glbcfg.mysql_schema_names[self.engine_name]['es_cache']
                target_table_name       = f"Index_D_{self.doc_type}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=cache_schema_name,  table_name=buildup_link_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name)

                # Generate evaluation query
                upd_column_compare = [
                    ('t.degree_score', 'n.degree_score'),
                    ('t.short_code'  , 'p.short_code'),
                    ('t.subtype_en'  , 'p.subtype_en'),
                    ('t.subtype_fr'  , 'p.subtype_fr'),
                    ('t.name_en', "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_en_value), p.name_en_value)"),
                    ('t.name_fr', "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_fr_value), p.name_fr_value)"),
                    ('t.short_description_en', 'p.description_short_en_value'),
                    ('t.short_description_fr', 'p.description_short_fr_value'),
                    ('t.long_description_en' , 'p.description_long_en_value'),
                    ('t.long_description_fr' , 'p.description_long_fr_value'),
                ] + [(f't.{c}', f'n.{c}') for c in self.elasticsearch_obj_fields]

                # Build comparison conditions
                compare_conditions = '    '+'\n\t\t\t\t\t OR '.join([
                    f"""COALESCE({t_col}, "__null__") != COALESCE({src_expr}, "__null__")"""
                    for t_col, src_expr in upd_column_compare
                ])

                # Generate evaluation query
                sql_query_eval_1 = f"""
                      SELECT COUNT(*) AS n_total,
                             COALESCE(SUM(\n\t\t\t\t\t{compare_conditions}
                             ), 0) AS n_patch
                        FROM {cache_schema_name}.Data_N_Object_T_PageProfile p

                   LEFT JOIN {target_schema_name}.{target_table_name} t
                          ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)

                  INNER JOIN {cache_schema_name}.{buildup_link_table_name} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)

                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 1
                """

                # Generate evaluation query
                sql_query_eval_2 = f"""
                      SELECT COUNT(*) AS n_total,
                             COALESCE(SUM(\n\t\t\t\t\t{compare_conditions}
                             ), 0) AS n_patch
                        FROM {cache_schema_name}.Data_N_Object_T_PageProfile p

                   LEFT JOIN {target_schema_name}.{target_table_name} t
                          ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)

                  INNER JOIN {cache_schema_name}.{buildup_link_table_name} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)

                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 0
                         AND n.to_process = 1
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval_1, title='4KpdVwsE[1/2]')
                        print_sql(sql_query_eval_2, title='4KpdVwsE[2/2]')

                    # Execute and validate the evaluation query
                    out_1 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_1, query_id='4KpdVwsE[1/2]')
                    out_1 = out_1 if type(out_1) is list else [[0,0]]

                    # Execute and validate the evaluation query
                    out_2 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_2, query_id='4KpdVwsE[2/2]')
                    out_2 = out_2 if type(out_2) is list else [[0,0]]

                    # Build combied out (with sums)
                    out = [[out_1[0][0]+out_2[0][0], out_1[0][1]+out_2[0][1]]]

                    # Extract evalutation parameters
                    rows_to_process, rows_to_patch = out[0]

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_process, rows_to_patch = 0, 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval_1)
                        print_sql(sql_query_eval_2)

                    # Print the evaluation results
                    if rows_to_process + rows_to_patch > 0:
                        df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_schema_name}.{target_table_name}:')
                    if rows_to_patch == 0 and 'print' in actions:
                        sysmsg.warning(f"No rows to patch in table '{target_schema_name}.{target_table_name}'.")

                # Update column names
                upd_column_names = [
                    'degree_score',
                    'short_code',
                    'subtype_en',
                    'subtype_fr',
                    'name_en',
                    'name_fr',
                    'short_description_en',
                    'short_description_fr',
                    'long_description_en',
                    'long_description_fr'
                ] + self.elasticsearch_obj_fields

                # Update column values
                upd_column_values = [
                    'n.degree_score',
                    'p.short_code',
                    'p.subtype_en',
                    'p.subtype_fr',
                    "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_en_value), p.name_en_value)",
                    "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_fr_value), p.name_fr_value)",
                    'p.description_short_en_value',
                    'p.description_short_fr_value',
                    'p.description_long_en_value',
                    'p.description_long_fr_value'
                ] + [f'n.{c}' for c in self.elasticsearch_obj_fields]

                # Generate commit query
                sql_query_commit_1 = f"""
                     SELECT n.doc_type, n.doc_id, {', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])}
                       FROM {cache_schema_name}.Data_N_Object_T_PageProfile p
                 INNER JOIN {cache_schema_name}.{buildup_link_table_name} n
                         ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                      WHERE p.object_type = '{self.doc_type}'
                        AND p.to_process = 1
                """

                # Generate commit query
                sql_query_commit_2 = f"""
                     SELECT n.doc_type, n.doc_id, {', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])}
                       FROM {cache_schema_name}.Data_N_Object_T_PageProfile p
                 INNER JOIN {cache_schema_name}.{buildup_link_table_name} n
                         ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                      WHERE p.object_type = '{self.doc_type}'
                        AND p.to_process = 0
                        AND n.to_process = 1
                """

                # Print the commit query
                if 'print' in actions:
                    print_sql(sql_query_commit_1, title='vdEk9bpn[1/2]')
                    print_sql(sql_query_commit_2, title='vdEk9bpn[2/2]')

                # Execute the commit query
                if 'commit' in actions:

                    # Return if there are no rows to patch
                    if rows_to_patch == 0:
                        return
                    # Else, execute the query as safe inserts
                    else:
                        for qn, sql_query_commit in enumerate([sql_query_commit_1, sql_query_commit_2], start=1):
                            sysmsg.trace(f"⚙️  Processing page profile (commit query {qn}/2) ...")
                            sysmsg.trace(f"🔥 Executing commit query {qn}/2 on table: '{target_schema_name}.{target_table_name}' ...")

                            # Execute the commit query as safe inserts in chunks.
                            # chunk_filter scopes the boundary discovery to the rows of PageProfile that
                            # satisfy the p-table predicates, so chunks are dense over matching row_ids.
                            # The n.to_process=1 predicate in query 2 is still evaluated inside the query.
                            chunk_filter_by_query = {
                                1: f"object_type = '{self.doc_type}' AND to_process = 1",
                                2: f"object_type = '{self.doc_type}' AND to_process = 0",
                            }
                            db.execute_query_as_safe_inserts_in_chunks(
                                engine_name       = self.engine_name,
                                schema_name       = target_schema_name,
                                table_name        = target_table_name,
                                query             = sql_query_commit,
                                key_column_names  = ['doc_type', 'doc_id'],
                                upd_column_names  = upd_column_names,
                                eval_column_names = ['doc_type'],
                                actions           = actions,
                                table_to_chunk    = f"{cache_schema_name}.Data_N_Object_T_PageProfile",
                                chunk_filter      = chunk_filter_by_query[qn],
                                chunk_size        = 100000,
                                row_id_name       = 'p.row_id',
                                query_id          = f"vdEk9bpn[{qn}/2]"
                            )

            # Index > Docs > General patching > Roll back to previous state
            def rollback(self, rollback_date, actions=()):
                raise NotImplementedError

            #=====================================#
            # Airflow, Flag, and Checksum updates #
            #=====================================#

            # Index > Docs > Airflow updates > Update 'Operations_N_Object_T_FieldsChanged' [last_date_cached=NOW, has_expired=0, to_process=0]
            def airflow_update(self, verbose=False):

                # Generate commit query
                sql_query_commit_1 = f"""
                      UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged a
                  INNER JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p
                          ON (a.object_type, a.object_id) = (p.object_type, p.object_id)
                  INNER JOIN {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{self.doc_type} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                         SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 1
                """

                # Generate commit query
                sql_query_commit_2 = f"""
                      UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_FieldsChanged a
                  INNER JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p
                          ON (a.object_type, a.object_id) = (p.object_type, p.object_id)
                  INNER JOIN {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{self.doc_type} n
                          ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                         SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
                       WHERE p.object_type = '{self.doc_type}'
                         AND p.to_process = 0
                         AND n.to_process = 1
                """

                # Execute the commit query
                db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit_1, verbose=verbose, query_id='42vKAJcy[1/2]')
                db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit_2, verbose=verbose, query_id='42vKAJcy[2/2]')

            # Index > Docs > Flags cleanup > Update 'Data_N_Object_T_PageProfile' and 'IndexBuildup_Fields_Docs_*' [to_process=0]
            def flags_cleanup(self, verbose=False):

                # Generate commit query
                sql_query_commit = f"""
                      UPDATE {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile
                         SET to_process = 0
                       WHERE object_type = '{self.doc_type}'
                         AND to_process = 1
                """

                # Execute the commit query
                db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit, verbose=verbose, query_id='P9Caiq8w')

                # Generate commit query
                sql_query_commit = f"""
                      UPDATE {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{self.doc_type}
                         SET to_process = 0
                       WHERE to_process = 1
                """

                # Execute the commit query
                db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit, verbose=verbose, query_id='yJ74cRvU')

        #------------------------------------------------#
        # Sub-subclass definition: Index Doc-Links Table #
        #------------------------------------------------#
        class IndexDocLinks():

            # Class constructor
            def __init__(self, doc_type, link_type, link_subtype, engine_name='xaas_coresrv'):

                # Assign DB pointer
                # db = GraphDB()

                # Define internal variables
                self.doc_type     = doc_type
                self.link_type    = link_type
                self.link_subtype = link_subtype
                self.engine_name  = engine_name
                self.buildup_doc_table_name  = f'IndexBuildup_Fields_Docs_{doc_type}'
                self.buildup_link_table_name = f'IndexBuildup_Fields_Docs_{link_type}'
                self.index_table_name        = f'Index_D_{doc_type}_L_{link_type}_T_{link_subtype.upper()}'
                self.key_column_names        = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id']

                # Create buildup and graphsearch tables if they don't exist
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_graph_cache_test, table_name=self.buildup_doc_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_graph_cache_test, table_name=self.buildup_link_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_graphsearch_test, table_name=self.index_table_name)

                # Fetch doclink settings from index config
                self.graphsearch_obj_fields     = idxcfg.settings['graphsearch'  ]['fields' ]['links']['default'].get(self.link_type, [])
                self.graphsearch_obj2obj_fields = idxcfg.settings['graphsearch'  ]['fields' ]['links']['parent_child'].get(self.doc_type, {}).get(self.link_type, []) if link_subtype.upper() == 'ORG' else []
                self.elasticsearch_obj_fields   = idxcfg.settings['elasticsearch']['fields' ]['links'].get(self.link_type, [])
                self.elasticsearch_filters      = idxcfg.settings['elasticsearch']['filters']['links'].get(self.link_type, [])

            # Index > Doc-Links > Table info
            def info(self):

                print('\nSelected table:', self.index_table_name)
                out = db.get_column_names(
                    engine_name = 'xaas_coresrv',
                    schema_name = f'graphsearch_{self.engine_name}',
                    table_name  = self.index_table_name
                )
                print('\nList of columns:')
                print(' - '+'\n - '.join(out))

            # Index > Doc-Links > Get engine
            def get_engine(self):
                return self.engine_name

            # Index > Doc-Links > Set engine
            def set_engine(self, engine_name):
                self.engine_name = engine_name

            #===================#
            # Vertical patching #
            #===================#

            # ------- Snapshots ------- #

            # Index > Doc-Links > Vertical patching > Generate snapshot
            def vertical_snapshot_parentchild(self, rollback_date=False):
                return NotImplementedError

            # ------- Patching ------- #

            # Index > Doc-Links > Vertical patching > Update custom fields (all types)
            def vertical_patch(self, actions=()):

                # Check if there are fields to patch
                if len(self.graphsearch_obj_fields) == 0:
                    if 'print' in actions:
                        sysmsg.trace(f"No fields to patch for doc-link type '{self.doc_type} --> {self.link_type}'.")
                    return

                # Full table paths
                buildup_link_table_path = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{self.buildup_link_table_name}"
                target_schema_name      = glbcfg.mysql_schema_names[self.engine_name]['graphsearch']
                target_table_name       = self.index_table_name
                target_table_path       = f"{target_schema_name}.{target_table_name}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=self.buildup_link_table_name)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name)

                # Generate evaluation query
                sql_query_eval = f"""
                    SELECT COUNT(*) AS n_total, COALESCE(SUM({' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.graphsearch_obj_fields])}), 0) AS n_patch
                      FROM {buildup_link_table_path} b
                INNER JOIN {target_table_path} i
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                     WHERE b.to_process = 1;
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval, title='hLdNx8Hb')

                    # Execute and validate the evaluation query
                    out = db.execute_query(engine_name=self.engine_name, query=sql_query_eval, query_id='hLdNx8Hb')
                    out = out if type(out) is list else [[0,0]]

                    # Extract evalutation parameters
                    rows_to_process, rows_to_patch = out[0]

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_process, rows_to_patch = 0, 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval)

                    # Print the evaluation results
                    if rows_to_process + rows_to_patch > 0:
                        df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')
                    if rows_to_patch == 0 and 'print' in actions:
                        sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

                # Generate commit query
                sql_query_commit = f"""
                    UPDATE {target_table_path} i
                INNER JOIN {buildup_link_table_path} b
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                       SET {   ', '.join([f'i.{c}  = b.{c}' for c in self.graphsearch_obj_fields])}
                     WHERE b.to_process = 1
                       AND ({' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.graphsearch_obj_fields])});
                """

                # Print the commit query
                if 'print' in actions:
                    print_sql(sql_query_commit, title='FCQgBmb2')

                # Execute the commit query
                if 'commit' in actions:

                    # Return if there are no rows to patch
                    if rows_to_patch == 0:
                        return
                    # Small patches: run directly to avoid execute_query_in_chunks iterating
                    # over a huge sparse row_id range and issuing thousands of shell commands.
                    elif rows_to_patch <= 10000:
                        db.execute_query_in_shell(
                            engine_name = self.engine_name,
                            query       = sql_query_commit,
                            query_id    = 'FCQgBmb2',
                            verbose     = 'print' in actions
                        )
                    # Large patches: keep chunking.
                    # chunk_filter scopes boundary discovery to target rows that have a pending
                    # buildup counterpart, avoiding empty chunks over sparse row_id ranges.
                    else:
                        db.execute_query_in_chunks(
                            engine_name   = self.engine_name,
                            schema_name   = target_schema_name,
                            table_name    = target_table_name,
                            query         = sql_query_commit,
                            chunk_filter  = f"EXISTS (SELECT 1 FROM {buildup_link_table_path} b WHERE (b.doc_type, b.doc_id) = (link_type, link_id) AND b.to_process = 1)",
                            chunk_size    = 10000,
                            row_id_name   = 'i.row_id',
                            show_progress = False,
                            query_id      = 'FCQgBmb2',
                            verbose       = 'print' in actions
                        )

            # Index > Doc-Links > Vertical patching > Update ORG-table specific custom fields
            def vertical_patch_parentchild(self, actions=()):

                # Check if there are fields to patch
                if len(self.graphsearch_obj_fields) == 0:
                    if 'print' in actions:
                        sysmsg.trace(f"No fields to patch for doc-link type '{self.doc_type} --> {self.link_type}'.")
                    return

                # Get unique link direction for obj2obj buildup table
                src,trg = sorted([self.doc_type, self.link_type])

                # Link was flipped? (set flag)
                f = [src,trg] != [self.doc_type, self.link_type]

                # Full table paths
                buildup_link_table_name_obj     = f'IndexBuildup_Fields_Docs_{self.link_type}'
                buildup_link_table_name_obj2obj = f'IndexBuildup_Fields_Links_ParentChild_{src}_{trg}'
                buildup_link_table_path_obj     = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{buildup_link_table_name_obj}"
                buildup_link_table_path_obj2obj = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{buildup_link_table_name_obj2obj}"
                target_schema_name = glbcfg.mysql_schema_names[self.engine_name]['graphsearch']
                target_table_name  = self.index_table_name
                target_table_path  = f"{target_schema_name}.{target_table_name}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=buildup_link_table_name_obj)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=buildup_link_table_name_obj2obj)
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name)

                # Does obj2obj table buildup exists
                if db.table_exists(
                    engine_name = self.engine_name,
                    schema_name = glbcfg.mysql_schema_names[self.engine_name]['graph_cache'],
                    table_name  = buildup_link_table_name_obj2obj
                ):
                    # Set flag: obj2obj table buildup exists
                    e = True
                    # Set flag: obj2obj table has fields to patch
                    ec = len(self.graphsearch_obj2obj_fields)>0
                else:
                    # Set flag: obj2obj table buildup does not exist
                    e = False
                    # Set flag: obj2obj table has no fields to patch
                    ec = 0
                    sysmsg.warning(f"Source table '{buildup_link_table_name_obj2obj}' does not exist.")

                # Generate placeholder for joining obj2obj buildup table (if exists)
                obj2obj_placeholder = ''
                if e is True:
                    # Link notation map
                    m = {False:'doc', True:'link'}
                    # Generate placeholder SQL chunk
                    obj2obj_placeholder = f"""
                 LEFT JOIN {buildup_link_table_path_obj2obj} l
                        ON (i.doc_type, i.doc_id, i.link_type, i.link_id)
                         = (l.{m[f]}_type, l.{m[f]}_id, l.{m[not f]}_type, l.{m[not f]}_id)
                    """

                # Generate evaluation query [of8T3uCG]
                sql_query_eval = f"""
                    SELECT COUNT(*) AS n_total,
                           COALESCE(SUM(                      {' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.graphsearch_obj_fields])}), 0){' OR ' if e and ec else ''}
                         {'COALESCE(SUM(' if e and ec else ''}{' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in self.graphsearch_obj2obj_fields])}{'), 0)' if e and ec else ''}
                           AS n_patch
                      FROM {buildup_link_table_path_obj} b
                INNER JOIN {target_table_path} i
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                           {obj2obj_placeholder}
                     WHERE b.to_process = 1;
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Execute and validate the evaluation query
                    out = db.execute_query(engine_name=self.engine_name, query=sql_query_eval, query_id='of8T3uCG', verbose='print' in actions)
                    out = out if type(out) is list else [[0,0]]

                    # Extract evalutation parameters
                    rows_to_process, rows_to_patch = out[0]

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_process, rows_to_patch = 0, 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation results
                    if rows_to_process + rows_to_patch > 0:
                        df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')
                    if rows_to_patch == 0 and 'print' in actions:
                        sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

                # Generate commit query [sxUZ7wER]
                sql_query_commit = f"""
                    UPDATE {target_table_path} i
                INNER JOIN {buildup_link_table_path_obj} b
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                           {obj2obj_placeholder}
                       SET {', '.join([f'i.{c}  = b.{c}' for c in self.graphsearch_obj_fields])}{',' if ec else ''}
                           {', '.join([f'i.{c}  = l.{c}' for c in self.graphsearch_obj2obj_fields])}
                     WHERE b.to_process = 1
                       AND        ({' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.graphsearch_obj_fields])}){' OR ' if ec else ''}
                {'(' if ec else ''}{' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in self.graphsearch_obj2obj_fields])}{')' if ec else ''};
                """

                # Execute the commit query
                if 'commit' in actions:

                    # Return if there are no rows to patch
                    if rows_to_patch == 0:
                        return
                    # Else, execute the query in chunks.
                    # chunk_filter scopes boundary discovery to target rows that have a pending
                    # buildup counterpart, avoiding empty chunks over sparse row_id ranges.
                    else:
                        db.execute_query_in_chunks(
                            engine_name   = self.engine_name,
                            schema_name   = target_schema_name,
                            table_name    = target_table_name,
                            query         = sql_query_commit,
                            chunk_filter  = f"EXISTS (SELECT 1 FROM {buildup_link_table_path_obj} b WHERE (b.doc_type, b.doc_id) = (link_type, link_id) AND b.to_process = 1)",
                            chunk_size    = 10000,
                            row_id_name   = 'i.row_id',
                            show_progress = False,
                            query_id      = 'sxUZ7wER',
                            verbose       = 'print' in actions
                        )

            # Index > Doc-Links > Vertical patching > Update ElasticSearch specific fields
            def vertical_patch_elasticsearch(self, actions=()):

                # Check if there are fields to patch
                if len(self.elasticsearch_obj_fields) == 0:
                    if 'print' in actions:
                        sysmsg.trace(f"No fields to patch for doc-link type '{self.doc_type} --> {self.link_type}'.")
                    return

                # Full table paths
                buildup_link_table_path = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.IndexBuildup_Fields_Docs_{self.link_type}"
                target_schema_name      = glbcfg.mysql_schema_names[self.engine_name]['es_cache']
                target_table_name       = f"Index_D_{self.doc_type}_L_{self.link_type}"
                target_table_path       = f"{target_schema_name}.{target_table_name}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=f"IndexBuildup_Fields_Docs_{self.link_type}")
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name)

                # Generate evaluation query
                sql_query_eval = f"""
                      SELECT COUNT(*) AS n_total, COALESCE(SUM(
                                   COALESCE(t.link_name_en, "__null__") != COALESCE(IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value), "__null__")
                                OR COALESCE(t.link_name_fr, "__null__") != COALESCE(IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value), "__null__")
                                OR COALESCE(t.link_short_description_en, "__null__") != COALESCE(p.description_short_en_value, "__null__")
                                OR COALESCE(t.link_short_description_fr, "__null__") != COALESCE(p.description_short_fr_value, "__null__")
                                {'OR ' if len(self.elasticsearch_obj_fields)>0 else ''}{' OR '.join([f'COALESCE(t.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in self.elasticsearch_obj_fields])}
                             ), 0) AS n_patch
                        FROM {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p

                   LEFT JOIN {target_table_path} t
                          ON (t.link_type, t.link_id) = (p.object_type, p.object_id)

                  INNER JOIN {buildup_link_table_path} l
                          ON (t.link_type, t.link_id) = (l.doc_type, l.doc_id)

                       WHERE p.object_type = '{self.link_type}'
                         AND (p.to_process = 1 OR l.to_process = 1)
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Execute and validate the evaluation query
                    out = db.execute_query(engine_name=self.engine_name, query=sql_query_eval, query_id='XL1265bE')
                    out = out if type(out) is list else [[0,0]]

                    # Extract evalutation parameters
                    rows_to_process, rows_to_patch = out[0]

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_process, rows_to_patch = 0, 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print_sql(sql_query_eval)

                    # Print the evaluation results
                    if rows_to_process + rows_to_patch > 0:
                        df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')
                    if rows_to_patch == 0 and 'print' in actions:
                        sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

                # Generate commit query
                sql_query_commit = f"""
                    UPDATE {target_table_path} t

                INNER JOIN {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p
                        ON (t.link_type, t.link_id) = (p.object_type, p.object_id)

                INNER JOIN {buildup_link_table_path} l
                        ON (t.link_type, t.link_id) = (l.doc_type, l.doc_id)

                       SET t.link_name_en = IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value),
                           t.link_name_fr = IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value),
                           t.link_short_description_en = p.description_short_en_value,
                           t.link_short_description_fr = p.description_short_fr_value
                           {', ' if len(self.elasticsearch_obj_fields)>0 else ''}{', '.join([f't.{c} = l.{c}' for c in self.elasticsearch_obj_fields])}

                     WHERE p.object_type = '{self.link_type}'
                       AND (p.to_process = 1 OR l.to_process = 1)

                       AND (    COALESCE(t.link_name_en, "__null__") != COALESCE(IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value), "__null__")
                             OR COALESCE(t.link_name_fr, "__null__") != COALESCE(IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value), "__null__")
                             OR COALESCE(t.link_short_description_en, "__null__") != COALESCE(p.description_short_en_value, "__null__")
                             OR COALESCE(t.link_short_description_fr, "__null__") != COALESCE(p.description_short_fr_value, "__null__")
                             {'OR ' if len(self.elasticsearch_obj_fields)>0 else ''}{' OR '.join([f'COALESCE(t.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in self.elasticsearch_obj_fields])}
                           )
                """

                # Print the commit query
                if 'print' in actions:
                    print_sql(sql_query_commit, title='Z16jRm9j')

                # Execute the commit query
                if 'commit' in actions:

                    # Return if there are no rows to patch
                    if rows_to_patch == 0:
                        return
                    # Else, execute the query in chunks.
                    # chunk_filter scopes boundary discovery to target rows that have a pending
                    # PageProfile / buildup counterpart, avoiding empty chunks over sparse row_id ranges.
                    else:
                        db.execute_query_in_chunks(
                            engine_name = self.engine_name,
                            schema_name = target_schema_name,
                            table_name  = target_table_name,
                            query       = sql_query_commit,
                            chunk_filter = f"EXISTS (SELECT 1 FROM {glbcfg.schema_graph_cache_test}.Data_N_Object_T_PageProfile p INNER JOIN {buildup_link_table_path} l ON (l.doc_type, l.doc_id) = (p.object_type, p.object_id) WHERE (p.object_type, p.object_id) = (link_type, link_id) AND p.object_type = '{self.link_type}' AND (p.to_process = 1 OR l.to_process = 1))",
                            chunk_size  = 10000,
                            row_id_name = 't.row_id',
                            query_id    = 'Z16jRm9j',
                            verbose       = 'print' in actions
                        )

            # ------- Rollbacks ------- #

            # Index > Doc-Links > Vertical patching > Roll back to previous state
            def vertical_rollback_parentchild(self, rollback_date=False, actions=()):
                return NotImplementedError

            #=====================#
            # Horizontal patching #
            #=====================#

            # ------ Snapshots ------- #

            # Index > Doc-Links > Horizontal patching > Generate snapshot
            def horizontal_snapshot(self, rollback_date, actions=()):
                return NotImplementedError

            # Helper: check if this doc-link is an ontology-object edge
            def _is_ontology_object_edge(self):
                """
                Returns True if exactly one of doc_type/link_type is an ontology type
                (Concept or Category), i.e. an object-to-ontology semantic edge.
                """
                ontology_types = {'Concept', 'Category'}
                return (self.doc_type in ontology_types) != (self.link_type in ontology_types)

            # Helper: get final-scores source for ontology-object edges
            def _get_ontology_final_scores_source(self):
                """
                For ontology-object edges, returns the final scores table path,
                the ontology id column name, the ontology type, and the object type.
                """
                ontology_types = {'Concept', 'Category'}
                if self.doc_type in ontology_types:
                    ontology_type = self.doc_type
                    object_type = self.link_type
                else:
                    ontology_type = self.link_type
                    object_type = self.doc_type

                if ontology_type == 'Concept':
                    final_scores_table = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.Edges_N_Object_N_Concept_T_FinalScores"
                    ontology_id_col = 'concept_id'
                else:  # Category
                    final_scores_table = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.Edges_N_Object_N_Category_T_FinalScores"
                    ontology_id_col = 'category_id'

                return final_scores_table, ontology_id_col, ontology_type, object_type

            # ------- Patching ------- #

            # Index > Doc-Links > Horizontal patching > Insert new, replace existing, re-rank (graphsearch_test)
            def horizontal_patch(self, row_rank_thr=32, actions=()):

                #---------------------------#
                # Convert order list to SQL #
                #---------------------------#

                # Define mapping from link subtype onto type of score (semantic vs degree)
                index_to_score_type = {'ORG':'degree_score', 'SEM':'semantic_score'}

                # Fetch ordering rules from config
                if self.link_subtype.upper()=='SEM':
                    order_by_rules_list = idxcfg.settings['graphsearch']['order_by']['links']['default'].get(self.link_type, [])
                elif self.link_subtype.upper()=='ORG':
                    order_by_rules_list = idxcfg.settings['graphsearch']['order_by']['links']['parent_child'].get(self.doc_type, {}).get(self.link_type, [])
                else:
                    sysmsg.error("Invalid link subtype.")
                    return

                # Get score type
                score_type = index_to_score_type.get(self.link_subtype.upper())

                # Initialise ORDER BY statement with default SQL
                order_by = f'{score_type} DESC, link_id ASC'

                # If additional fields are included in ordering rules, prepend them
                if len(order_by_rules_list)>0:
                    # order_by = ', '.join([cast_mapping[datatypes_config['data-types']['index_fields'][o]]%o+' '+d for o,d in order_by_rules_list]) + f', {order_by}'
                    order_by = ', '.join([ cast_mapping[sql_data_type_mapping[idxcfg.settings['data_types'][o]]] % o + ' ' + d for o,d in order_by_rules_list ]) + f', {order_by}'

                #---------------------------#
                #---------------------------#

                # Full table paths
                parentchild_table_path  = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{'Edges_N_Object_N_Object_T_ParentChildSymmetric'}"
                scoresmatrix_table_path = None  # set only for non-ontology SEM edges
                buildup_doc_table_path  = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{self.buildup_doc_table_name}"
                buildup_link_table_path = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{self.buildup_link_table_name}"
                target_table_path       = f"{glbcfg.mysql_schema_names[self.engine_name]['graphsearch']}.{self.index_table_name}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graphsearch'], table_name=self.index_table_name)

                # Does the buildup table exist?
                buildup_table_exists_direct  = db.table_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=f'IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type}')
                buildup_table_exists_flipped = db.table_exists(engine_name=self.engine_name, schema_name=glbcfg.mysql_schema_names[self.engine_name]['graph_cache'], table_name=f'IndexBuildup_Fields_Links_ParentChild_{self.link_type}_{self.doc_type}')
                buildup_table_exists = buildup_table_exists_direct or buildup_table_exists_flipped

                # Cross-engine collate correction
                colate_correct = 'COLLATE utf8mb4_unicode_ci' if self.engine_name=='prod' else ''

                #--------------------------#
                # Build commit SQL queries #
                #--------------------------#

                # Initialise the SQL queries
                SQLQuery1, SQLQuery2, SQLQuery3 = None, None, None

                # Organisational table?
                if self.link_subtype.upper() == 'ORG':

                    # Modify row rank threshold to infinite
                    row_rank_thr = 9999999

                    # Buildup table exists?
                    if buildup_table_exists:

                        # Generate SQL query 1
                        SQLQuery1 = f"""
                        REPLACE INTO {target_table_path}
                                     (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}{', '.join(self.graphsearch_obj2obj_fields)}{',' if len(self.graphsearch_obj2obj_fields)>0 else ''} degree_score, row_score, row_rank)
                              SELECT p.from_object_type AS  doc_type, p.from_object_id AS doc_id,
                                       p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                     {', '.join([f'bd.{c}' for c in self.graphsearch_obj_fields])}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}{', '.join([f'bl.{c}' for c in self.graphsearch_obj2obj_fields])}{',' if len(self.graphsearch_obj2obj_fields)>0 else ''}
                                     bd.degree_score, 0 AS row_score, 99 AS row_rank
                                FROM {parentchild_table_path} p
                          INNER JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                           LEFT JOIN {glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.IndexBuildup_Fields_Links_ParentChild_{self.doc_type if buildup_table_exists_direct else self.link_type}_{self.link_type if buildup_table_exists_direct else self.doc_type} bl
                                  ON (p.{'from' if buildup_table_exists_direct else 'to'}_object_type, p.{'from' if buildup_table_exists_direct else 'to'}_object_id, p.{'to' if buildup_table_exists_direct else 'from'}_object_type, p.{'to' if buildup_table_exists_direct else 'from'}_object_id) = (bl.doc_type, bl.doc_id, bl.link_type, bl.link_id)
                               WHERE p.from_object_type {colate_correct} = '{self.doc_type}'
                                 AND p.to_object_type   {colate_correct} = '{self.link_type}'
                                 AND p.to_process = 1
                        """

                    # No buildup table
                    else:

                        # Generate SQL query 1
                        SQLQuery1 = f"""
                        REPLACE INTO {target_table_path}
                                     (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ' '} degree_score, row_score, row_rank)
                              SELECT p.from_object_type AS  doc_type, p.from_object_id AS doc_id,
                                       p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                     {', '.join([f'bd.{c}' for c in self.graphsearch_obj_fields])}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}
                                     bd.degree_score, 0 AS row_score, 99 AS row_rank
                                FROM {parentchild_table_path} p
                           LEFT JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                               WHERE p.from_object_type {colate_correct} = '{self.doc_type}'
                                 AND p.to_object_type   {colate_correct} = '{self.link_type}'
                                 AND p.to_process = 1
                        """

                    # Generate SQL query 3
                    SQLQuery3 = f"""
                    REPLACE INTO {target_table_path}
                                        (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}{', '.join(self.graphsearch_obj2obj_fields)}{',' if len(self.graphsearch_obj2obj_fields)>0 else ''} degree_score, row_score, row_rank)
                          SELECT         doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}{', '.join(self.graphsearch_obj2obj_fields)}{',' if len(self.graphsearch_obj2obj_fields)>0 else ''} degree_score, row_score, row_rank
                            FROM (SELECT doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ' '}{', '.join(self.graphsearch_obj2obj_fields)}{',' if len(self.graphsearch_obj2obj_fields)>0 else ''} degree_score,
                                         1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})) AS row_score,
                                                    row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})  AS row_rank
                                    FROM {target_table_path}
                              INNER JOIN (SELECT DISTINCT IF(from_object_type='{self.doc_type}', from_object_id, to_object_id) AS doc_id
                                                     FROM {parentchild_table_path}
                                                    WHERE from_object_type {colate_correct} = '{self.doc_type}'
                                                      AND to_object_type   {colate_correct} = '{self.link_type}'
                                                      AND to_process = 1) t
                                   USING (doc_id)
                                 ) tt
                           WHERE {score_type} >= 0.1
                             AND row_rank <= {row_rank_thr}
                    """

                # Semantic table?
                elif self.link_subtype.upper() == 'SEM':

                    # Ontology-object edges (e.g. Concept-Exercise) get their semantic scores
                    # from the final scores tables, not from the object-to-object scores matrix.
                    if self._is_ontology_object_edge():

                        final_scores_table, ontology_id_col, ontology_type, object_type = self._get_ontology_final_scores_source()

                        # For ontology-object edges there are two index tables
                        # (e.g. Category->Exercise and Exercise->Category). Each table should only
                        # store its named forward direction to avoid duplicating the reverse rows
                        # in both tables. The forward direction is:
                        #   - SQLQuery1 when this table's doc_type is the ontology side
                        #   - SQLQuery2 when this table's doc_type is the object side
                        # Both queries use the link_type's buildup table and field set.
                        if self.link_type == object_type:
                            link_join_condition = f"(fs.object_type, fs.object_id) = ('{object_type}', i.doc_id)"
                        else:
                            link_join_condition = f"fs.{ontology_id_col} = i.doc_id"

                        if self.doc_type == ontology_type:
                            # Forward: ontology as doc, object as link
                            SQLQuery1 = f"""
                            REPLACE INTO {target_table_path}
                                         (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{',' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank)
                                  SELECT '{ontology_type}' AS doc_type, fs.{ontology_id_col} AS doc_id,
                                         fs.object_type AS link_type, 'Semantic' AS link_subtype, fs.object_id AS link_id,
                                         {', '.join([f'i.{c}' for c in self.graphsearch_obj_fields])}{',' if len(self.graphsearch_obj_fields)>0 else ''}
                                         fs.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                                    FROM {final_scores_table} fs
                              INNER JOIN {buildup_link_table_path} i
                                      ON {link_join_condition}
                                   WHERE fs.object_type = '{object_type}'
                                     AND fs.to_process = 1
                            """
                            SQLQuery2 = None
                        else:
                            # Forward: object as doc, ontology as link
                            SQLQuery1 = None
                            SQLQuery2 = f"""
                            REPLACE INTO {target_table_path}
                                         (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{',' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank)
                                  SELECT fs.object_type AS doc_type, fs.object_id AS doc_id,
                                         '{ontology_type}' AS link_type, 'Semantic' AS link_subtype, fs.{ontology_id_col} AS link_id,
                                         {', '.join([f'i.{c}' for c in self.graphsearch_obj_fields])}{',' if len(self.graphsearch_obj_fields)>0 else ''}
                                         fs.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                                    FROM {final_scores_table} fs
                              INNER JOIN {buildup_link_table_path} i
                                      ON {link_join_condition}
                                   WHERE fs.object_type = '{object_type}'
                                     AND fs.to_process = 1
                            """

                        # Doc ids to re-rank depend on which side is the doc
                        if self.doc_type in ('Concept', 'Category'):
                            doc_id_subquery = f"""
                                SELECT DISTINCT {ontology_id_col} AS doc_id
                                  FROM {final_scores_table}
                                 WHERE object_type = '{object_type}'
                                   AND to_process = 1
                            """
                        else:
                            doc_id_subquery = f"""
                                SELECT DISTINCT object_id AS doc_id
                                  FROM {final_scores_table}
                                 WHERE object_type = '{object_type}'
                                   AND to_process = 1
                             """

                    else:

                        # Non-ontology SEM edges need the adjusted scores matrix table.
                        # Compute it here so ORG links do not trigger the config_scores lookup.
                        scores_matrix_table_name_as = get_scores_matrix_table_name(self.doc_type, self.link_type, gbc_or_as='AS')
                        scoresmatrix_table_path = f"{glbcfg.mysql_schema_names[self.engine_name]['graph_cache']}.{scores_matrix_table_name_as}"

                        # Generate SQL query 1
                        SQLQuery1 = f"""
                        REPLACE INTO {target_table_path}
                                     (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{',' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank)
                              SELECT s.from_object_type AS  doc_type, s.from_object_id AS doc_id,
                                       s.to_object_type AS link_type, 'Semantic' AS link_subtype, s.to_object_id AS link_id,
                                     {', '.join([f'i.{c}' for c in self.graphsearch_obj_fields])}{',' if len(self.graphsearch_obj_fields)>0 else ''}
                                     s.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                                FROM {scoresmatrix_table_path} s
                          INNER JOIN {buildup_link_table_path} i
                                  ON (s.from_object_type, s.to_object_type, s.to_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                               WHERE s.to_process = 1
                        """

                        # Generate SQL query 2 (same as SQL query 1 but flipped)
                        SQLQuery2 = f"""
                        REPLACE INTO {target_table_path}
                                     (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{',' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank)
                              SELECT   s.to_object_type AS  doc_type, s.to_object_id AS doc_id,
                                     s.from_object_type AS link_type, 'Semantic' AS link_subtype, s.from_object_id AS link_id,
                                     {', '.join([f'i.{c}' for c in self.graphsearch_obj_fields])}{',' if len(self.graphsearch_obj_fields)>0 else ''}
                                     s.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                                FROM {scoresmatrix_table_path} s
                          INNER JOIN {buildup_link_table_path} i
                                  ON (s.to_object_type, s.from_object_type, s.from_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                               WHERE s.to_process = 1
                        """

                        doc_id_subquery = f"""
                                          SELECT DISTINCT IF(from_object_type="{self.doc_type}", from_object_id, to_object_id) AS doc_id
                                                     FROM {scoresmatrix_table_path}
                                                    WHERE (
                                                                (       from_object_type {colate_correct} = "{self.doc_type}"
                                                                    AND   to_object_type {colate_correct} = "{self.link_type}"
                                                                )
                                                            OR
                                                                (
                                                                          to_object_type {colate_correct} = "{self.doc_type}"
                                                                    AND from_object_type {colate_correct} = "{self.link_type}"
                                                                )
                                                          )
                                                      AND to_process = 1

                                                    UNION

                                          SELECT DISTINCT IF(to_object_type="{self.doc_type}", to_object_id, from_object_id) AS doc_id
                                                     FROM {scoresmatrix_table_path}
                                                    WHERE (
                                                                (       from_object_type {colate_correct} = "{self.doc_type}"
                                                                    AND   to_object_type {colate_correct} = "{self.link_type}"
                                                                )
                                                            OR
                                                                (
                                                                          to_object_type {colate_correct} = "{self.doc_type}"
                                                                    AND from_object_type {colate_correct} = "{self.link_type}"
                                                                )
                                                          )
                                                      AND to_process = 1
                        """

                    # Generate SQL query 3 (re-rank)
                    SQLQuery3 = f"""
                    REPLACE INTO {target_table_path}
                                        (doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank)
                          SELECT         doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score, row_score, row_rank
                            FROM (SELECT doc_type, doc_id, link_type, link_subtype, link_id, {', '.join(self.graphsearch_obj_fields)}{', ' if len(self.graphsearch_obj_fields)>0 else ''} semantic_score,
                                         1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})) AS row_score,
                                                    row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})  AS row_rank
                                    FROM {target_table_path}
                              INNER JOIN ({doc_id_subquery}) t
                                   USING (doc_id)
                                 ) tt
                           WHERE {score_type} >= 0.1
                             AND row_rank <= {row_rank_thr}
                    """

                #------------------------------#
                # Evaluate the patch operation #
                #------------------------------#
                if 'eval' in actions:

                    # Generate evaluation query (#1)
                    if SQLQuery1 is not None:
                        sql_query_no_replace_1 = re.sub(r'REPLACE INTO[^\(\)]*\([^\(\)]*\)', '', SQLQuery1)
                        sql_query_eval_1 = f"""
                            SELECT COALESCE(SUM(ISNULL(e.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'})),0) AS rows_to_insert, COALESCE(SUM(ABS(e.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'}-t.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'})>0.01),0) AS rows_to_re_score
                            FROM ({sql_query_no_replace_1}) t LEFT JOIN {target_table_path} e USING (doc_id, link_id)
                        """
                    else:
                        sql_query_eval_1 = f"""
                            SELECT 0 AS rows_to_insert, 0 AS rows_to_re_score
                        """

                    # Generate evaluation query (#2)
                    if SQLQuery2 is not None:
                        sql_query_no_replace_2 = re.sub(r'REPLACE INTO[^\(\)]*\([^\(\)]*\)', '', SQLQuery2)
                        sql_query_eval_2 = f"""
                            SELECT COALESCE(SUM(ISNULL(e.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'})),0) AS rows_to_insert, COALESCE(SUM(ABS(e.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'}-t.{'semantic_score' if self.link_subtype.upper()=='SEM' else 'degree_score'})>0.01),0) AS rows_to_re_score
                            FROM ({sql_query_no_replace_2}) t LEFT JOIN {target_table_path} e USING (doc_id, link_id)
                        """
                    else:
                        sql_query_eval_2 = f"""
                            SELECT 0 AS rows_to_insert, 0 AS rows_to_re_score
                        """

                    # # Generate evaluation query (#3)
                    # sql_query_eval_3 = f"""
                    #     SELECT COUNT(*) AS n_total
                    #     FROM (SELECT {SQLQuery3.split('FROM (SELECT')[1]}
                    # """

                    # Print the evaluation queries
                    if 'print' in actions:
                        print(f"\n🔍 Evaluation queries for {target_table_path}:")
                        print_sql(sql_query_eval_1, title='z0rFNfM5')
                        print_sql(sql_query_eval_2, title='oxyoF81R')
                        # print(sql_query_eval_3)

                    # Execute the evaluation queries
                    out_1 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_1, query_id='z0rFNfM5')
                    out_2 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_2, query_id='oxyoF81R')
                    # out_3 = db.execute_query(engine_name=self.engine_name, query=sql_query_eval_3)

                    # Sum up the results
                    out = [[out_1[0][0] + out_2[0][0], out_1[0][1] + out_2[0][1]]]

                    # Print the results
                    if np.sum(out) > 0:
                        df = pd.DataFrame(out, columns=['rows to insert/replace', 'rows to re-score'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')

                # Execute SQL query
                if 'commit' in actions and not ('eval' in actions and np.sum(out)==0):
                    if SQLQuery1:
                        db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery1, verbose='print' in actions, query_id='8BV6hv6h')
                    if SQLQuery2:
                        db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery2, verbose='print' in actions, query_id='XG1EvKuT')
                    if SQLQuery3:
                        db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery3, verbose='print' in actions, query_id='uh3n6T1B')

            # Index > Doc-Links > Horizontal patching > Insert new, replace existing, re-rank (elasticseach_cache)
            def horizontal_patch_elasticsearch(self, row_rank_thr=16, actions=()):

                # Scores matrix table name is only needed for SEM/MIX links.
                # It will be resolved lazily below so ORG links skip the config_scores lookup.
                scores_matrix_table_name_as = None

                # Ensure mixed view exists before trying to use it
                GraphRegistry.IndexDB._ensure_mixed_view_exists(self.doc_type, self.link_type)

                #--------------------------------------------------#
                # Resolve table name or return if it doesn't exist #
                #--------------------------------------------------#
                # Table type: MIX
                if   db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_MIX", exclude_views=False):

                    # Print status
                    if 'print' in actions:
                        sysmsg.trace(f"Using MIX table for {self.doc_type} --> {self.link_type}")

                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_MIX"

                    # Generate score column name
                    score_column_name = 'adjusted_row_rank'

                    # Resolve scores matrix name lazily (MIX may or may not have a SEM component)
                    if scores_matrix_table_name_as is None:
                        try:
                            scores_matrix_table_name_as = get_scores_matrix_table_name(self.doc_type, self.link_type, gbc_or_as='AS')
                        except ValueError:
                            scores_matrix_table_name_as = None

                    # Generate SQL query segment for fetching rows to process
                    to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process = 1
                        """
                    if scores_matrix_table_name_as is not None:
                        to_process_sql_statement += f"""
                                  UNION
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process = 1
                                  UNION
                        SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                   FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                  WHERE (to_object_type, from_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process = 1
                    """

                # Table type: ORG
                elif db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_ORG", exclude_views=True):

                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_ORG"

                    # Generate score column name
                    score_column_name = 'row_rank'

                    # Generate SQL query segment for fetching rows to process
                    to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {glbcfg.schema_graph_cache_test}.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process = 1
                    """

                # Table type: SEM
                elif db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.mysql_schema_names['xaas_coresrv']['graphsearch'], table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_SEM", exclude_views=True):

                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_SEM"

                    # Generate score column name
                    score_column_name = 'row_rank'

                    # Ontology-object edges use final scores tables
                    if self._is_ontology_object_edge():

                        final_scores_table, ontology_id_col, ontology_type, object_type = self._get_ontology_final_scores_source()

                        if self.doc_type in ('Concept', 'Category'):
                            to_process_sql_statement = f"""
                                SELECT DISTINCT '{ontology_type}' AS doc_type, {ontology_id_col} AS doc_id
                                               FROM {final_scores_table}
                                              WHERE object_type = '{object_type}'
                                                AND to_process = 1
                            """
                        else:
                            to_process_sql_statement = f"""
                                SELECT DISTINCT '{object_type}' AS doc_type, object_id AS doc_id
                                               FROM {final_scores_table}
                                              WHERE object_type = '{object_type}'
                                                AND to_process = 1
                            """

                    else:

                        # Resolve scores matrix name for non-ontology SEM edges (may not exist)
                        try:
                            scores_matrix_table_name_as = get_scores_matrix_table_name(self.doc_type, self.link_type, gbc_or_as='AS')
                        except ValueError:
                            scores_matrix_table_name_as = None

                        # SEM tables require a scores matrix table
                        if scores_matrix_table_name_as is None:
                            sysmsg.warning(
                                f"Skipping ES horizontal patch for {self.doc_type} --> {self.link_type} [SEM]: "
                                f"no scores matrix table mapping found."
                            )
                            return False

                        # Generate SQL query segment for fetching rows to process
                        to_process_sql_statement = f"""
                            SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                       FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                      WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                        AND to_process = 1
                                      UNION
                            SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                       FROM {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as}
                                      WHERE (to_object_type, from_object_type) = ("{self.doc_type}", "{self.link_type}")
                                        AND to_process = 1
                        """
                else:
                    return False

                # Genenerate table name
                t = f"{glbcfg.schema_es_cache}.Index_D_{self.doc_type}_L_{self.link_type}"

                # Check if tables exist, and create if not
                create_table_if_not_exists(engine_name=self.engine_name, schema_name=glbcfg.schema_es_cache, table_name=f"Index_D_{self.doc_type}_L_{self.link_type}")

                # Generate SQL query
                sql_query_commit = f"""
                    INSERT INTO {t}
                                (doc_type, doc_id, link_type, link_subtype, link_id, link_rank, link_name_en, link_name_fr, link_short_description_en, link_short_description_fr{', ' if len(self.elasticsearch_obj_fields)>0 else ''}{', '.join([f'{c}' for c in self.elasticsearch_obj_fields])})
                         SELECT d.doc_type, d.doc_id, dl.link_type, dl.link_subtype, dl.link_id, dl.{score_column_name} AS link_rank,
                                IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value) AS link_name_en,
                                IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value) AS link_name_fr,
                                p.description_short_en_value AS link_short_description_en, p.description_short_fr_value AS link_short_description_fr{',' if len(self.elasticsearch_obj_fields)>0 else ''}
                                {', '.join([f'l.{c}' for c in self.elasticsearch_obj_fields])}
                           FROM {glbcfg.schema_graphsearch_test}.Index_D_{self.doc_type} d
                     INNER JOIN {glbcfg.schema_graphsearch_test}.{table_name} dl
                          USING (doc_type, doc_id)
                     INNER JOIN {glbcfg.schema_graphsearch_test}.Index_D_{self.link_type} l
                             ON (dl.link_type, dl.link_id) = (l.doc_type, l.doc_id)
                     INNER JOIN {glbcfg.schema_graphsearch_test}.Data_N_Object_T_PageProfile p
                             ON (p.object_type, p.object_id) = (l.doc_type, l.doc_id)
                     INNER JOIN (
                                {to_process_sql_statement}
                                ) tp
                             ON (dl.doc_type, dl.doc_id) = (tp.doc_type, tp.doc_id)
                          WHERE dl.row_rank <= {row_rank_thr}
                                {'AND' if len(self.elasticsearch_filters)>0 else ''} {' AND '.join([f'l.{f}' for f in self.elasticsearch_filters])}
               ON DUPLICATE KEY
                         UPDATE {t}.link_rank = IF(COALESCE({t}.link_rank, "__null__") != COALESCE(dl.{score_column_name}, "__null__"), dl.{score_column_name}, {t}.link_rank);
                """

                # Generate evaluation query (#1)
                sql_query_chunk_1 = re.sub(r"(?is)\A.*?\bINSERT\s+INTO\b.*?\bSELECT\b.*?(?P<block>\bFROM\b.*?)(?=\bWHERE\s+dl\.row_rank\s*<=\s*16\b).*?\Z", r"\g<block>", sql_query_commit, count=1)
                sql_query_chunk_2 = re.sub(r"(?is)\A.*?\bWHERE\s+dl\.row_rank\s*<=\s*16\b(?P<block>.*?)(?=\bON\s+DUPLICATE\s+KEY\b).*?\Z", r"\g<block>", sql_query_commit, count=1)
                sql_query_eval = f"""
                         SELECT COALESCE(SUM(ISNULL(e.link_rank)),0) AS rows_to_insert,
                                COALESCE(SUM(COALESCE(e.link_rank, "__null__") != COALESCE(dl.row_rank, "__null__")),0) AS rows_to_replace
                           {sql_query_chunk_1}
                      LEFT JOIN {t} e ON dl.doc_id = e.doc_id AND dl.link_id = e.link_id
                          WHERE dl.row_rank <= 16
                           {sql_query_chunk_2}
                """

                # !TEST: Override with optimised evaluation query
                # Generate evaluation query
                sql_query_eval = f"""
                    SELECT
                        COALESCE(SUM(e.row_id IS NULL), 0) AS rows_to_insert,
                        COALESCE(
                            SUM(
                                e.row_id IS NOT NULL
                                AND e.link_rank <> dl.{score_column_name}
                            ),
                            0
                        ) AS rows_to_replace
                    FROM (
                        {to_process_sql_statement}
                    ) AS tp

                    STRAIGHT_JOIN {glbcfg.schema_graphsearch_test}.{table_name} AS dl
                        {f"FORCE INDEX (idx_doc_rank_link)" if not table_name.endswith('MIX') else ""}
                        ON dl.doc_type = tp.doc_type
                    AND dl.doc_id = tp.doc_id
                    AND dl.row_rank <= {row_rank_thr}

                    INNER JOIN {glbcfg.schema_graphsearch_test}.Index_D_{self.doc_type} AS d
                        ON d.doc_type = dl.doc_type
                    AND d.doc_id = dl.doc_id

                    INNER JOIN {glbcfg.schema_graphsearch_test}.Index_D_{self.link_type} AS l
                        ON l.doc_type = dl.link_type
                    AND l.doc_id = dl.link_id

                    INNER JOIN {glbcfg.schema_graphsearch_test}.Data_N_Object_T_PageProfile AS p
                        ON p.object_type = l.doc_type
                    AND p.object_id = l.doc_id

                    LEFT JOIN {t} AS e
                        ON e.doc_type = dl.doc_type
                    AND e.doc_id = dl.doc_id
                    AND e.link_type = dl.link_type
                    AND e.link_subtype = dl.link_subtype
                    AND e.link_id = dl.link_id

                    WHERE 1 = 1
                        {'AND' if self.elasticsearch_filters else ''}
                        {' AND '.join(f'l.{f}' for f in self.elasticsearch_filters)}
                """

                # Execute the evaluation query.
                # In this case, we execute the query regardless of the 'eval' action,
                # in order to reduce the execution time of the patch operation on 'commit'.
                if 'commit' in actions or 'eval' in actions:

                    # Execute and validate the evaluation query
                    out = db.execute_query(engine_name=self.engine_name, query=sql_query_eval, query_id='FJ9HVCLW')
                    out = [int(out[0][0]), int(out[0][1])] if type(out) is list else [[0,0]]

                    # Number of rows to patch
                    rows_to_patch = np.sum(out)

                # Else, we assume that the evaluation query has not been executed
                else:
                    rows_to_patch = 0

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Print the evaluation query
                    if 'print' in actions:
                        print(f"\n🔍 Evaluation query for {t}:") 
                        print_sql(sql_query_eval, title='LLzeD3NV')

                    # Execute the evaluation query
                    out = db.execute_query(engine_name=self.engine_name, query=sql_query_eval, query_id='LLzeD3NV')

                    # Print the results
                    if np.sum(out) > 0:
                        df = pd.DataFrame(out, columns=['rows to insert', 'rows to replace'])
                        print_dataframe(df, title=f'\n🔍 Evaluation results for {t}:')

                # Execute the commit query
                if 'commit' in actions and not ('eval' in actions and rows_to_patch==0):

                    # Print the commit query
                    if 'print' in actions:
                        # def print_sql(sql: str, *, params: Any = None, elapsed_ms: float | None = None, db: str | None = None, title: str = "SQL", show_header: bool = True, box_style: BoxStyle = "minimal", copyable: bool = False, theme: str = "monokai", word_wrap: bool = True, console: Console | None = None) -> None:
                        print_sql(sql_query_commit, title='zwRx2b8a')

                    # Execute the commit SQL query
                    db.execute_query_in_shell(engine_name='xaas_coresrv', query=sql_query_commit, query_id='zwRx2b8a')

            # TODO: SELECT * FROM elasticsearch_cache.Index_D_Unit_L_Person WHERE (doc_id, link_id) NOT IN (SELECT doc_id, link_id FROM graphsearch_test.Index_D_Unit_L_Person_T_ORG)

            # ------- Rollbacks ------- #

            # Index > Doc-Links > Horizontal patching > Roll back to previous state
            def horizontal_rollback(self, source_doc_type, target_doc_type, index_type, rollback_date, test_mode=False):
                raise NotImplementedError

            #=================#
            # Airflow updates #
            #=================#

            # Index > Doc-Links > Airflow updates > Update 'Operations_N_Object_N_Object_T_FieldsChanged' and 'Operations_N_Object_T_ScoresExpired'
            def airflow_update(self, verbose=False):

                # Generate commit query
                sql_query_commit = f"""
                      UPDATE {glbcfg.schema_airflow}.Operations_N_Object_N_Object_T_FieldsChanged a
                  INNER JOIN {glbcfg.schema_graphsearch_test}.Index_D_{self.doc_type}_L_{self.link_type}_T_{self.link_subtype} i
                          ON (a.from_object_type, a.from_object_id, a.to_object_type, a.to_object_id) = (i.doc_type, i.doc_id, i.link_type, i.link_id)
                  INNER JOIN {glbcfg.schema_graph_cache_test}.IndexBuildup_Fields_Docs_{self.link_type} b
                          ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                         SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
                       WHERE b.to_process = 1
                """

                # Execute the commit query
                db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit, verbose=verbose, query_id='4cYaPsAQ')

                # Execute semantic related quries if the link type is 'Semantic'
                if self.link_subtype == 'SEM':

                    # Generate scores matrix table name
                    scores_matrix_table_name_as = get_scores_matrix_table_name(self.doc_type, self.link_type, gbc_or_as='AS')

                    # Generate commit query
                    sql_query_commit = f"""
                          UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired a
                      INNER JOIN {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as} s
                              ON (a.object_type, a.object_id) = (s.from_object_type, s.from_object_id)
                             SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
                           WHERE (s.from_object_type, s.to_object_type) = ('{self.doc_type}', '{self.link_type}')
                             AND s.to_process = 1
                    """

                    # Execute the commit query
                    db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit, verbose=verbose, query_id='YtLVzrX7')

                    # Generate commit query
                    sql_query_commit = f"""
                          UPDATE {glbcfg.schema_airflow}.Operations_N_Object_T_ScoresExpired a
                      INNER JOIN {glbcfg.schema_graph_cache_test}.{scores_matrix_table_name_as} s
                              ON (a.object_type, a.object_id) = (s.to_object_type, s.to_object_id)
                             SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
                           WHERE (s.from_object_type, s.to_object_type) = ('{self.link_type}', '{self.doc_type}')
                             AND s.to_process = 1
                    """

                    # Execute the commit query
                    db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit, verbose=verbose, query_id='LKw9ZyV1')

    #------------------------------------------------------------#
    # Subclass definition: GraphIndex Management (ElasticSearch) #
    #------------------------------------------------------------#
    class IndexES():

        # Class constructor
        def __init__(self):
            pass


        def generate_local_cache_streaming(self, index_date=None, ignore_warnings=True, replace_existing=False, force_replace=False):
            """
            Drop-in replacement.

            Writes ONE file per doc_type:
            es_splitindex_{index_date}_{doc_type}.jsonl.gz

            Format: JSONL (one valid JSON object per line), where each line is the FULL doc JSON:
            {
                "doc_type": ...,
                "doc_id": ...,
                ...,
                "links": [ ... ]
            }

            This avoids hand-crafted nested JSON and guarantees proper escaping of newlines, quotes, etc.
            """
            import os
            import json
            import gzip

            sysmsg.info(f"🐙 📝 Generate local JSON cache for ElasticSearch index creation (index date: {index_date}).")

            default_column_names_doc = [
                "doc_type", "doc_id", "degree_score", "short_code", "subtype_en", "subtype_fr",
                "name_en", "name_fr", "short_description_en", "short_description_fr",
                "long_description_en", "long_description_fr",
            ]
            default_column_names_link = [
                "doc_type", "doc_id", "link_type", "link_subtype", "link_id", "link_rank",
                "link_name_en", "link_name_fr", "link_short_description_en", "link_short_description_fr",
            ]

            def _iter_jsonl(path):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            yield json.loads(line)

            list_of_doc_types = idxcfg.settings["doc_types"]
            overwrite_flag = False

            target_folder = f"{ELASTICSEARCH_DATA_EXPORT_PATH}/{index_date}"
            os.makedirs(target_folder, exist_ok=True)

            with tqdm(list_of_doc_types, unit="doc type") as pb:
                for doc_type in pb:
                    pb.set_description(f"⚙️ [GLC-ES] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                    target_output_path = f"{target_folder}/es_splitindex_{index_date}_{doc_type}.jsonl.gz"

                    # -------------------------------------------
                    # If file exists, handle according to flags
                    # -------------------------------------------
                    if os.path.exists(target_output_path):
                        if not ignore_warnings:
                            sysmsg.warning(f"File already exists: {target_output_path}")
                        if not replace_existing:
                            sysmsg.warning(
                                f"Failed to generate local ElasticSearch cache. File already exists: {target_output_path}"
                            )
                            continue

                        if not overwrite_flag:
                            confirmation = "yes" if force_replace else input(
                                "Are you sure you want to replace the existing files? (yes/no): "
                            )
                            if confirmation.lower() != "yes":
                                sysmsg.error("❌ Operation cancelled by user.")
                                return
                            overwrite_flag = True
                            if not ignore_warnings:
                                sysmsg.warning("Replacing existing files ...")

                        os.remove(target_output_path)

                    # -------------------------
                    # 1) Stream DOC rows to JSONL (temp)
                    # -------------------------
                    custom_doc_cols = idxcfg.settings["elasticsearch"]["fields"]["docs"].get(doc_type, [])
                    column_names_doc = default_column_names_doc + custom_doc_cols

                    docs_jsonl = f"{target_folder}/.tmp_docs_{index_date}_{doc_type}.jsonl"


                    # Check first if {glbcfg.schema_es_cache}.Index_D_{doc_type} table exists, ignore if it doesn't
                    if not db.table_exists(
                        engine_name="xaas_coresrv",
                        schema_name=glbcfg.schema_es_cache,
                        table_name=f"Index_D_{doc_type}",
                    ):
                        if not ignore_warnings:
                            sysmsg.warning(f"Table '{glbcfg.schema_es_cache}.Index_D_{doc_type}' does not exist. Skipping doc type '{doc_type}'.")
                        continue

                    db.execute_query_stream_to_file(
                        engine_name="xaas_coresrv",
                        query=f"""
                            SELECT {', '.join(column_names_doc)}
                            FROM {glbcfg.schema_es_cache}.Index_D_{doc_type}
                        ORDER BY doc_id ASC
                        """,
                        fetch_size=2000,
                        output_file=docs_jsonl,
                        query_id='wsg7uZ1k'
                    )

                    # If no docs (empty file), skip
                    if os.path.getsize(docs_jsonl) == 0:
                        if not ignore_warnings:
                            sysmsg.warning(f"No docs found for doc type '{doc_type}'. Skipping.")
                        try:
                            os.remove(docs_jsonl)
                        except Exception:
                            pass
                        continue

                    # -------------------------
                    # 2) Stream each LINK table to JSONL (temp), only if table exists
                    # -------------------------
                    link_files = {}       # link_type -> temp jsonl path
                    link_custom_cols = {} # link_type -> [custom cols]

                    for link_type in list_of_doc_types:
                        if not db.table_exists(
                            engine_name="xaas_coresrv",
                            schema_name=glbcfg.schema_es_cache,
                            table_name=f"Index_D_{doc_type}_L_{link_type}",
                        ):
                            continue

                        custom_link_cols = idxcfg.settings["elasticsearch"]["fields"]["links"].get(link_type, [])
                        column_names_link = default_column_names_link + custom_link_cols

                        links_jsonl = f"{target_folder}/.tmp_links_{index_date}_{doc_type}_L_{link_type}.jsonl"

                        db.execute_query_stream_to_file(
                            engine_name="xaas_coresrv",
                            query=f"""
                                SELECT {', '.join(column_names_link)}
                                FROM {glbcfg.schema_es_cache}.Index_D_{doc_type}_L_{link_type}
                            ORDER BY doc_id ASC, link_rank ASC
                            """,
                            fetch_size=5000,
                            output_file=links_jsonl,
                            query_id='fhLp0sNg'
                        )

                        link_files[link_type] = links_jsonl
                        link_custom_cols[link_type] = custom_link_cols

                    # Prepare link iterators and “current row” pointers (merge join)
                    link_iters = {lt: _iter_jsonl(p) for lt, p in link_files.items()}
                    link_curr = {lt: next(it, None) for lt, it in link_iters.items()}

                    def collect_links_for_doc_id(doc_id_value):
                        out = []
                        for lt, it in link_iters.items():
                            curr = link_curr[lt]
                            while curr is not None and curr.get("doc_id") == doc_id_value:
                                json_link = {
                                    "doc_type": curr["doc_type"],
                                    "doc_id": curr["doc_id"],
                                    "link_type": curr["link_type"],
                                    "link_subtype": curr["link_subtype"],
                                    "link_id": curr["link_id"],
                                    "link_rank": curr["link_rank"],
                                    "link_name": {"en": curr["link_name_en"], "fr": curr["link_name_fr"]},
                                    "link_short_description": {
                                        "en": curr["link_short_description_en"],
                                        "fr": curr["link_short_description_fr"],
                                    },
                                }

                                # append custom link fields
                                for c in link_custom_cols.get(lt, []):
                                    if c in curr:
                                        json_link[c] = curr[c]

                                out.append(json_link)

                                curr = next(it, None)
                                link_curr[lt] = curr
                        return out

                    # -------------------------
                    # 3) Write FINAL JSONL.GZ (one doc per line)
                    # -------------------------
                    with gzip.open(target_output_path, "wt", encoding="utf-8") as out_fp:
                        for d in _iter_jsonl(docs_jsonl):
                            doc_id = d["doc_id"]

                            doc_json = {
                                "doc_type": d["doc_type"],
                                "doc_id": doc_id,
                                "degree_score": d["degree_score"],
                                "degree_score_factor": es_degree_score_factors[doc_type] * d["degree_score"],
                                "short_code": d["short_code"],
                                "subtype": {"en": d["subtype_en"], "fr": d["subtype_fr"]},
                                "name": {"en": d["name_en"], "fr": d["name_fr"]},
                                "short_description": {"en": d["short_description_en"], "fr": d["short_description_fr"]},
                                "long_description": {"en": d["long_description_en"], "fr": d["long_description_fr"]},
                                "links": [],  # filled below
                            }

                            # custom doc fields
                            for c in custom_doc_cols:
                                if c in d:
                                    doc_json[c] = d[c]

                            # attach links (streamed)
                            doc_json["links"] = collect_links_for_doc_id(doc_id)

                            # IMPORTANT: write the WHOLE dict via json.dumps to guarantee escaping (newlines, quotes, etc.)
                            out_fp.write(json.dumps(doc_json, ensure_ascii=False, default=str))
                            out_fp.write("\n")

                    # cleanup temp files
                    try:
                        os.remove(docs_jsonl)
                    except Exception:
                        pass
                    for p in link_files.values():
                        try:
                            os.remove(p)
                        except Exception:
                            pass

            sysmsg.success("🐙 ✅ Done generating local JSON cache.\n")



        # Generate local JSON cache for ElasticSearch index creation
        def generate_local_cache(self, index_date=None, ignore_warnings=True, replace_existing=False, force_replace=False):

            # Print status
            sysmsg.info(f"🐙 📝 Generate local JSON cache for ElasticSearch index creation (index date: {index_date}).")

            # Initialise default column names
            default_column_names_doc  = ['doc_type', 'doc_id', 'degree_score', 'short_code', 'subtype_en', 'subtype_fr', 'name_en', 'name_fr', 'short_description_en', 'short_description_fr', 'long_description_en', 'long_description_fr']
            default_column_names_link = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id', 'link_rank', 'link_name_en', 'link_name_fr', 'link_short_description_en', 'link_short_description_fr']

            #-------------------------#
            # Loop over all doc types #
            #-------------------------#

            # Get list of doc types from index config
            list_of_doc_types = idxcfg.settings['doc_types']

            # Initialise overwrite flag
            overwrite_flag = False

            # Loop over all doc types
            with tqdm(list_of_doc_types, unit='doc type') as pb:
                for doc_type in pb:

                    # Print status
                    pb.set_description(f"⚙️ [GLC-ES] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                    # Create target folder (if not exists) - with date
                    target_folder = f"{ELASTICSEARCH_DATA_EXPORT_PATH}/{index_date}"
                    if not os.path.exists(target_folder):
                        os.makedirs(target_folder)

                    # Generate target output path
                    target_output_path = f"{target_folder}/es_splitindex_{index_date}_{doc_type}.jsonl.gz"

                    #-------------------------------------------#
                    # If file exists, handle according to flags #
                    #-------------------------------------------#
                    if os.path.exists(target_output_path):
                        if not ignore_warnings:
                            sysmsg.warning(f"File already exists: {target_output_path}")
                        if not replace_existing:
                            sysmsg.warning(f"Failed to generate local ElasticSearch cache. File already exists: {target_output_path}")
                            continue
                        elif replace_existing:
                            if not overwrite_flag:
                                if force_replace:
                                    confirmation = 'yes'
                                else:
                                    confirmation = input(f"Are you sure you want to replace the existing files? (yes/no): ")
                                if confirmation.lower() != 'yes':
                                    sysmsg.error("❌ Operation cancelled by user.")
                                    return
                                else:
                                    overwrite_flag = True
                                    if not ignore_warnings:
                                        sysmsg.warning(f"Replacing existing files ...")
                            os.remove(target_output_path)
                    #-------------------------------------------#

                    # Initialise index dict struct
                    es_index_struct = {}

                    # Fetch doc fields from config
                    custom_column_names_doc = idxcfg.settings['elasticsearch']['fields']['docs'].get(doc_type, [])

                    # Combine default and custom column names
                    column_names_doc = default_column_names_doc + custom_column_names_doc

                    # Fetch list of docs for doc_type
                    list_of_docs = db.execute_query(engine_name='xaas_coresrv', query=f"""
                        SELECT {', '.join(column_names_doc)}
                            FROM {glbcfg.schema_es_cache}.Index_D_{doc_type}
                        ORDER BY doc_id ASC
                    """, query_id='aGn6e3kZ')

                    # Move on if no docs found for doc_type
                    if not list_of_docs or type(list_of_docs) is not list:
                        if not ignore_warnings:
                            sysmsg.warning(f"No docs found for doc type '{doc_type}'. Skipping.")
                        continue

                    # Add doc type to index struct
                    if doc_type not in es_index_struct:
                        es_index_struct[doc_type] = {}

                    # Loop over list of docs
                    for d in list_of_docs:

                        # Build doc JSON
                        doc_json = {
                            'doc_type'            : d[0],
                            'doc_id'              : d[1],
                            'degree_score'        : d[2],
                            'degree_score_factor' : es_degree_score_factors[doc_type] * d[2],
                            'short_code'          : d[3],
                            'subtype'             : {'en': d[4],  'fr': d[5]},
                            'name'                : {'en': d[6],  'fr': d[7]},
                            'short_description'   : {'en': d[8],  'fr': d[9]},
                            'long_description'    : {'en': d[10], 'fr': d[11]}
                        }

                        # Append remaining custom columns to JSON (as fields)
                        for i, c in enumerate(custom_column_names_doc):
                            doc_json[c] = d[i+12]

                        # Append links field
                        doc_json['links'] = []

                        # Append doc JSON to ES index
                        if d[1] not in es_index_struct[doc_type]:
                            es_index_struct[doc_type][d[1]] = doc_json

                    # Loop over all link doc types
                    for link_type in list_of_doc_types:

                        # Fetch link fields from config
                        custom_column_names_link = idxcfg.settings['elasticsearch']['fields' ]['links'].get(link_type, [])

                        # Combine default and custom column names
                        column_names_link = default_column_names_link + custom_column_names_link

                        # Check if link table exists
                        if not db.table_exists(engine_name='xaas_coresrv', schema_name=glbcfg.schema_es_cache, table_name=f"Index_D_{doc_type}_L_{link_type}"):
                            if not ignore_warnings:
                                sysmsg.warning(f"Table does not exist: Index_D_{doc_type}_L_{link_type}.")
                            continue

                        # Fetch list of links for doc_type and link_type
                        list_of_links = db.execute_query(engine_name='xaas_coresrv', query=f"""
                            SELECT {', '.join(column_names_link)}
                              FROM {glbcfg.schema_es_cache}.Index_D_{doc_type}_L_{link_type}
                          ORDER BY doc_id ASC, link_rank ASC
                        """, query_id='tKPvnpZ1')
                        list_of_links = list_of_links if type(list_of_links) is list else []

                        # Loop over list of links
                        for l in list_of_links:

                            # Build link JSON
                            json_link = {
                                'doc_type'     : l[0],
                                'doc_id'       : l[1],
                                'link_type'    : l[2],
                                'link_subtype' : l[3],
                                'link_id'      : l[4],
                                'link_rank'    : l[5],
                                'link_name'              : {'en': l[6],  'fr': l[7]},
                                'link_short_description' : {'en': l[8],  'fr': l[9]}
                            }

                            # Append remaining custom columns to JSON (as fields)
                            for i, c in enumerate(custom_column_names_link):
                                json_link[c] = l[i+10]

                            # Check if doc_id exists in index struct
                            if l[1] not in es_index_struct[doc_type]:
                                print('')
                                sysmsg.warning(f"Doc ID '{l[1]}' not found in index struct for doc type '{doc_type}'. Skipping link append.")
                                continue

                            # Append link to doc JSON
                            es_index_struct[doc_type][l[1]]['links'] += [json_link]

                    # Save index JSON to file (as json.gz)
                    with gzip.open(f'{target_output_path}', 'wt', encoding='utf-8') as f:
                        json.dump(es_index_struct, f, indent=4)

            # Print status
            sysmsg.success(f"🐙 ✅ Done generating local JSON cache.\n")

        # Generate ElasticSearch index from local JSON cache
        def generate_index_from_local_cache(self, index_date=None, ignore_warnings=True, replace_existing=False, force_replace=False):

            # Helper function for JSON serialization of Decimal objects
            def _json_default(o):
                if isinstance(o, Decimal):
                    return float(o)  # numeric in JSON
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

            # Display status
            sysmsg.info(f"🐙 📝 Generate ElasticSearch import folder from local JSON cache (index date: {index_date}).")

            # ------------------------------------------------------------------
            # Output folder layout expected by import_index_from_folder():
            #   <output_folder>/
            #       settings_mappings.json
            #       documents.jsonl
            # ------------------------------------------------------------------
            output_folder = f"{ELASTICSEARCH_DATA_EXPORT_PATH}/{index_date}/es_fullindex_{index_date}"
            os.makedirs(output_folder, exist_ok=True)
            docs_path = os.path.join(output_folder, "documents.jsonl")
            settings_path = os.path.join(output_folder, "settings_mappings.json")

            # Existing file handling (folder-level)
            existing_files = [p for p in (docs_path, settings_path) if os.path.exists(p)]
            if existing_files:
                if not ignore_warnings:
                    for p in existing_files:
                        sysmsg.warning(f"File already exists: {p}")

                if not replace_existing:
                    sysmsg.error(f"❌ Failed. Output already exists in: {output_folder}")
                    return

                confirmation = "yes" if force_replace else input(
                    f"Are you sure you want to replace existing files in '{output_folder}'? (yes/no): "
                ).strip().lower()

                if confirmation != "yes":
                    sysmsg.error("❌ Operation cancelled by user.")
                    return

                for p in existing_files:
                    try:
                        os.remove(p)
                    except FileNotFoundError:
                        pass

            # ------------------------------------------------------------------
            # Write settings_mappings.json (exact structure as requested)
            # ------------------------------------------------------------------
            settings_mappings_payload = {
                "aliases": {},
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                        "max_ngram_diff": 2,
                        "analysis": {
                            "char_filter": {
                                "strip_html": {"type": "html_strip"}
                            },
                            "normalizer": {
                                "lc_fold": {
                                    "type": "custom",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            },
                            "filter": {
                                "stemmer_en": {"type": "stemmer", "language": "light_english"},
                                "stemmer_fr": {"type": "stemmer", "language": "light_french"},
                                "shingle_2_3": {
                                    "type": "shingle",
                                    "min_shingle_size": 2,
                                    "max_shingle_size": 3,
                                    "output_unigrams": True
                                },
                                "synonym_en": {
                                    "type": "synonym_graph",
                                    "synonyms": ["computational complexity, algorithmic complexity"]
                                },
                                "edge_2_20": {"type": "edge_ngram", "min_gram": 2, "max_gram": 20},
                                "ngram_3_5": {"type": "ngram", "min_gram": 3, "max_gram": 5}
                            },
                            "analyzer": {
                                "raw_lc": {
                                    "tokenizer": "keyword",
                                    "filter": ["lowercase", "asciifolding"]
                                },
                                "base_en": {
                                    "type": "custom",
                                    "char_filter": ["strip_html"],
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "stemmer_en"]
                                },
                                "base_fr": {
                                    "type": "custom",
                                    "char_filter": ["strip_html"],
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "stemmer_fr"]
                                },
                                "search_en": {
                                    "type": "custom",
                                    "char_filter": ["strip_html"],
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "stemmer_en", "synonym_en"]
                                },
                                "search_fr": {
                                    "type": "custom",
                                    "char_filter": ["strip_html"],
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "stemmer_fr"]
                                },
                                "autocomplete_en": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "edge_2_20"]
                                },
                                "autocomplete_fr": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "edge_2_20"]
                                },
                                "trigram": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "shingle_2_3"]
                                },
                                "typo_ngram": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding", "ngram_3_5"]
                                },
                                "typo_search": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            }
                        }
                    }
                },
                "mappings": {
                    "dynamic": True,
                    "properties": {
                        "name": {
                            "properties": {
                                "en": {
                                    "type": "text",
                                    "analyzer": "base_en",
                                    "search_analyzer": "search_en",
                                    "fields": {
                                        "raw": {"type": "keyword", "normalizer": "lc_fold"},
                                        "sayt": {
                                            "type": "search_as_you_type",
                                            "analyzer": "base_en",
                                            "doc_values": False,
                                            "max_shingle_size": 3
                                        },
                                        "ac": {
                                            "type": "text",
                                            "analyzer": "autocomplete_en",
                                            "search_analyzer": "search_en"
                                        },
                                        "trigram": {"type": "text", "analyzer": "trigram"},
                                        "typo": {
                                            "type": "text",
                                            "analyzer": "typo_ngram",
                                            "search_analyzer": "typo_search"
                                        }
                                    }
                                },
                                "fr": {
                                    "type": "text",
                                    "analyzer": "base_fr",
                                    "search_analyzer": "search_fr",
                                    "fields": {
                                        "raw": {"type": "keyword", "normalizer": "lc_fold"},
                                        "sayt": {
                                            "type": "search_as_you_type",
                                            "analyzer": "base_fr",
                                            "doc_values": False,
                                            "max_shingle_size": 3
                                        },
                                        "ac": {
                                            "type": "text",
                                            "analyzer": "autocomplete_fr",
                                            "search_analyzer": "search_fr"
                                        },
                                        "trigram": {"type": "text", "analyzer": "trigram"},
                                        "typo": {
                                            "type": "text",
                                            "analyzer": "typo_ngram",
                                            "search_analyzer": "typo_search"
                                        }
                                    }
                                }
                            }
                        },
                        "long_description": {
                            "properties": {
                                "en": {
                                    "type": "text",
                                    "analyzer": "base_en",
                                    "search_analyzer": "search_en",
                                    "fields": {
                                        "typo": {
                                            "type": "text",
                                            "analyzer": "typo_ngram",
                                            "search_analyzer": "typo_search"
                                        }
                                    }
                                },
                                "fr": {
                                    "type": "text",
                                    "analyzer": "base_fr",
                                    "search_analyzer": "search_fr",
                                    "fields": {
                                        "typo": {
                                            "type": "text",
                                            "analyzer": "typo_ngram",
                                            "search_analyzer": "typo_search"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            # Write settings and mappings to file
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(settings_mappings_payload, f, ensure_ascii=False, indent=4)

            # ------------------------------------------------------------------
            # Stream documents into documents.jsonl
            #
            # IMPORTANT for the importer:
            # - one valid JSON object per line
            # - each object should preferably be {"_id": ..., "_source": {...}}
            # - if cache docs are already shaped like that, we keep them as-is
            # ------------------------------------------------------------------

            # Get list of doc types from index config
            list_of_doc_types = idxcfg.settings["doc_types"]

            # Display status
            sysmsg.trace(f"⚙️  Streaming documents to '{docs_path}' ...")

            # Stream over doc types and write to JSONL file (one JSON object per line)
            with open(docs_path, "w", encoding="utf-8") as out:
                with tqdm(list_of_doc_types, unit="doc type") as pb:

                    # For each doc type, read the corresponding JSON cache file (if exists) and stream its documents to the output JSONL file
                    for doc_type in pb:

                        # Print status
                        pb.set_description(f"⚙️ Loading doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])

                        # Check if source JSON cache file exists for doc type
                        source_file_path = f"{ELASTICSEARCH_DATA_EXPORT_PATH}/{index_date}/es_splitindex_{index_date}_{doc_type}.jsonl.gz"
                        if not os.path.exists(source_file_path):
                            if not ignore_warnings:
                                sysmsg.warning(f"Source file does not exist: {source_file_path}. Skipping doc type '{doc_type}'.")
                            continue

                        with gzip.open(source_file_path, "rt", encoding="utf-8", errors="strict") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                doc = json.loads(line)

                                # expect doc already has doc_id (or _id)
                                doc_id = doc.get("doc_id") or doc.get("_id")
                                if doc_id is None:
                                    # fail fast; cache format violation
                                    continue

                                # Ensure importer shape
                                if isinstance(doc, dict) and "_source" in doc:
                                    obj = doc
                                    obj.setdefault("_id", doc_id)
                                else:
                                    obj = {"_id": doc_id, "_source": doc}

                                out.write(json.dumps(obj, ensure_ascii=False, default=_json_default))
                                out.write("\n")

                        # # Stream over: { "<doc_type>": { "<doc_id>": <doc>, ... } }
                        # with gzip.open(source_file_path, "rb") as fbin:

                        #     # Wrap the binary file with a text wrapper for ijson
                        #     import io
                        #     ftxt = io.TextIOWrapper(fbin, encoding="utf-8", errors="replace")

                        #     # Use ijson to stream over the JSON structure and write each document as a separate line in the output JSONL file
                        #     for doc_id, doc in ijson.kvitems(ftxt, f"{doc_type}"):

                        #         # Ensure shape compatible with import_index_from_folder()
                        #         if isinstance(doc, dict) and "_source" in doc:
                        #             obj = doc
                        #             if "_id" not in obj:
                        #                 obj["_id"] = doc_id
                        #         else:
                        #             obj = {"_id": doc_id, "_source": doc}

                        #         # Write JSON object to file (one per line)
                        #         out.write(json.dumps(obj, ensure_ascii=True, default=_json_default))
                        #         out.write("\n")

            # Print status
            sysmsg.success(f"🐙 ✅ Done generating import folder:\n  {output_folder}\n")

#==================================#
# Main: >> python graphregistry.py #
#==================================#
if __name__ == '__main__':
    pass
