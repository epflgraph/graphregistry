#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# TODO:
# - make graph_lectures.Edges_N_Object_N_Object_T_ChildToParent tuple order consistent with other tables, particularly with graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
# - (related) add "relationship" field to graph_lectures.Edges_N_Object_N_Object_T_ChildToParent
# Edges_N_Lecture_N_Concept_T_Timestamps_UD is wrong. Correct it!

# Add this somewhere:

    # DELETE
    # FROM graphsearch_test.Index_D_Category_L_Category_T_ORG
    # WHERE link_subtype= 'Parent-to-Child'

# TODO: Create object to category tables (some are still missing)

# TODO: after calculating scores and fields, add tuples back to operations table
# TODO: delete all local variables in functions


from sqlalchemy import create_engine as SQLEngine, text
from tqdm import tqdm
from loguru import logger as sysmsg
import base64, re, time, subprocess, warnings, os, glob, rich, hashlib, random, termios, tty, inspect
from elasticsearch import Elasticsearch as ElasticSearchEngine, helpers, ElasticsearchWarning
import sys, json, logging, sys, datetime, Levenshtein, requests, itertools
from urllib.parse import quote
from copy import deepcopy
import numpy as np
import pandas as pd
from itertools import groupby
from typing import List, Tuple
from tabulate import tabulate
from yaml import safe_load
from graphai_client.client import login as graphai_login
from graphai_client.client_api.text import extract_concepts_from_text
import tkinter as tk
from tkinter import ttk

# Enable faulthandler to dump Python tracebacks explicitly on a fault
# faulthandler.enable()

# Set up system message handler to display TRACE messages
sysmsg.remove()  # remove default handler
# sysmsg.add(sys.stdout, level="TRACE")
sysmsg.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line:06d}</cyan> - "
           "<level>{message}</level>",
    level="TRACE"
)

# Progress bar configuration
PBWIDTH = 64 # Width of the progress bar

# Get the current working directory
package_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..'))
global_config_file = os.path.join(package_dir, 'config.yaml')
with open(global_config_file) as fp:
    global_config = safe_load(fp)

# ElasticSearch data export path
config_es_data_export_path = global_config['elasticsearch']['data_path']['export']
if os.path.isabs(config_es_data_export_path):
    ELASTICSEARCH_DATA_EXPORT_PATH = config_es_data_export_path
else:
    ELASTICSEARCH_DATA_EXPORT_PATH = os.path.join(package_dir, config_es_data_export_path)

# Index configuration file path
config_es_index_config_file = global_config['elasticsearch']['index_configuration_file']
if os.path.isabs(config_es_index_config_file):
    INDEX_CONFIG_FILE = config_es_index_config_file
else:
    INDEX_CONFIG_FILE = os.path.join(package_dir, config_es_index_config_file)

# Load Index configuration
with open(INDEX_CONFIG_FILE, 'r') as f:
    index_config = json.load(f)

# Load GraphAI client
config_graphai_client_config_file = global_config['graphai']['client_config_file']
if os.path.isabs(config_graphai_client_config_file):
    GRAPHAI_CLIENT_CONFIG_FILE = config_graphai_client_config_file
else:
    GRAPHAI_CLIENT_CONFIG_FILE = os.path.join(package_dir, config_graphai_client_config_file)
graphai_login_info = graphai_login(graph_api_json=GRAPHAI_CLIENT_CONFIG_FILE)

# MySQL schema names
# TODO: global replace graph_cache with this dict
mysql_schema_names = {
    'test' : {
        'cache' : 'graph_cache',
        'ui'    : 'graphsearch_test',
    },
    'prod' : {
        'cache' : 'graph_cache',
        'ui'    : 'graphsearch_prod',
    }
}

# Suppress only the ElasticsearchWarning
warnings.filterwarnings('ignore', category=ElasticsearchWarning)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')
logger.setLevel(logging.DEBUG)

# Term colours
term_colors = {
    'Unit'        : 'blue',
    'Lecture'     : 'green',
    'MOOC'        : 'yellow',
    'Person'      : 'magenta',
    'Publication' : 'cyan',
    'Concept'     : 'red'
}

# Index doc combinations
index_doc_types_list = [
    ('EPFL', 'Course'     ),
    ('EPFL', 'Lecture'    ),
    ('EPFL', 'MOOC'       ),
    ('EPFL', 'Person'     ),
    ('EPFL', 'Publication'),
    ('EPFL', 'Startup'    ),
    ('EPFL', 'Unit'       ),
    ('EPFL', 'Widget'     ),
    ('Ont' , 'Category'   ),
    ('Ont' , 'Concept'    )
]

# Index doc-link combinations (build from index doc combinations)
index_doc_link_types_permutations = []
for source_institution_id, source_node_type in index_doc_types_list:
    for target_institution_id, target_node_type in index_doc_types_list:
        index_doc_link_types_permutations.append((source_institution_id, source_node_type, target_institution_id, target_node_type))

# Object-to-object type combinations (for scores)
object_to_object_types_scoring_list = [
	('Category', 'Category'),
	('Category', 'Concept'),
	('Concept', 'Concept'),
	('Course', 'Category'),
	('Course', 'Concept'),
	('Course', 'Course'),
	('Course', 'Lecture'),
	('Course', 'MOOC'),
	('Course', 'Person'),
	('Course', 'Publication'),
	('Course', 'Startup'),
	('Course', 'Unit'),
	('Lecture', 'Category'),
	('Lecture', 'Concept'),
	('Lecture', 'Lecture'),
	('Lecture', 'MOOC'),
	('Lecture', 'Person'),
	('Lecture', 'Publication'),
	('Lecture', 'Startup'),
	('Lecture', 'Unit'),
	('MOOC', 'Category'),
	('MOOC', 'Concept'),
	('MOOC', 'MOOC'),
	('MOOC', 'Person'),
	('MOOC', 'Publication'),
	('MOOC', 'Startup'),
	('MOOC', 'Unit'),
	('Person', 'Category'),
	('Person', 'Concept'),
	('Person', 'Person'),
	('Person', 'Publication'),
	('Person', 'Startup'),
	('Person', 'Unit'),
	('Publication', 'Category'),
	('Publication', 'Concept'),
	('Publication', 'Publication'),
	('Publication', 'Startup'),
	('Publication', 'Unit'),
	('Startup', 'Category'),
	('Startup', 'Concept'),
	('Startup', 'Startup'),
	('Startup', 'Unit'),
	('Unit', 'Category'),
	('Unit', 'Concept'),
	('Unit', 'Unit'),
	('Widget', 'Category'),
	('Widget', 'Concept'),
	('Widget', 'Widget')
]

# Define function for playing system sounds
def play_system_sound(msg_type, sound_strength):
    sound_file = f"{msg_type}_{sound_strength}.aiff"
    subprocess.run([global_config['sound']['player_bin'], f'{global_config["sound"]["data_path"]}/{sound_file}'])

#-------------------------------#
# ElasticSearch query templates #
#-------------------------------#

# ElasticSearch query template for the search index
es_query_template = {
    "_source": [
        "doc_id",
        "doc_type",
        "name.en",
        "name.fr",
        "short_description.en",
        "short_description.fr",
        "degree_score",
        "degree_score_factor",
        "depth",
        "links",
    ],
    "size": 10,
    "query": {
        "function_score": {
            "score_mode": "multiply",
            "functions": [
                {
                    "field_value_factor": {
                        "field": "degree_score_factor",
                    },
                },
            ],
            "query": {
                "bool": {
                    "filter": [
                        # {
                        #     "terms": {
                        #         "doc_type.keyword": ["Category", "Course", "Person"]
                        #     }
                        # }
                    ],
                    "should": [
                        {
                            "term": {
                                "doc_id.keyword": {
                                    "boost": 10,
                                    "value": None,
                                },
                            },
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "multi_match": {
                                            "type": "bool_prefix",
                                            "operator": "and",
                                            "fuzziness": "AUTO",
                                            "fields": [
                                                "long_description.en^0.001",
                                                "name.en",
                                                "name.en.keyword",
                                                "name.en.raw",
                                                "name.en.trigram",
                                                "name.en.sayt._2gram",
                                                "name.en.sayt._3gram",
                                            ],
                                            "query": None,
                                        },
                                    },
                                    {
                                        "multi_match": {
                                            "type": "bool_prefix",
                                            "operator": "and",
                                            "fuzziness": "AUTO",
                                            "fields": [
                                                "long_description.fr^0.001",
                                                "name.fr",
                                                "name.fr.keyword",
                                                "name.fr.raw",
                                                "name.fr.trigram",
                                                "name.fr.sayt._2gram",
                                                "name.fr.sayt._3gram",
                                            ],
                                            "query": None,
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                    "minimum_should_match": 1,
                },
            },
        },
    },
    "highlight": {
        "number_of_fragments": 1,
        "pre_tags": ["<strong>"],
        "post_tags": ["</strong>"],
        "fields": {
            "name.en": {},
            "name.fr": {},
            "long_description.en": {},
            "long_description.fr": {},
        },
    },
}

# ElasticSearch settings and mappings
es_settings_and_mappings = {
    "settings": {
        "index":{
            "analysis":{
                "analyzer":{
                    "raw":{
                        "tokenizer":"keyword",
                        "filter":["lowercase"]
                    },
                    "base_en":{
                        "tokenizer":"standard",
                        "filter":["lowercase", "asciifolding", "stemmer_en"]
                    },
                    "base_fr":{
                        "tokenizer":"standard",
                        "filter":["lowercase", "asciifolding", "stemmer_fr"]
                    },
                    "synonym_en":{
                        "tokenizer":"standard",
                        "filter":["lowercase", "asciifolding", "stemmer_en", "synonym_en"]
                    },
                    "trigram":{
                        "tokenizer":"standard",
                        "filter":["lowercase", "asciifolding", "shingle"]
                    }
                },
                "filter":{
                    "shingle":{
                        "type":"shingle",
                        "min_shingle_size":"2",
                        "max_shingle_size":"3"
                    },
                    "stemmer_en":{
                        "type":"stemmer",
                        "language":"light_english"
                    },
                    "stemmer_fr":{
                        "type":"stemmer",
                        "language":"light_french"
                    },
                    "synonym_en":{
                        "type":"synonym_graph",
                        "synonyms":["computational complexity , algorithmic complexity"]
                    }
                }
            }
        }
    },
    "mappings": {
        "properties":{
            "name.en":{
            "type":"text",
            "analyzer":"base_en",
            "search_analyzer":"synonym_en",
            "fields":{
                "raw":{
                "type":"text",
                "analyzer":"raw"
                },
                "sayt":{
                "type":"search_as_you_type",
                "analyzer":"base_en"
                },
                "trigram":{
                "type":"text",
                "analyzer":"trigram"
                }
            }
            },
            "name.fr":{
            "type":"text",
            "analyzer":"base_fr",
            "search_analyzer":"base_fr",
            "fields":{
                "raw":{
                "type":"text",
                "analyzer":"raw"
                },
                "sayt":{
                "type":"search_as_you_type",
                "analyzer":"base_fr"
                },
                "trigram":{
                "type":"text",
                "analyzer":"trigram"
                }
            }
            },
            "long_description.en":{
            "type":"text",
            "analyzer":"base_en",
            "search_analyzer":"synonym_en"
            },
            "long_description.fr":{
            "type":"text",
            "analyzer":"base_fr",
            "search_analyzer":"base_fr"
            }
        }
    }
}

# Define degree score factors
es_degree_score_factors = {
    'Course'     : 128,
    'Lecture'    : 128,
    'MOOC'       : 64,
    'Person'     : 128,
    'Publication': 1,
    'Startup'    : 64,
    'Unit'       : 64,
    'Widget'     : 64,
    'Category'   : 512,
    'Concept'    : 512
}

#-----------------------------#
# Column datatype definitions #
#-----------------------------#

# Object type to schema mapping
object_type_to_schema = {
    'Category'       : 'graph_ontology',
    'Concept'        : 'graph_ontology',
    'Course'         : 'graph_registry',
    'Lecture'        : 'graph_lectures',
    'MOOC'           : 'graph_registry',
    'Person'         : 'graph_registry',
    'Publication'    : 'graph_registry',
    'Slide'          : 'graph_lectures',
    'Specialisation' : 'graph_registry',
    'Startup'        : 'graph_registry',
    'Transcript'     : 'graph_lectures',
    'StudyPlan'      : 'graph_registry',
    'Unit'           : 'graph_registry',
    'Widget'         : 'graph_registry',
}

# Object type to institution id mapping
object_type_to_institution_id = {
    'Category'       : 'Ont',
    'Concept'        : 'Ont',
    'Course'         : 'EPFL',
    'Lecture'        : 'EPFL',
    'MOOC'           : 'EPFL',
    'Person'         : 'EPFL',
    'Publication'    : 'EPFL',
    'Slide'          : 'EPFL',
    'Specialisation' : 'EPFL',
    'Startup'        : 'EPFL',
    'Transcript'     : 'EPFL',
    'StudyPlan'      : 'EPFL',
    'Unit'           : 'EPFL',
    'Widget'         : 'EPFL',
}

# List of possible values for institution_id and object_type
institution_ids = ['Ont', 'EPFL', 'ETHZ', 'PSI', 'Empa', 'Eawag', 'WSL']
object_types = ['Category', 'Chart', 'Concept', 'Course', 'Dashboard', 'Exercise', 'External person', 'Hardware', 'Historical figure', 'Lecture', 'Learning module', 'MOOC', 'News', 'Notebook', 'Person', 'Publication', 'Specialisation', 'Startup', 'Strategic area', 'Slide', 'StudyPlan', 'Transcript', 'Unit', 'Widget']

# SQL Enum definitions
institution_ids_enum = f"""ENUM('{"', '".join(institution_ids)}')"""
object_types_enum = f"""ENUM('{"', '".join(object_types)}')"""

# Datatypes JSON
table_datatypes_json = {
    "from_to_edges" : {
        "from_id" : "VARCHAR(255) NOT NULL",
        "to_id"   : "VARCHAR(255) NOT NULL"
    },
    "object" : {
        "institution_id" : f"{institution_ids_enum} NOT NULL",
        "object_type"    : f"{object_types_enum} NOT NULL",
        "object_id"      : "VARCHAR(255) NOT NULL",
        "field_language" : "ENUM('en', 'fr', 'de', 'it') NOT NULL",
        "field_name"     : "VARCHAR(32) NOT NULL",
    },
    "object_to_object" : {
        "from_institution_id" : f"{institution_ids_enum} NOT NULL",
        "from_object_type"    : f"{object_types_enum} NOT NULL",
        "from_object_id"      : "VARCHAR(255) NOT NULL",
        "to_institution_id"   : f"{institution_ids_enum} NOT NULL",
        "to_object_type"      : f"{object_types_enum} NOT NULL",
        "to_object_subtype"   : "ENUM('Parent-to-Child', 'Child-to-Parent', 'Semantic') NOT NULL",
        "to_object_id"        : "VARCHAR(255) NOT NULL",
        "field_language"      : "ENUM('en', 'fr', 'de', 'it') NOT NULL",
        "field_name"          : "VARCHAR(32) NOT NULL",
        "context"             : "VARCHAR(64) NOT NULL"
    },
    "object_to_concept" : {
        "institution_id" : f"{institution_ids_enum} NOT NULL",
        "object_type"    : f"{object_types_enum} NOT NULL",
        "object_id"      : "VARCHAR(255) NOT NULL",
        "concept_id"     : "VARCHAR(255) NOT NULL"
    },
    "doc_profile" : {
        "institution_id"         : f"{institution_ids_enum} NOT NULL",
        "object_type"            : f"{object_types_enum} NOT NULL",
        "object_id"              : "VARCHAR(255) NOT NULL",
        "numeric_id_en"          : "INT UNSIGNED",
        "numeric_id_fr"          : "INT UNSIGNED",
        "numeric_id_de"          : "INT UNSIGNED",
        "numeric_id_it"          : "INT UNSIGNED",
        "short_code"             : "VARCHAR(32)",
        "subtype_en"             : "VARCHAR(255)",
        "subtype_fr"             : "VARCHAR(255)",
        "subtype_de"             : "VARCHAR(255)",
        "subtype_it"             : "VARCHAR(255)",
        "name_en_is_auto_generated"  : "TINYINT",
        "name_en_is_auto_corrected"  : "TINYINT",
        "name_en_is_auto_translated" : "TINYINT",
        "name_en_translated_from"    : "CHAR(6)",
        "name_en_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "name_fr_is_auto_generated"  : "TINYINT",
        "name_fr_is_auto_corrected"  : "TINYINT",
        "name_fr_is_auto_translated" : "TINYINT",
        "name_fr_translated_from"    : "CHAR(6)",
        "name_fr_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "name_de_is_auto_generated"  : "TINYINT",
        "name_de_is_auto_corrected"  : "TINYINT",
        "name_de_is_auto_translated" : "TINYINT",
        "name_de_translated_from"    : "CHAR(6)",
        "name_de_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "name_it_is_auto_generated"  : "TINYINT",
        "name_it_is_auto_corrected"  : "TINYINT",
        "name_it_is_auto_translated" : "TINYINT",
        "name_it_translated_from"    : "CHAR(6)",
        "name_it_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_short_en_is_auto_generated"   : "TINYINT",
        "description_short_en_is_auto_corrected"   : "TINYINT",
        "description_short_en_is_auto_translated"  : "TINYINT",
        "description_short_en_translated_from"     : "CHAR(6)",
        "description_short_en_value"               : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_short_fr_is_auto_generated"   : "TINYINT",
        "description_short_fr_is_auto_corrected"   : "TINYINT",
        "description_short_fr_is_auto_translated"  : "TINYINT",
        "description_short_fr_translated_from"     : "CHAR(6)",
        "description_short_fr_value"               : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_short_de_is_auto_generated"   : "TINYINT",
        "description_short_de_is_auto_corrected"   : "TINYINT",
        "description_short_de_is_auto_translated"  : "TINYINT",
        "description_short_de_translated_from"     : "CHAR(6)",
        "description_short_de_value"               : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_short_it_is_auto_generated"   : "TINYINT",
        "description_short_it_is_auto_corrected"   : "TINYINT",
        "description_short_it_is_auto_translated"  : "TINYINT",
        "description_short_it_translated_from"     : "CHAR(6)",
        "description_short_it_value"               : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_medium_en_is_auto_generated"  : "TINYINT",
        "description_medium_en_is_auto_corrected"  : "TINYINT",
        "description_medium_en_is_auto_translated" : "TINYINT",
        "description_medium_en_translated_from"    : "CHAR(6)",
        "description_medium_en_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_medium_fr_is_auto_generated"  : "TINYINT",
        "description_medium_fr_is_auto_corrected"  : "TINYINT",
        "description_medium_fr_is_auto_translated" : "TINYINT",
        "description_medium_fr_translated_from"    : "CHAR(6)",
        "description_medium_fr_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_medium_de_is_auto_generated"  : "TINYINT",
        "description_medium_de_is_auto_corrected"  : "TINYINT",
        "description_medium_de_is_auto_translated" : "TINYINT",
        "description_medium_de_translated_from"    : "CHAR(6)",
        "description_medium_de_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_medium_it_is_auto_generated"  : "TINYINT",
        "description_medium_it_is_auto_corrected"  : "TINYINT",
        "description_medium_it_is_auto_translated" : "TINYINT",
        "description_medium_it_translated_from"    : "CHAR(6)",
        "description_medium_it_value"              : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_long_en_is_auto_generated"    : "TINYINT",
        "description_long_en_is_auto_corrected"    : "TINYINT",
        "description_long_en_is_auto_translated"   : "TINYINT",
        "description_long_en_translated_from"      : "CHAR(6)",
        "description_long_en_value"                : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_long_fr_is_auto_generated"    : "TINYINT",
        "description_long_fr_is_auto_corrected"    : "TINYINT",
        "description_long_fr_is_auto_translated"   : "TINYINT",
        "description_long_fr_translated_from"      : "CHAR(6)",
        "description_long_fr_value"                : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_long_de_is_auto_generated"    : "TINYINT",
        "description_long_de_is_auto_corrected"    : "TINYINT",
        "description_long_de_is_auto_translated"   : "TINYINT",
        "description_long_de_translated_from"      : "CHAR(6)",
        "description_long_de_value"                : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "description_long_it_is_auto_generated"    : "TINYINT",
        "description_long_it_is_auto_corrected"    : "TINYINT",
        "description_long_it_is_auto_translated"   : "TINYINT",
        "description_long_it_translated_from"      : "CHAR(6)",
        "description_long_it_value"                : "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "external_key_en" : "VARCHAR(255)",
        "external_key_fr" : "VARCHAR(255)",
        "external_key_de" : "VARCHAR(255)",
        "external_key_it" : "VARCHAR(255)",
        "external_url_en" : "VARCHAR(255)",
        "external_url_fr" : "VARCHAR(255)",
        "external_url_de" : "VARCHAR(255)",
        "external_url_it" : "VARCHAR(255)",
        "is_visible" : "TINYINT NOT NULL DEFAULT 0",
        "object_created" : "DATETIME DEFAULT CURRENT_TIMESTAMP",
        "object_updated" : "DATETIME DEFAULT NULL"
    },
    "doc_index" : {
        "doc_institution"      : f"{institution_ids_enum} NOT NULL",
        "doc_type"             : f"{object_types_enum} NOT NULL",
        "doc_id"               : "VARCHAR(255) NOT NULL",
        "include_code_in_name" : "TINYINT(1) NOT NULL",
        "degree_score"         : "FLOAT NOT NULL",
    },
    "link_index" : {
        "doc_institution"  : f"{institution_ids_enum} NOT NULL",
        "doc_type"         : f"{object_types_enum} NOT NULL",
        "doc_id"           : "VARCHAR(255) NOT NULL",
        "link_institution" : f"{institution_ids_enum} NOT NULL",
        "link_type"        : f"{object_types_enum} NOT NULL",
        "link_subtype"     : "ENUM('Parent-to-Child', 'Child-to-Parent', 'Semantic') NOT NULL",
        "link_id"          : "VARCHAR(255) NOT NULL",
        "semantic_score"   : "FLOAT NOT NULL",
        "degree_score"     : "FLOAT NOT NULL",
        "row_score"        : "FLOAT NOT NULL",
        "row_rank"         : "SMALLINT UNSIGNED NOT NULL"
    },
    "index_vars" : {
        "depth"                 : "SMALLINT UNSIGNED",
        "latest_academic_year"  : "VARCHAR(16)",
        "latest_teaching_assignment_year" : "VARCHAR(16)",
        "is_at_epfl"            : "TINYINT(1)",
        "year"                  : "YEAR",
        "is_active_unit"        : "TINYINT(1)",
        "is_research_unit"      : "TINYINT(1)",
        "subtype_rank"          : "SMALLINT UNSIGNED",
        "sort_number"           : "SMALLINT UNSIGNED",
        "is_active_affiliation" : "TINYINT(1)",
        "current_position_rank" : "SMALLINT UNSIGNED",
        "video_stream_url"      : "MEDIUMTEXT",
        "video_duration"        : "FLOAT",
        "creation_time"         : "DATETIME",
        "update_time"           : "DATETIME",
        "is_restricted"         : "TINYINT(1)",
        "available_start_date"  : "DATETIME",
        "available_end_date"    : "DATETIME",
        "srt_subtitles_en"      : "MEDIUMTEXT",
        "srt_subtitles_fr"      : "MEDIUMTEXT",
        "gender"                : "ENUM('Male', 'Female')",
        "published_in"          : "MEDIUMTEXT",
        "publisher"             : "MEDIUMTEXT",
        "domain"                : "ENUM('Basic Science', 'Environment', 'Life Science', 'Brain', 'Engineering', 'Urbanism', 'Computer Science', 'Business', 'Development', 'Misc', 'Water')",
        "language"              : "ENUM('English', 'French', 'German', 'Italian', 'Portuguese')",
        "level"                 : "ENUM('Bachelor', 'Hors Programme', 'Master', 'Preparatory', 'Propedeutic')",
        "platform"              : "ENUM('coursera', 'courseraod', 'courseware', 'edx', 'edx edge', 'youtube')",
        "thumbnail_image_url"   : "VARCHAR(255)",
        "lecture_source"        : "ENUM('Course', 'MOOC')",
        "teaching_formats"      : "ENUM('Course', 'MOOC', 'Course,MOOC')",
        "name_en"               : "VARCHAR(255)",
        "name_fr"               : "VARCHAR(255)",
        "timestamps"            : "MEDIUMTEXT",
        "timestamps_md5"        : "CHAR(32)",
        "n_timestamps"          : "SMALLINT UNSIGNED",
        "detection_sum_score"   : "FLOAT",
        "detection_avg_score"   : "FLOAT",
        "detection_max_score"   : "FLOAT"
    },
    None : {}
}

#-------------------------#
# Column keys definitions #
#-------------------------#

# Column keys JSON
table_keys_json = {
    "from_to_edges" : {
        "from_id" : "PRIMARY KEY",
        "to_id"   : "PRIMARY KEY"
    },
    "object" : {
        "institution_id" : "PRIMARY KEY",
        "object_type"    : "PRIMARY KEY",
        "object_id"      : "PRIMARY KEY",
        "field_language" : "PRIMARY KEY",
        "field_name"     : "PRIMARY KEY"
    },
    "object_to_object" : {
        "from_institution_id" : "PRIMARY KEY",
        "from_object_type"    : "PRIMARY KEY",
        "from_object_id"      : "PRIMARY KEY",
        "to_institution_id"   : "PRIMARY KEY",
        "to_object_type"      : "PRIMARY KEY",
        "to_object_subtype"   : "PRIMARY KEY",
        "to_object_id"        : "PRIMARY KEY",
        "field_language"      : "PRIMARY KEY",
        "field_name"          : "PRIMARY KEY",
        "context"             : "PRIMARY KEY"
    },
    "object_to_concept" : {
        "institution_id"     : "PRIMARY KEY",
        "object_type"        : "PRIMARY KEY",
        "object_id"          : "PRIMARY KEY",
        "concept_id"         : "PRIMARY KEY",
        "detection_time_hms" : "PRIMARY KEY",
    },
    "doc_profile" : {
        "institution_id" : "PRIMARY KEY",
        "object_type"    : "PRIMARY KEY",
        "object_id"      : "PRIMARY KEY",
        "is_visible"     : "KEY"
    },
    "doc_index" : {
        "doc_institution"      : "PRIMARY KEY",
        "doc_type"             : "PRIMARY KEY",
        "doc_id"               : "PRIMARY KEY"
    },
    "link_index" : {
        "doc_institution"  : "PRIMARY KEY",
        "doc_type"         : "PRIMARY KEY",
        "doc_id"           : "PRIMARY KEY",
        "link_institution" : "PRIMARY KEY",
        "link_type"        : "PRIMARY KEY",
        "link_subtype"     : "PRIMARY KEY",
        "link_id"          : "PRIMARY KEY"
    },
    "index_vars" : {
        "depth"                	: "KEY", 
        "latest_academic_year" 	: "KEY",
        "is_at_epfl"           	: "KEY",
        "year"                 	: "KEY",
        "is_active_unit"       	: "KEY",
        "is_research_unit"		: "KEY",
        "subtype_rank"			: "KEY",
        "is_active_affiliation" : "KEY",
        "current_position_rank" : "KEY",
        "gender"                : "KEY",
        "domain"                : "KEY",
        "language"              : "KEY",
        "level"                 : "KEY",
        "platform"              : "KEY",
        "lecture_source"        : "KEY",
        "name_en"               : "KEY",
        "name_fr"               : "KEY",
        "timestamps_md5"        : "KEY",
        "n_timestamps"          : "KEY"
    },
    None : {}
}

# Function to get the table type from the table name
def get_table_type_from_name(table_name):
    
    match_gen_from_to_edges    = re.findall(r"Edges_N_[^_]*_[^_]*_N_[^_]*_[^_]*_T_(GBC|AS)$", table_name)
    match_obj_to_obj_edges     = re.findall(r"Edges_N_[^_]*_N_(?!Concept)[^_]*_T_[^_]*$", table_name)
    match_obj_to_concept_edges = re.findall(r"Edges_N_[^_]*_N_Concept_T_[^_]*$", table_name)
    match_data_object          = re.findall(r"Data_N_Object_T_[^_]*(_COPY)?$", table_name)
    match_data_obj_to_obj      = re.findall(r"Data_N_Object_N_Object_T_[^_]*$", table_name)
    match_doc_index            = re.findall(r"Index_D_[^_]*(_COPY)?$", table_name)
    match_link_index           = re.findall(r"Index_D_[^_]*_L_[^_]*_T_[^_]*(_Search)?(_COPY)?$", table_name)
    match_stats_object         = re.findall(r"Stats_N_Object_T_[^_]*$", table_name)
    match_stats_obj_to_obj     = re.findall(r"Stats_N_Object_N_Object_T_[^_]*$", table_name)
    match_buildup_docs         = re.findall(r'^IndexBuildup_Fields_Docs_[^_]*', table_name)
    match_buildup_links        = re.findall(r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*', table_name)
    match_scores_matrix        = re.findall(r"Edges_N_Object_N_Object_T_ScoresMatrix_AS$", table_name)

    if match_gen_from_to_edges:
        return 'from_to_edges'
    elif match_obj_to_obj_edges:
        return 'object_to_object'
    elif match_obj_to_concept_edges:
        return 'object_to_concept'
    elif match_data_object:
        if 'PageProfile' in table_name:
            return 'doc_profile'
        else:
            return 'object'
    elif match_data_obj_to_obj:
        return 'object_to_object'
    elif match_doc_index:
        return 'doc_index'
    elif match_link_index:
        return 'link_index'
    elif match_stats_object:
        return 'object'
    elif match_stats_obj_to_obj:
        return 'object_to_object'
    elif match_buildup_docs:
        return 'doc_index'
    elif match_buildup_links:
        return 'link_index'
    elif match_scores_matrix:
        return 'object_to_object'
    else:
        return None

#----------------------#
# Temporary parameters #
#----------------------#

# Estimated processing time per row
processing_times_per_row = {
    'materialise_view' : 30*60/15908574,
    'apply_datatypes'  : 115.27/15908574,
    'apply_keys'       : 404.88/15908574
}

# Local cache
local_cache = {
    'checksums_calculation' : {
        'graph_lectures' : {
            'Data_N_Object_T_*' : {
                "Lecture": {
                    "CustomFields": [
                        "available_end_date",
                        "available_start_date",
                        "is_restricted",
                        "original_description",
                        "original_tags",
                        "original_title",
                        "platform",
                        "recording_date",
                        "srt_subtitles",
                        "video_duration",
                        "video_modified_date",
                        "video_stream_url",
                        "video_upload_date"
                    ],
                    "CalculatedFields": [
                        "n_slides"
                    ]
                },
                "Slide": {
                    "CustomFields": [
                        "detected_language",
                        "text_en",
                        "text_original"
                    ],
                    "CalculatedFields": []
                }
            },
            'Data_N_Object_N_Object_T_*' : {
                "Course-Lecture": {
                    "CustomFields": [
                        "academic_year,sort_number"
                    ],
                    "CalculatedFields": [
                        "latest_academic_year"
                    ]
                },
                "Lecture-Slide": {
                    "CustomFields": [
                        "end_time_hms",
                        "end_timestamp",
                        "sort_number",
                        "start_time_hms",
                        "start_timestamp",
                        "time_hms",
                        "timestamp"
                    ]
                },
                "Lecture-Transcript": {
                    "CustomFields": [
                        "sort_number"
                    ]
                },
                "MOOC-Lecture": {
                    "CustomFields": [
                        "session_id,sort_number"
                    ]
                }
            }
        }
    }
}

#---------------------#
# Auxiliary functions #
#---------------------#

# Print in colour
def print_colour(msg, colour='white', background='black', style='normal', display_method=False):
    colour_codes = {
        'black'  : 30,
        'red'    : 31,
        'green'  : 32,
        'yellow' : 33,
        'blue'   : 34,
        'purple' : 35,
        'magenta': 35,
        'cyan'   : 36,
        'white'  : 37
    }
    background_codes = {
        'black'  : 40,
        'red'    : 41,
        'green'  : 42,
        'yellow' : 43,
        'blue'   : 44,
        'purple' : 45,
        'magenta': 45,
        'cyan'   : 46,
        'white'  : 47
    }
    style_codes = {
        'normal'  : 0,
        'bold'    : 1,
        'underline': 4,
        'blink'   : 5,
        'reverse' : 7,
        'hidden'  : 8
    }
    
    if display_method:
        frame = inspect.currentframe().f_back
        method = frame.f_code.co_name

        # Attempt to get class name from 'self' or 'cls'
        class_name = None
        if 'self' in frame.f_locals:
            class_name = type(frame.f_locals['self']).__name__
        elif 'cls' in frame.f_locals:
            class_name = frame.f_locals['cls'].__name__

        if class_name:
            msg = f"{class_name}.{method}(): {msg}"
        else:
            msg = f"{method}(): {msg}"

    print(f"\033[{style_codes[style]};{colour_codes[colour]};{background_codes[background]}m{msg}\033[0m")

# Pretty-print dataframe
def print_dataframe(df, title):
    print('')
    print_colour(title, colour='white', background='black', style='bold')
    print(tabulate(df, headers=df.columns, tablefmt='fancy_grid', showindex=False))
    print('')    

# Get input key from keyboard
def get_keypress():
    """Capture a single keypress without needing to press Enter."""
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        key = sys.stdin.read(1)  # Read a single character
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return key

# Handle progress output
def es_write_progress(prog, total, status=''):
    sys.stdout.write(f"\r{prog} of {total} documents indexed... {status}")
    sys.stdout.flush()

# Calculate the normalized Levenshtein distance between two strings
def normalized_levenshtein(str1, str2):

    # Calculate the raw Levenshtein distance
    distance = Levenshtein.distance(str1, str2)
    
    # Get the length of the longer string
    max_len = max(len(str1), len(str2))
    
    # If both strings are empty, consider them identical
    if max_len == 0:
        return 1.0
    
    # Normalize the distance to a value between 0 and 1
    normalized_distance = 1 - (distance / max_len)
    
    # Return the normalized distance
    return normalized_distance

# Function to get access token from GraphAI
def graphai_get_access_token(username, password):
    response_login = requests.post(
        url=f'{global_config["graphai"]["url"]}/token', data={'username': username, 'password': password}
    )
    return response_login.json()['access_token']

# Function to detect concepts through GraphAI
headers = None
def graphai_text_endpoint(object_id, input, endpoint='wikify'):

    #Request access token if needed
    if headers is None:
        response_login = requests.post(
            url=f'{global_config["graphai"]["url"]}/token',
            data={'username': global_config['graphai']['user'], 'password': global_config['graphai']['password']}
        )
        graphai_access_token = response_login.json()['access_token']
        # Define the headers
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {graphai_access_token}',
            'Content-Type': 'application/json'
        }

    # Check input type
    if type(input) is str:

        # Generate md5 from title
        request_md5 = hashlib.md5(input.encode()).hexdigest()

    elif type(input) is list:

        # Generate md5 from title
        request_md5 = hashlib.md5(''.join(input).encode()).hexdigest()

    # Define the JSON payload
    json_payload = {
        "object_id" : object_id,
        "request_md5" : request_md5,
        {str:"raw_text", list:"keywords"}[type(input)] : input
    }

    # Initiate the output JSON file
    output_json = {
        "payload"  : None,
        "response" : None,
        "timestamp_sent" : None,
        "timestamp_received" : None,
    }

    # Save the payload to the output JSON
    output_json['payload'] = deepcopy(json_payload)
    
    # Send the POST request
    output_json['timestamp_sent'] = datetime.datetime.now().timestamp()
    try:
        response = requests.post(f"https://graphai.epfl.ch/text/{endpoint}", headers=headers, json=json_payload)
    except:
        print(f'Error: Request failed for ID {object_id}.')
        return None
    output_json['timestamp_received'] = datetime.datetime.now().timestamp()

    # Get the request body from the response
    response_request_body = json.loads(deepcopy(response.request.body))

    # Get the JSON response
    response_json = deepcopy(response.json())
    output_json['response'] = deepcopy(response_json)

    # Check if the token has expired
    if 'detail' in response_json:
        if 'Token has expired' in response_json['detail']:
            print('Token has expired or has an invalid timestamp, obtain another')
            return None
        elif "Could not validate credentials" in response_json['detail']:
            print('Could not validate credentials')
            return None
        else:
            print('Unknown error')
            return None

    # Check if the response is valid
    if response_request_body != json_payload:

        # Print error message
        print(f'Error: Request body does not match the input for ID {object_id}. Saving as logged error ...')

        # Save output as logged error
        with open(f'errors/{object_id}_{request_md5}.json', 'w') as fid:
            fid.write(json.dumps(output_json, indent=2))

        # Return None
        return None

    # Wait for a bit
    time.sleep(0.1)

    # Return the response
    return output_json

# Function to execute an INSERT operation in the registry
def registry_insert(
        schema_name, table_name, key_column_names, key_column_values, upd_column_names, upd_column_values, actions=(),
        db_connector=None
):
    """
    Possible actions: 'print', 'eval', 'commit'
    """

    # Generate the full table name
    t = f'{schema_name}.{table_name}'

    # Get the number of columns to update and create the dictionary with values
    num_upd_columns = len(upd_column_names)
    num_key_columns = len(key_column_names)
    sql_params = {key_column_names[k]: key_column_values[k] for k in range(num_key_columns)}
    sql_params.update({upd_column_names[u]: upd_column_values[u] for u in range(num_upd_columns)})

    # Initialise test results dictionary
    eval_results = None

    # Evaluate changes to be made
    if 'eval' in actions:

        # Define the colour map
        colour_map = {
            'no change'     : 'green',
            'new value'     : 'cyan',
            'set to null'   : 'red',
            'key exists'    : 'green',
            'key is new'    : 'cyan'
        }

        # Generate SELECT statement
        if num_upd_columns > 0:
            if_statements = []
            for k in range(num_upd_columns):
                if isinstance(upd_column_values[k], float):
                    if_statements.append(
                        f'IF('
                            f'ABS({upd_column_names[k]} - :{upd_column_names[k]})<1e-6 '
                                f'OR (:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NULL), '
                            f'"no change", '
                            f'IF(:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NOT NULL, "set to null", "new value")'
                        f') AS TEST_{upd_column_names[k]}'
                    )
                else:
                    if_statements.append(
                        f'IF('
                            f'({upd_column_names[k]} = :{upd_column_names[k]}) '
                                f'OR (:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NULL), '
                            f'"no change", '
                            f'IF(:{upd_column_names[k]} IS NULL AND {upd_column_names[k]} IS NOT NULL, "set to null", "new value")'
                        f') AS TEST_{upd_column_names[k]}'
                    )
            sql_select_statement = ', '.join(if_statements)
        else:
            sql_select_statement = '*'

        # Generate the SQL query for evaluation
        sql_query_eval = f"""
            SELECT {sql_select_statement}
            FROM {t}
            WHERE ({', '.join(key_column_names)}) = (:{', :'.join(key_column_names)});
        """

        # Print the SQL query
        if 'print' in actions:
            print(sql_query_eval)
        
        # Execute the query
        out = db_connector.execute_query(engine_name='test', query=sql_query_eval, params=sql_params)

        # Build up the test results dictionary
        print_colour(f'\nChanges on table {t}:', style='bold')
        eval_result = 'key exists' if len(out) > 0 else 'key is new'
        eval_results = [{'column': 'primary_key', 'result': eval_result}]
        print(f"primary_key {'.'*(32-len('primary_key'))} ", end="", flush=True)
        print_colour(eval_result, colour=colour_map[eval_result])
        if len(out) > 0:
            for k in range(num_upd_columns):
                eval_result = out[0][k]
                eval_results.append({'column': upd_column_names[k], 'result': eval_result})
                print(f"{upd_column_names[k]} {'.'*(32-len(upd_column_names[k]))} ", end="", flush=True)
                print_colour(eval_result, colour=colour_map[eval_result])
        
    # Generate the SQL query for commit
    if num_upd_columns > 0:
        sql_query_commit = f"""
            INSERT INTO {t}
                ({', '.join(key_column_names)}, {', '.join(upd_column_names)})
            SELECT {', '.join(key_column_names)}, {', '.join(upd_column_names)}
            FROM (
                SELECT 
                    {', '.join([f':{key_column_names[k]} AS {key_column_names[k]}' for k in range(num_key_columns)])},
                    {', '.join([f':{upd_column_names[u]} AS {upd_column_names[u]}' for u in range(num_upd_columns)])}
            ) AS d
            ON DUPLICATE KEY UPDATE 
                record_updated_date = IF(
                    {' OR '.join([f"COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__')" for c in upd_column_names])},
                    CURRENT_TIMESTAMP, 
                    {t}.record_updated_date
                ),
                {', '.join(
                    [f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})" for c in upd_column_names]
                )};"""
    else:
        sql_query_commit = f"""
            INSERT IGNORE INTO {t}
                ({', '.join(key_column_names)})
            SELECT 
                {', '.join([f':{key_column_names[k]} AS {key_column_names[k]}' for k in range(num_key_columns)])};"""

    # Print the SQL query
    if 'print' in actions:
        print(sql_query_commit)

    # Execute commit
    if 'commit' in actions:
        db_connector.execute_query(engine_name='test', query=sql_query_commit, params=sql_params, commit=True)

    # Return the test results
    return eval_results

def delete_nodes_by_ids(db_connector, institution_id, object_type, nodes_id: List[str], engine_name='test', actions=()):
    schema_objects = object_type_to_schema.get(object_type, 'graph_registry')
    query_where_per_table = {}
    for table in (
            'Nodes_N_Object', 'Data_N_Object_T_PageProfile', 'Data_N_Object_T_CustomFields',
            'Edges_N_Object_N_Concept_T_ConceptDetection'
    ):
        query_where_per_table[f'{schema_objects}.{table}'] = \
            f'institution_id="{institution_id}" AND object_type="{object_type}" AND object_id IN :object_id'

    for schema in ('graph_registry', 'graph_lectures'):
        for table in (
                'Edges_N_Object_N_Object_T_ChildToParent', 'Data_N_Object_N_Object_T_CustomFields',
        ):
            query_where_per_table[f'{schema}.{table}'] = f'''
                (
                    from_institution_id='{institution_id}' 
                    AND from_object_type='{object_type}' 
                    AND from_object_id IN :object_id
                ) OR (
                    to_institution_id='{institution_id}' 
                    AND to_object_type='{object_type}' 
                    AND to_object_id IN :object_id
                )'''
    eval_results = {} if 'eval' in actions else None
    query_remove = ''
    for table, query_where in query_where_per_table.items():
        if 'eval' in actions:
            query_eval = f'SELECT COUNT(*) FROM {table} WHERE {query_where};'
            if 'print' in actions:
                print(query_eval)
            out = db_connector.execute_query(query=query_eval, params={'object_id': nodes_id}, engine_name=engine_name)
            eval_results[table] = out[0][0]
            if eval_results[table] > 0:
                print(table, eval_results[table])
        query_remove += f'DELETE FROM {table} WHERE {query_where};\n'
    if 'print' in actions:
        print(query_remove)
    if 'commit' in actions:
        db_connector.execute_query(
            query=query_remove, params={'object_id': nodes_id}, engine_name=engine_name, commit=True
        )
    return eval_results

def delete_edges_by_ids(
        db_connector, from_institution_id, from_object_type, to_institution_id, to_object_type,
        edges_id: List[Tuple[str, str]], engine_name='test', actions=()
):
    schema_edges = GraphRegistry.Edge.get_schema(from_object_type, to_object_type)
    query_where_per_table = {}
    for table in (
            'Edges_N_Object_N_Object_T_ChildToParent', 'Data_N_Object_N_Object_T_CustomFields'
    ):
        query_where_per_table[f'{schema_edges}.{table}'] = f'''
            from_institution_id="{from_institution_id}" 
            AND from_object_type="{from_object_type}" 
            AND to_institution_id="{to_institution_id}" 
            AND to_object_type="{to_object_type}" 
            AND (from_object_id, to_object_id) IN :edges_id'''
    eval_results = {} if 'eval' in actions else None
    query_remove = ''
    for table, query_where in query_where_per_table.items():
        if 'eval' in actions:
            query_eval = f'SELECT COUNT(*) FROM {table} WHERE {query_where};'
            if 'print' in actions:
                print(query_eval)
            out = db_connector.execute_query(query=query_eval, params={'edges_id': edges_id}, engine_name=engine_name)
            eval_results[table] = out[0][0]
            if eval_results[table] > 0:
                print(table, eval_results[table])
        query_remove += f'DELETE FROM {table} WHERE {query_where};\n'
    if 'print' in actions:
        print(query_remove)
    if 'commit' in actions:
        db_connector.execute_query(
            query=query_remove, params={'edges_id': edges_id}, engine_name=engine_name, commit=True
        )
    return eval_results

# Get the list of object_id from the existing nodes in the database
def get_existing_nodes_id(db_connector, institution_id: str, object_type: str, engine_name='test'):
    schema_name = object_type_to_schema.get(object_type, 'graph_registry')
    existing_nodes_id = db_connector.execute_query(
        engine_name=engine_name,
        query=f"""
            SELECT object_id 
            FROM {schema_name}.Nodes_N_Object
            WHERE institution_id='{institution_id}' AND object_type='{object_type}';"""
    )
    return [object_id for object_id, in existing_nodes_id]

def get_existing_edges_id(
        db_connector, from_institution_id: str, from_object_type: str, to_institution_id: str, to_object_type: str,
        engine_name='test'
):
    schema_name = GraphRegistry.Edge.get_schema(from_object_type, to_object_type)
    existing_edges_id = db_connector.execute_query(
        engine_name=engine_name,
        query=f"""
            SELECT from_object_id, to_object_id
            FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent
            WHERE 
                from_institution_id='{from_institution_id}' AND from_object_type='{from_object_type}'
                AND to_institution_id='{to_institution_id}' AND to_object_type='{to_object_type}';"""
    )
    return existing_edges_id

#-----------------------------------------#
# Class definition for Graph MySQL engine #
#-----------------------------------------#
class GraphDB():

    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)  # Use `object.__new__()` explicitly
            cls._instance._initialized = False  # Flag for initialization check
        return cls._instance

    def __init__(self, name="GraphDB"):
        if not self._initialized:  # Prevent reinitialization
            self.name = name
            self._initialized = True  # Mark as initialized
            print(f"GraphDB initialized with name: {self.name}")

        # Initialize the MySQL engines
        self.params_test, self.engine_test = self.initiate_engine(global_config['mysql']['server_test'])
        self.params_prod, self.engine_prod = self.initiate_engine(global_config['mysql']['server_prod'])
        self.params = {'test': self.params_test, 'prod': self.params_prod}
        self.engine = {'test': self.engine_test, 'prod': self.engine_prod}

        # Set the MySQL password in an environment variable
        os.environ['MYSQL_TEST_PWD'] = self.params_test['password']
        os.environ['MYSQL_PROD_PWD'] = self.params_prod['password']

        # Build base shell command (for MySQL)
        self.base_command_mysql = {
            'test': [global_config['mysql']['client_bin'], '-u', self.params_test['username'], f'--password={os.getenv("MYSQL_TEST_PWD")}', '-h', self.params_test['host_address'], '-P', str(self.params_test['port'])],
            'prod': [global_config['mysql']['client_bin'], '-u', self.params_prod['username'], f'--password={os.getenv("MYSQL_PROD_PWD")}', '-h', self.params_prod['host_address'], '-P', str(self.params_prod['port'])]
        }

        # Build base shell command (for MySQLDump)
        self.base_command_mysqldump = {
            'test': [global_config['mysql']['dump_bin'], '-u', self.params_test['username'], f'--password={os.getenv("MYSQL_TEST_PWD")}', '-h', self.params_test['host_address'], '-P', str(self.params_test['port']), '-v', '--no-create-db', '--no-create-info', '--skip-lock-tables', '--single-transaction'],
            'prod': [global_config['mysql']['dump_bin'], '-u', self.params_prod['username'], f'--password={os.getenv("MYSQL_PROD_PWD")}', '-h', self.params_prod['host_address'], '-P', str(self.params_prod['port']), '-v', '--no-create-db', '--no-create-info', '--skip-lock-tables', '--single-transaction'],
        }

    #-------------------------------------#
    # Method: Initialize the MySQL engine #
    #-------------------------------------#
    def initiate_engine(self, server_name):
        if server_name not in global_config['mysql']:
            raise ValueError(
                f'could not find the configuration for mysql server {server_name} in {global_config_file}.'
            )
        params = global_config['mysql'][server_name]
        engine = SQLEngine(
            f'mysql+pymysql://{params["username"]}:{params["password"]}@{params["host_address"]}:{params["port"]}/'
        )
        return params, engine

    #-------------------------------#
    # Method: Test MySQL connection #
    #-------------------------------#
    def test(self, engine_name='test'):
        """
        Test the MySQL connection by executing a simple query.
        """
        try:
            connection = self.engine[engine_name].connect()
            result = connection.execute(text("SELECT 1")).fetchone()
            connection.close()
            return result[0] == 1
        except Exception as e:
            print(f"Error connecting to MySQL {engine_name}: {e}")
            return False

    #----------------------------------#
    # Method: Check if database exists #
    #----------------------------------#
    def database_exists(self, engine_name, schema_name):
        query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema_name}'"
        return len(self.execute_query(engine_name=engine_name, query=query)) > 0
    
    #-------------------------------#
    # Method: Check if table exists #
    #-------------------------------#
    def table_exists(self, engine_name, schema_name, table_name, exclude_views=False):

        # Start building the query
        query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME     = '{table_name}'
        """

        # If exclude_views is True, add a condition to exclude views
        if exclude_views:
            query += " AND TABLE_TYPE = 'BASE TABLE'"

        # Execute the query
        tables = self.execute_query(engine_name=engine_name, query=query)

        # Check if the table exists
        return len(tables) > 0

    #-------------------------------------#
    # Method: Drop a database             #
    #-------------------------------------#
    def drop_database(self, engine_name, schema_name):
        connection = self.engine[engine_name].connect()
        try:
            connection.execute(text(f'DROP DATABASE IF EXISTS {schema_name}'))
        finally:
            connection.close()

    #-------------------------------------#
    # Method: Create a database           #
    #-------------------------------------#
    def create_database(self, engine_name, schema_name, drop_database=False):
        connection = self.engine[engine_name].connect()
        if drop_database:
            self.drop_database(engine_name, schema_name)
        try:
            connection.execute(text(f'CREATE DATABASE IF NOT EXISTS {schema_name}'))
        finally:
            connection.close()

    #-------------------------------------#
    # Method: Create view from a query    #
    #-------------------------------------#
    def create_view(self, engine_name, schema_name, view_name, query):
        connection = self.engine[engine_name].connect()
        try:
            connection.execute(text(f'CREATE OR REPLACE VIEW {schema_name}.{view_name} AS {query}'))
        finally:
            connection.close()

    #---------------------#
    # Method: Drop a view #
    #---------------------#
    def drop_view(self, engine_name, schema_name, view_name):
        connection = self.engine[engine_name].connect()
        try:
            connection.execute(text(f'DROP VIEW IF EXISTS {schema_name}.{view_name}'))
        finally:
            connection.close()

    #------------------------#
    # Method: Get table size #
    #------------------------#
    def get_table_size(self, engine_name, schema_name, table_name):

        # Define the query
        query = f'SELECT COUNT(*) FROM {schema_name}.{table_name}'

        # Execute the query
        row_count = self.execute_query(engine_name=engine_name, query=query)[0][0]

        # Return the row count
        return row_count

    #-------------------------------------#
    # Method: Get table definition        #
    #-------------------------------------#
    def get_create_table(self, engine_name, schema_name, table_name):
        query = f"SHOW CREATE TABLE {schema_name}.{table_name}"
        return self.execute_query(engine_name=engine_name, query=query)[0][1]

    #-------------------------------------#
    # Method: Get column names of a table #
    #-------------------------------------#
    def get_column_names(self, engine_name, schema_name, table_name):

        # Define the query
        query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"

        # Execute the query
        column_names = []
        for r in self.execute_query(engine_name=engine_name, query=query):
            column_names.append(r[0])
        
        # Return the column names
        return column_names

    #-----------------------------------------#
    # Method: Get column datatypes of a table #
    #-----------------------------------------#
    def get_column_datatypes(self, engine_name, schema_name, table_name):
            
            # Define the query
            query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
    
            # Execute the query
            column_datatypes = {}
            for r in self.execute_query(engine_name=engine_name, query=query):
                column_datatypes[r[0]] = r[1]
            
            # Return the column datatypes
            return column_datatypes

    #----------------------------------------------------#
    # Method: Check if a table has a primary key defined #
    #----------------------------------------------------#
    def has_primary_key(self, engine_name, schema_name, table_name):
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        return len(self.execute_query(engine_name=engine_name, query=query)) > 0

    #-------------------------------------#
    # Method: Get primary keys of a table #
    #-------------------------------------#
    def get_primary_keys(self, engine_name, schema_name, table_name):
        query = f"SHOW KEYS FROM {schema_name}.{table_name} WHERE Key_name = 'PRIMARY'"
        primary_keys = []
        for r in self.execute_query(engine_name=engine_name, query=query):
            primary_keys.append(r[4])
        return primary_keys

    #--------------------------------------------------#
    # Method: Get all keys (of all types) from a table #
    #--------------------------------------------------#
    def get_keys(self, engine_name, schema_name, table_name):
        query = f"SHOW KEYS FROM {schema_name}.{table_name}"
        keys = {}
        for r in self.execute_query(engine_name=engine_name, query=query):
            key_name = r[2]
            if key_name not in keys:
                keys[key_name] = []
            keys[key_name].append(r[4])
        return keys

    #----------------------------------------------#
    # Method: Executes a query using Python module #
    #----------------------------------------------#
    def execute_query(self, engine_name, query, params=None, commit=False):
        connection = self.engine[engine_name].connect()
        try:
            result = connection.execute(text(query), parameters=params)
            if result.returns_rows:
                rows = result.fetchall()
            else:
                rows = []
            if commit:
                connection.commit()
        except Exception as e:
            print('Error executing query.')
            # print specific error
            print(e)
            raise e
            rows = []
        finally:
            connection.close()
        return rows
    
    #------------------------------------------------------------------#
    # Method: Executes/Evaluates a query using ON DUPLICATE KEY UPDATE #
    #------------------------------------------------------------------#
    def execute_query_as_safe_inserts(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=()):

        # Target table path
        t = target_table_path = f'{schema_name}.{table_name}'
        
        # Evaluate the patch operation
        if 'eval' in actions:

            # Generate evaluation query
            query_eval = f"""
                SELECT {', '.join(eval_column_names)}, COUNT(*) AS n_to_process
                  FROM (
                        {query}
                       ) t
              GROUP BY {', '.join(eval_column_names)}
            """

            # Print the evaluation query
            if 'print' in actions:
                print(query_eval)

            # Execute the evaluation query and print the results
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            df = pd.DataFrame(out, columns=eval_column_names+['# to process'])
            print_dataframe(df, title=f'🔍 Evaluation results for {target_table_path}:')

        # Generate the SQL commit query
        query_commit = f"""
                 INSERT INTO {target_table_path}
                             ({', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)})
                      SELECT  {', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)}
                        FROM (
                              {query}
                             ) AS d
            ON DUPLICATE KEY
                      UPDATE {', '.join([f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})" for c in upd_column_names])};
        """

        # Print the commit query
        if 'print' in actions:
            print(query_commit)

        # Execute the commit query
        if 'commit' in actions:
            self.execute_query_in_shell(engine_name=engine_name, query=query_commit)

    #-------------------------------------------------#
    # Method: Executes a query sequentially by chunks #
    #-------------------------------------------------#
    def execute_query_in_chunks(self, engine_name, schema_name, table_name, query, chunk_size=10000, row_id_name='row_id'):

        # Remove trailing semicolon from the query
        if query.strip()[-1] == ';':
            query = query.strip()[:-1]

        # Which filter command to use?
        if 'WHERE' in query:
            filter_command = 'AND'
        else:
            filter_command = 'WHERE'

        # Row_id name contains alias?
        if '.' in row_id_name:
            row_id_name_no_alias = row_id_name.split('.')[1]
        else:
            row_id_name_no_alias = row_id_name

        # Get min and max row_id
        row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT MIN({row_id_name_no_alias}) FROM {schema_name}.{table_name}")[0][0]
        row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT MAX({row_id_name_no_alias}) FROM {schema_name}.{table_name}")[0][0]
        n_rows = row_num_max - row_num_min + 1

        # Process table in chunks
        for offset in tqdm(range(row_num_min, row_num_max, chunk_size), desc=f'Executing query', unit='chunks', total=round(n_rows/chunk_size)):

            # Generate SQL query
            sql_query = f"{query} {filter_command} {row_id_name} BETWEEN {offset} AND {offset + chunk_size - 1};"

            # Execute the query
            self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

    #---------------------------------------------#
    # Method: Executes a query in the MySQL shell #
    #---------------------------------------------#
    def execute_query_in_shell(self, engine_name, query, verbose=False):

        # Define the shell command
        shell_command = self.base_command_mysql[engine_name] + ['-e', query]

        # If verbose is enabled, print the command being executed
        if verbose:
            print("Executing query:", query)

        # Run the command and capture stdout and stderr
        result = subprocess.run(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # If verbose is enabled, print the outputs
        if verbose:
            print("Command stdout:", result.stdout)
            print("Command stderr:", result.stderr)

        # Check if there's a MySQL-specific warning
        if result.stderr:
            if result.stderr.strip() == 'mysql: [Warning] Using a password on the command line interface can be insecure.':
                # Suppress the warning by doing nothing
                pass
            else:
                # Print the stderr output if it's not the specific MySQL warning
                print('Error executing query:', query)
                print(result.stderr)
                if 'ERROR' in result.stderr:
                    print('MySQL error:', result.stderr)
                    return False

        # Return the result
        return result

    #--------------------------------------------------------------#
    # Method: Executes a query in the MySQL shell from an SQL file #
    #--------------------------------------------------------------#
    def execute_query_from_file(self, engine_name, file_path, verbose=False):
       
        # Define the shell command (including verbose output if requested)
        shell_command = self.base_command_mysql[engine_name] + ['-v' if verbose else ''] + ['-e', f'SOURCE {file_path}']

        # Run the command and capture stdout and stderr
        result = subprocess.run(shell_command, stdout=None, stderr=None, text=True)

        # Check if there's a MySQL-specific warning
        if result.stderr:
            if result.stderr.strip() == 'mysql: [Warning] Using a password on the command line interface can be insecure.':
                # Suppress the warning by doing nothing
                pass
            else:
                # Print the stderr output if it's not the specific MySQL warning
                print('Error executing query from file:', file_path)
                print(result.stderr)
                if 'ERROR' in result.stderr:
                    return False
        
        # Return the result
        return result

    #--------------------------------------------------#
    # Method: Drop all keys in a table (except row_id) #
    #--------------------------------------------------#
    def drop_keys(self, engine_name, schema_name, table_name, ignore_keys=['row_id']):

        # Get all keys
        keys = self.get_keys(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Check if there are any keys to drop
        if len(keys) == 0:
            return

        # Build the query for dropping keys (except row_id) all at once
        query = f'ALTER TABLE {schema_name}.{table_name}'
        for key_name, key_columns in keys.items():
            if key_name not in ignore_keys:
                if key_name == 'PRIMARY':
                    query += ' DROP PRIMARY KEY,'
                else:
                    query += f' DROP KEY {key_name},'
        if query.endswith(','):
            query = query[:-1]

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=query)

    #-----------------------------------------------#
    # Method: Create target table like source table #
    #-----------------------------------------------#
    def create_table_like(self, engine_name, source_schema_name, source_table_name, target_schema_name, target_table_name, drop_table=False, drop_keys=False):

        # Drop the target table if it exists
        if drop_table:
            self.execute_query(engine_name=engine_name, query=f"DROP TABLE IF EXISTS {target_schema_name}.{target_table_name}")

        # Execute the CREATE TABLE query
        self.execute_query(engine_name=engine_name, query=f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{target_table_name} LIKE {source_schema_name}.{source_table_name}")

        # Drop all keys in the target table
        if drop_keys:
            self.drop_keys(engine_name=engine_name, schema_name=target_schema_name, table_name=target_table_name)

    #----------------------------------#
    # Method: Drop a table in a schema #
    #----------------------------------#
    def drop_table(self, engine_name, schema_name, table_name):
        self.execute_query(engine_name=engine_name, query=f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

    #------------------------------------#
    # Method: Rename a table in a schema #
    #------------------------------------#
    def rename_table(self, engine_name, schema_name, table_name, rename_to, replace_existing=False, simulation_mode=False):

        # Check if the source table exists
        if not self.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
            sysmsg.error(f"Table {schema_name}.{table_name} does not exist.")
            return
        
        # Check if the target table exists
        if self.table_exists(engine_name=engine_name, schema_name=schema_name, table_name=rename_to):
            sysmsg.warning(f"Table {schema_name}.{rename_to} already exists. Flag 'replace_existing' set to {replace_existing}.")
            if not replace_existing:
                sysmsg.warning("Table not renamed.")
                return

        # Drop the new table if it exists
        if replace_existing:
            sysmsg.warning(f"Dropping existing target table {schema_name}.{rename_to}")
            if not simulation_mode:
                self.drop_table(engine_name=engine_name, schema_name=schema_name, table_name=rename_to)
            else:
                sysmsg.info(f"Simulation mode: Dropping existing target table {schema_name}.{rename_to}")

        # Generate the SQL query
        sql_query = f"ALTER TABLE {schema_name}.{table_name} RENAME {schema_name}.{rename_to}"

        # Rename the table
        if not simulation_mode:
            self.execute_query(engine_name=engine_name, query=sql_query)
            sysmsg.success(f"Table renamed: {schema_name}.{table_name} --> {schema_name}.{rename_to}")
        else:
            sysmsg.info(f"Simulation mode: {schema_name}.{table_name} --> {schema_name}.{rename_to}")

    #-------------------------------------------------------#
    # Method: Copy a table definition from source to target #
    #-------------------------------------------------------#
    def copy_create_table(self, source_engine_name, source_schema_name, source_table_name, target_engine_name, target_schema_name, target_table_name, ignore_if_exists=False, drop_table=False, drop_keys=False):

        # Check if the target table exists
        if ignore_if_exists:
            if self.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name):
                sysmsg.warning(f"Table {target_schema_name}.{target_table_name} already exists. Flag 'ignore_if_exists' set to {ignore_if_exists}.")
                sysmsg.warning("Table not copied.")
                return

        # Get the create table SQL
        create_table_sql = self.get_create_table(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name)

        # Drop the target table if it exists
        if drop_table:
            self.drop_table(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name)

        # Use the target database
        self.execute_query(engine_name=target_engine_name, query=f'USE {target_schema_name}')

        # Fix missing namespace in the create table SQL
        create_table_sql = create_table_sql.replace("CREATE TABLE ", f"CREATE TABLE {target_schema_name}.")

        # Execute the create table SQL
        self.execute_query(engine_name=target_engine_name, query=create_table_sql)

        # Drop all keys in the target table
        if drop_keys:
            self.drop_keys(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name)

    #----------------------------------------------#
    # Method: Copies a table from source to target #
    #----------------------------------------------#
    def copy_table(self, engine_name, source_schema_name, source_table_name, target_schema_name, target_table_name, list_of_columns=False, where_condition='TRUE', row_id_name=None, chunk_size=100000, create_table=False, drop_keys=False, use_replace_or_ignore=False):

        # Create the target table if it does not exist
        if create_table:
            self.create_table_like(engine_name=engine_name, source_schema_name=source_schema_name, source_table_name=source_table_name, target_schema_name=target_schema_name, target_table_name=target_table_name, drop_table=False, drop_keys=True)

        # Drop all keys in the target table
        if drop_keys:
            self.drop_keys(engine_name=engine_name, schema_name=target_schema_name, table_name=target_table_name)

        # Define the insert or replace statement
        if use_replace_or_ignore == 'REPLACE':
            insert_replace_or_ignore = 'REPLACE'
            print('Using REPLACE ...')
        elif use_replace_or_ignore == 'IGNORE':
            insert_replace_or_ignore = 'INSERT IGNORE'
            print('Using INSERT IGNORE ...')
        else:
            insert_replace_or_ignore = 'INSERT'
            print('Using INSERT (default) ...')

        # Get min and max row_id
        row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT MIN({row_id_name}) FROM {source_schema_name}.{source_table_name}")[0][0]
        row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT MAX({row_id_name}) FROM {source_schema_name}.{source_table_name}")[0][0]
        n_rows = row_num_max - row_num_min + 1

        # Process table in chunks
        for offset in tqdm(range(row_num_min, row_num_max, chunk_size), desc=f'Copying table', unit='rows', total=round(n_rows/chunk_size)):

            # Generate SQL query
            sql_query = f"""
                {insert_replace_or_ignore} INTO {target_schema_name}.{target_table_name}
                                                {' ' if list_of_columns is False else '(%s)' % ', '.join(list_of_columns)}
                                         SELECT {'*' if list_of_columns is False else  '%s'  % ', '.join(list_of_columns)}
                                           FROM {source_schema_name}.{source_table_name}
                                          WHERE {where_condition}
            """
            
            # Add row_id condition if specified
            if row_id_name is not None:
                sql_query += f"""AND {row_id_name} BETWEEN {offset} AND {offset + chunk_size - 1}"""

            # Execute the query
            self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

            # Break if not processed in chunks
            if row_id_name is None:
                break
    
    #---------------------------------------------#
    # Method: Copies a view from source to target #
    #---------------------------------------------#
    def copy_view_definition(self, engine_name, source_schema_name, source_view_name, target_schema_name, target_view_name, drop_view=False):

        # Drop the target view if it exists
        if drop_view:
            self.drop_view(engine_name=engine_name, schema_name=target_schema_name, view_name=target_view_name)

        # Get the view definition
        view_definition = self.get_create_table(engine_name=engine_name, schema_name=source_schema_name, table_name=source_view_name)

        # Fix the view definition
        view_definition = view_definition.replace(f'`{source_schema_name}`', f'`{target_schema_name}`')
        view_definition = re.sub(r"CREATE ALGORITHM=UNDEFINED DEFINER=`[^`]*`@`[^`]*` SQL SECURITY DEFINER VIEW `[^`]*`.`[^`]*` AS ", "", view_definition)

        # Create the view in the target schema
        self.create_view(engine_name=engine_name, schema_name=target_schema_name, view_name=target_view_name, query=view_definition)

    #----------------------------------------------#
    # Method: Print list of schemas in an engine   #
    #----------------------------------------------#
    def print_schemas(self, engine_name):
        print(f"List of schemas in {engine_name}:")
        for r in self.execute_query(engine_name=engine_name, query='SHOW DATABASES'):
            print(' - ', r[0])
    
    #----------------------------------------#
    # Method: Get list of tables in a schema #
    #----------------------------------------#
    def get_tables_in_schema(self, engine_name, schema_name, include_views=False, filter_by=False, use_regex=False):
        
        # Get the list of tables and views
        query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}'"
        
        # Include views if requested
        if not include_views:
            query += " AND TABLE_TYPE = 'BASE TABLE'"

        # Get the list of tables
        list_of_tables = [r[0] for r in self.execute_query(engine_name=engine_name, query=query)]

        # Filter the list of tables if requested
        if filter_by and not use_regex:
            list_of_tables = [t for t in list_of_tables if any([f in t for f in filter_by])]

        # Filter the list of tables using regex if requested
        if use_regex:
            list_of_tables = [t for t in list_of_tables if any([re.search(f, t) for f in use_regex])]
        
        # Execute the query and return the result
        return list_of_tables
    
    #-------------------------------#
    # Method: Get views in a schema #
    #-------------------------------#
    def get_views_in_schema(self, engine_name, schema_name):

        # Get the list of views
        query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'VIEW'"

        # Execute the query and return the result
        return [r[0] for r in self.execute_query(engine_name=engine_name, query=query)]

    #----------------------------------------------#
    # Method: Print list of tables in a schema     #
    #----------------------------------------------#
    def print_tables_in_schema(self, engine_name, schema_name):
        print(f"Tables in schema {schema_name}:")
        for r in self.execute_query(engine_name=engine_name, query=f'SHOW TABLES IN {schema_name}'):
            print(' - ', r[0])
    
    #----------------------------------------------#
    # Method: Print list of tables in the cache    #
    #----------------------------------------------#
    def print_tables_in_cache(self):
        self.print_tables_in_schema(engine_name='test', schema_name=global_config['mysql']['schema_cache'])
    
    #----------------------------------------------#
    # Method: Print list of tables in the test     #
    #----------------------------------------------#
    def print_tables_in_test(self):
        self.print_tables_in_schema(engine_name='test', schema_name=global_config['mysql']['schema_test'])

    #-------------------------------------------------#
    # Method: Apply data types to a table (from JSON) #
    #-------------------------------------------------#
    def apply_datatypes(self, engine_name, schema_name, table_name, datatypes_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:
                
                # Display the current time
                print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")
    
                # Calculate the estimated processing time
                processing_time = processing_times_per_row['apply_datatypes'] * estimated_num_rows
    
                # Display the estimated processing time in # hours, # min and # sec format
                print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        column_names = self.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Build the sql query for applying data types
        sql_query = f"ALTER TABLE {schema_name}.{table_name} "

        # Loop over the column names
        for column_name in column_names:
            if column_name in datatypes_json:
                sql_query += f"MODIFY COLUMN {column_name} {datatypes_json[column_name]}, "

        # Remove the trailing comma and space
        if sql_query.endswith(', '):
            sql_query = sql_query[:-2]

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

        # Print the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

    #-------------------------------------------#
    # Method: Apply keys to a table (from JSON) #
    #-------------------------------------------#
    def apply_keys(self, engine_name, schema_name, table_name, keys_json, display_elapsed_time=False, estimated_num_rows=False):

        # Display processing time estimate
        if estimated_num_rows:
                
                # Display the current time
                print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")
    
                # Calculate the estimated processing time
                processing_time = processing_times_per_row['apply_keys'] * estimated_num_rows
    
                # Display the estimated processing time in # hours, # min and # sec format
                print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Get the column names
        column_names = self.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Build composite primary key
        composite_primary_key = ''
        for column_name in keys_json:
            if keys_json[column_name] == 'PRIMARY KEY' and column_name in column_names:
                composite_primary_key += column_name + ', '
        
        # Remove the trailing comma and space
        if composite_primary_key.endswith(', '):
            composite_primary_key = composite_primary_key[:-2]

        # Build the sql query for applying keys
        sql_query = f"ALTER TABLE {schema_name}.{table_name} "

        # Append the composite primary key
        if composite_primary_key:
            sql_query += f"ADD PRIMARY KEY ({composite_primary_key}), "
            sql_query += f"ADD UNIQUE KEY uid ({composite_primary_key}), "

        # Check if primary key already defined
        if self.has_primary_key(engine_name=engine_name, schema_name=schema_name, table_name=table_name):
            print(f"Table {schema_name}.{table_name} already has a primary key defined.")
            return

        # Loop over the remaining keys
        for column_name in keys_json:
            if column_name in column_names:
                sql_query += f"ADD {keys_json[column_name].replace('PRIMARY KEY', 'KEY')} {column_name} ({column_name}), "

        # Remove the trailing comma and space
        if sql_query.endswith(', '):
            sql_query = sql_query[:-2]

        # Execute the query
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query)

        # Display the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

    #----------------------------------------------#
    # Method: Materialise a view to the cache      #
    #----------------------------------------------#
    def materialise_view(self, source_schema, source_view, target_schema, target_table, drop_table=False, use_replace=False, auto_increment_column=False, datatypes_json=False, keys_json=False, display_elapsed_time=False, estimated_num_rows=False, verbose=False):

        # Display processing time estimate
        if estimated_num_rows:

            # Display the current time
            print(f"Current time: {datetime.datetime.now().strftime('%H:%M')}")

            # Calculate the estimated processing time
            processing_time = processing_times_per_row['materialise_view'] * estimated_num_rows

            # Display the estimated processing time in # hours, # min and # sec format
            print(f"Estimated processing time: {int(processing_time/3600)} hour(s), {int((processing_time%3600)/60)} minute(s), {int(processing_time%60)} second(s)")

        # Initialize the timer
        start_time = time.time()

        # Drop the target table if it exists
        if drop_table:
            self.execute_query(engine_name='test', query=f"DROP TABLE IF EXISTS {target_schema}.{target_table}")

        # If use_replace, set the REPLACE statement
        insert_or_replace_statement = 'REPLACE' if use_replace else 'INSERT'
        
        # Create the target table
        self.execute_query(engine_name='test', query=f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} AS SELECT * FROM {source_schema}.{source_view} WHERE 1=0")

        # Set auto increment column
        if auto_increment_column:
            self.execute_query(engine_name='test', query=f"ALTER TABLE {target_schema}.{target_table} MODIFY COLUMN row_id INT AUTO_INCREMENT UNIQUE KEY")

        # Populate the target table
        self.execute_query_in_shell(engine_name='test', query=f"{insert_or_replace_statement} INTO {target_schema}.{target_table} SELECT * FROM {source_schema}.{source_view}")

        # Print the elapsed time
        if display_elapsed_time:
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

        # Apply datatypes
        if datatypes_json:
            if verbose:
                sysmsg.info(f"Applying datatypes to {target_schema}.{target_table} ...")
            self.apply_datatypes(engine_name='test', schema_name=target_schema, table_name=target_table, datatypes_json=datatypes_json, display_elapsed_time=display_elapsed_time, estimated_num_rows=estimated_num_rows)

        # Create keys JSON
        if keys_json:
            if verbose:
                sysmsg.info(f"Applying keys to {target_schema}.{target_table} ...")
            self.apply_keys(engine_name='test', schema_name=target_schema, table_name=target_table, keys_json=keys_json, display_elapsed_time=display_elapsed_time, estimated_num_rows=estimated_num_rows)

    #----------------------------------------------#
    # Method: Materialise a view to the cache      #
    #----------------------------------------------#
    def update_table_from_view(self, engine_name, source_schema, source_view, target_schema, target_table, verbose=False):

        # Fetch list of columns in the source view
        source_columns = self.get_column_names(engine_name='test', schema_name=source_schema, table_name=source_view)
        
        # Generate the SQL query
        SQLQuery = f"REPLACE INTO {target_schema}.{target_table} ({', '.join(source_columns)}) SELECT * FROM {source_schema}.{source_view};"

        # Print status and the SQL query if verbose
        if verbose:
            sysmsg.info(f"Updating table '{target_table}' from view '{source_view}' ...")

        # Execute the query
        self.execute_query_in_shell(engine_name='test', query=f"REPLACE INTO {target_schema}.{target_table} ({', '.join(source_columns)}) SELECT * FROM {source_schema}.{source_view}")

        # Print status
        if verbose:
            sysmsg.success(f"Table '{target_table}' updated from view '{source_view}'.")

    #------------------------------#
    # Method: Dump table to folder #
    #------------------------------#
    def dump_table_to_folder(self, engine_name, schema_name, table_name, folder_path, filter_by='TRUE', chunk_size=100000):

        # Create the folder if it does not exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Get minimum row_id
        min_row_id = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MIN(row_id),0) FROM {schema_name}.{table_name} WHERE {filter_by}")[0][0]
        
        # Get maximum row_id
        max_row_id = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MAX(row_id),0) FROM {schema_name}.{table_name} WHERE {filter_by}")[0][0]

        # Convert values to integers
        min_row_id = int(min_row_id)
        max_row_id = int(max_row_id)

        # Check if there are any rows to process
        if min_row_id >= max_row_id:
            sysmsg.warning(f"No rows found in table {schema_name}.{table_name} with filter '{filter_by}'.")
            return

        # Process table in chunks (from min to max row_id)
        for offset in tqdm(range(min_row_id-1, max_row_id+1, chunk_size)):

            # Generate shell command to dump table chunck using mysqldump executable
            shell_command = self.base_command_mysqldump[engine_name] + [schema_name, table_name, f'--where="{filter_by} AND (row_id BETWEEN {offset} AND {offset + chunk_size - 1})"'] + ['--result-file=' + f'{folder_path}/{table_name}_{str(offset).zfill(10)}.sql']

            # Generate shell text command
            shell_text_command = ' '.join(shell_command)

            # Run the command and capture stdout and stderr
            result = subprocess.run(shell_text_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

            # Check if there's a MySQL-specific warning
            if result.stderr:
                if result.stderr.strip() == 'mysql: [Warning] Using a password on the command line interface can be insecure.':
                    # Suppress the warning by doing nothing
                    pass
                else:
                    # Print the stderr output if it's not the specific MySQL warning
                    if 'ERROR' in result.stderr:
                        print('Error dumping table:', table_name)
                        print(result.stderr)
                        exit()

    #----------------------------------#
    # Method: Import table from folder #
    #----------------------------------#
    def import_table_from_folder(self, engine_name, schema_name, folder_path):

        # Get list of files from the input folder
        list_of_sql_files = sorted(glob.glob(f'{folder_path}/*.sql'))

        # Loop over the files
        for sql_file in tqdm(list_of_sql_files):

            # Define the command components, including the schema name
            shell_command = self.base_command_mysql[engine_name] + [schema_name]

            # Open the SQL file and pass it to the command via stdin
            with open(sql_file, 'rb') as fid:
                process = subprocess.Popen(shell_command, stdin=fid, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()

            # Print the output and any errors
            # print(stdout.decode())
            # print(stderr.decode())

    #-----------------------------------#
    # Method: Copy table across engines #
    #-----------------------------------#
    def copy_table_across_engines(self, source_engine_name, source_schema_name, source_table_name, target_engine_name, target_schema_name, keys_json, filter_by='TRUE', chunk_size=100000, drop_table=False):

        # Display status
        sysmsg.info(f"Copying table {source_schema_name}.{source_table_name} from '{source_engine_name}' to {target_schema_name}.{source_table_name} in '{target_engine_name}' ...")
        play_system_sound('info', 'soft')

        # Check if the target database exists
        if not self.database_exists(engine_name=target_engine_name, schema_name=target_schema_name):
            sysmsg.warning(f"Database '{target_schema_name}' does not exist in '{target_engine_name}'. Returning without copying the table.")
            return False
        
        # Check if the target table exists
        if self.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=source_table_name):
            sysmsg.warning(f"Table {source_table_name} already exists in '{target_schema_name}' on '{target_engine_name}'.")
            if not drop_table:
                sysmsg.info("'drop_table' is set to FALSE. Returning without copying the table.")
                return False

        # Get current date in YYYY-MM-DD format
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Generate random MD5 hash
        md5_hash = hashlib.md5(str(random.random()).encode()).hexdigest()[:8]

        # Generate the full folder path
        config_mysql_export_path = global_config['mysql']['data_path']['export']
        if os.path.isabs(config_mysql_export_path):
            folder_path = os.path.join(             config_mysql_export_path, current_date, md5_hash, source_table_name)
        else:
            folder_path = os.path.join(package_dir, config_mysql_export_path, current_date, md5_hash, source_table_name)

        # Display status
        sysmsg.info(f"{'Creating' if not drop_table else 'Recreating'} table in target engine '{target_engine_name}'.")

        # Create the target schema
        self.copy_create_table(
            source_engine_name  = source_engine_name,
            source_schema_name  = source_schema_name,
            source_table_name   = source_table_name,
            target_engine_name  = target_engine_name,
            target_schema_name  = target_schema_name,
            target_table_name   = source_table_name,
            drop_table          = drop_table,
            drop_keys           = True
        )
        
        # Display status
        sysmsg.info(f"Dumping table from '{source_engine_name}' to folder '$/{current_date}/{md5_hash}/{source_table_name}'.")

        # Dump the table to the folder
        self.dump_table_to_folder(
            engine_name = source_engine_name,
            schema_name = source_schema_name,
            table_name  = source_table_name,
            folder_path = folder_path,
            chunk_size  = chunk_size,
            filter_by   = filter_by
        )

        # Display status
        sysmsg.info(f"Importing table from folder '$/{current_date}/{md5_hash}/{source_table_name}' into '{target_engine_name}'.")

        # Import the table from the folder
        self.import_table_from_folder(
            engine_name = target_engine_name,
            schema_name = target_schema_name,
            folder_path = folder_path
        )

        # Display status
        sysmsg.info(f"Applying keys to table {target_schema_name}.{source_table_name} in '{target_engine_name}'.")

        # Apply keys in the target table
        self.apply_keys(
            engine_name = target_engine_name,
            schema_name = target_schema_name,
            table_name  = source_table_name,
            keys_json   = keys_json
        )
         
        # Display status
        sysmsg.success(f"Table has been successfully copied from '{source_engine_name}' to '{target_engine_name}'.")
        play_system_sound('success', 'soft')

    #--------------------------------------#
    # Method: Copy database across engines #
    #--------------------------------------#
    def copy_database_across_engines(self, source_engine_name, source_schema_name, target_engine_name, target_schema_name, chunk_size=100000, list_of_tables=[], drop_tables=False):

        # Play sound
        play_system_sound('info', 'moderate')

        # Get list of tables in graphsearch_test
        if len(list_of_tables) == 0:
            list_of_tables = self.get_tables_in_schema(engine_name=source_engine_name, schema_name=source_schema_name)

        # Loop over the tables
        for table_name in list_of_tables:

            # Get keys json
            if get_table_type_from_name(table_name) in table_keys_json:
                table_type = get_table_type_from_name(table_name)
                sysmsg.info(f"Detected table type '{table_type}' for '{table_name}'.")
                keys_json = table_keys_json[table_type]
                keys_json.update(table_keys_json['index_vars'])
            else:
                sysmsg.error(f"Table type not found for '{table_name}'.")
                exit()

            # Copy the table from test to prod
            self.copy_table_across_engines(
                source_engine_name = source_engine_name,
                source_schema_name = source_schema_name,
                source_table_name  = table_name,
                target_engine_name = target_engine_name,
                target_schema_name = target_schema_name,
                keys_json          = keys_json,
                chunk_size         = chunk_size,
                drop_table         = drop_tables
            )

        # Play sound
        play_system_sound('success', 'strong')

    #------------------------------------------#
    # Method: Convert JSON list to SQL INSERTS #
    #------------------------------------------#
    def json_file_to_sql_file(self, json_file_path, sql_file_path, schema_name, table_name, include_file_id=False):

        # Get file id from the file path
        file_id = os.path.basename(json_file_path).split('.')[0]

        # Get the JSON list from file
        with open(json_file_path, 'r') as file:
            json_list = json.load(file)

        # Check if the JSON list is empty
        if not json_list:
            return False

        # Get the column names from json keys
        column_names = list(json_list[0].keys())

        # Generate column names string
        column_names_str = ', '.join(['file_id'] + column_names)

        # Initialize the SQL INSERTS
        sql_inserts = f"INSERT INTO {schema_name}.{table_name} ({column_names_str}) VALUES "

        # Loop over the JSON list
        for row in json_list:

            # Generate values string
            values_str = ', '.join([file_id] + [f'"{row[column]}"' for column in column_names])

            # Append the SQL INSERT statement
            sql_inserts += f"({values_str}),"

        # Replace the trailing comma with a semicolon
        if sql_inserts.endswith(','):
            sql_inserts = sql_inserts[:-1] + ';'

        # Write the SQL INSERTS to file
        with open(sql_file_path, 'w') as file:
            file.write(sql_inserts)

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def get_random_primary_key_set(self, engine_name, schema_name, table_name, sample_size=100, partition_by=None, use_row_id=False):
            
        # Get the primary keys
        primary_keys = self.get_primary_keys(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Using row_id?
        # Yes.
        if use_row_id:

            # Get maximum row_id -> FIX: add min row_id
            # print(f"SELECT MAX(row_id) FROM {schema_name}.{table_name}")
            max_row_id = self.execute_query(engine_name=engine_name, query=f"SELECT MAX(row_id) FROM {schema_name}.{table_name}")[0][0]

            # Generate random row_id set
            random_primary_key_set = sorted([random.randint(1, max_row_id) for _ in range(sample_size)])

            # Fetch respective primary keys set
            random_primary_key_set = self.execute_query(engine_name=engine_name, query=f"SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} WHERE row_id IN ({', '.join([str(r) for r in random_primary_key_set])});")

        # No.
        else:

            # Generate the SQL query for sample tuples
            sql_query = f"SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} ORDER BY RAND() LIMIT {sample_size};"

            # Generate the SQL query for sample tuples with partitioning
            if partition_by in primary_keys:

                # Fetch all object types
                partition_column_possible_vals = [r[0] for r in self.execute_query(engine_name=engine_name, query=f"SELECT DISTINCT {partition_by} FROM {schema_name}.{table_name};")]

                # Loop over the object types
                sql_query_stack = []
                for colval in partition_column_possible_vals:
                    sql_query_stack += [
                        f"(SELECT {', '.join(primary_keys)} FROM {schema_name}.{table_name} WHERE object_type = '{colval}' ORDER BY RAND() LIMIT {round(sample_size/len(object_types))})",
                    ]
                sql_query = ' UNION ALL '.join(sql_query_stack)

            # Execute the query
            random_primary_key_set = self.execute_query(engine_name=engine_name, query=sql_query)

        # Return the random sample tuples
        return random_primary_key_set

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def get_rows_by_primary_key_set(self, engine_name, schema_name, table_name, primary_key_set, return_as_dict=False):

        # Get the primary keys
        primary_keys = self.get_primary_keys(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Get the column names
        return_columns = self.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Remove row_id from the return columns
        if 'row_id' in return_columns:
            return_columns.remove('row_id')

        # Generate the SQL query for sample tuples
        sql_query = f"SELECT {', '.join(return_columns)} FROM {schema_name}.{table_name} WHERE ({', '.join(primary_keys)}) IN ({', '.join([str(r) for r in primary_key_set])});"

        # Execute the query
        row_set = self.execute_query(engine_name=engine_name, query=sql_query)

        # Return as list of tuples
        if not return_as_dict:
            return row_set

        # Remove the primary keys from the return columns
        return_columns = [c for c in return_columns if c not in primary_keys]

        # Convert to dictionary in format {primary_key: {column_name: value}}
        row_set_dict = {tuple(r[0:len(primary_keys)]): dict(zip(return_columns, r[len(primary_keys):])) for r in row_set}

        # Execute the query
        return row_set_dict

    #-----------------------------------------------#
    # Method: Compare two tables by random sampling #
    #-----------------------------------------------#
    def compare_tables_by_random_sampling(self, source_engine_name, source_schema_name, source_table_name, target_engine_name, target_schema_name, target_table_name, sample_size=1024):

        # Check if the source table exists
        # print(source_engine_name, source_schema_name, target_table_name)
        # print( self.table_exists(engine_name=source_engine_name, schema_name=source_schema_name, table_name=target_table_name))
        if not self.table_exists(engine_name=source_engine_name, schema_name=source_schema_name, table_name=target_table_name):
            sysmsg.error(f"🚨 Table {source_schema_name}.{target_table_name} does not exist in '{source_engine_name}'.")
            return
        
        # Check if the target table exists
        # print(target_engine_name, target_schema_name, source_table_name)
        # print( self.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=source_table_name))
        if not self.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=source_table_name):
            sysmsg.error(f"🚨 Table {target_schema_name}.{source_table_name} does not exist in '{target_engine_name}'.")
            return

        # MYSQL_TABLE_COMPARE_PATH

        # Detect table type
        table_type = get_table_type_from_name(source_table_name)
        if table_type == 'doc_profile':
            # return
            pass

        # print(target_table_name)
        # if target_table_name != 'Index_D_Course':
        #     return

        #------------------------------------------#
        # Generate the SQL query for sample tuples #
        #------------------------------------------#
      
        # Get random primary key set
        random_primary_key_set  = self.get_random_primary_key_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, sample_size=round(sample_size/2), partition_by='object_type', use_row_id=True)
        random_primary_key_set += self.get_random_primary_key_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, sample_size=round(sample_size/2), partition_by='object_type', use_row_id=True)

        # print(source_table_name)
        # for r in random_primary_key_set:
        #     print(r)
        # print('')

        # Get the rows by primary key set (for source and target)
        source_row_set_dict = self.get_rows_by_primary_key_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, primary_key_set=random_primary_key_set, return_as_dict=True)
        target_row_set_dict = self.get_rows_by_primary_key_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, primary_key_set=random_primary_key_set, return_as_dict=True)

        # for t in source_row_set_dict:
        #     print(t)
        #     rich.print_json(data=source_row_set_dict[t])
        # print('')

        # Get unique set of tuples
        unique_tuples  = list(set(source_row_set_dict.keys()).union(set(target_row_set_dict.keys())))

        # Update the sample size
        sample_size = len(unique_tuples)

        # Initialise stats dictionary
        stats = {
            'new_rows': 0,
            'deleted_rows': 0,
            'existing_rows': 0,
            'mismatch': 0,
            'custom_column_mismatch': 0,
            'match': 0,
            'set_to_null': 0,
            'percent_new_rows': 0,
            'percent_deleted_rows': 0,
            'percent_existing_rows': 0,
            'percent_mismatch': 0,
            'percent_custom_column_mismatch': 0,
            'percent_match': 0,
            'percent_set_to_null': 0,
            'mismatch_by_column': {}
        }

        # print('\n')

        # Initialise stacks
        mismatch_changes_stack = []

        # Initialise score and rank differences
        score_rank_diffs = {
            'semantic_score': [],
            'degree_score': [],
            'row_rank': []
        }

        #----------------------------#
        # Analyse comparison results #
        #----------------------------#

        # Initialise test results
        test_results = {
            'flawless_match_test' : False,
            'deleted_rows_test' : True,
            'column_missing_or_renamed_test' : True,
            'custom_column_mismatch_test' : True,
            'set_to_null_test' : True,
            'median_score_diff_test' : True,
            'warning_flag' : False
        }

        # Initialise column missing or renamed list
        column_missing_or_renamed_list = []

        # Loop over the unique tuples
        for t in unique_tuples:

            # Check if the tuple is new
            if t in source_row_set_dict and t not in target_row_set_dict:
                stats['new_rows'] += 1
            
            # Check if the tuple is deleted
            elif t not in source_row_set_dict and t in target_row_set_dict:
                stats['deleted_rows'] += 1

            # Check if the tuple is in both source and target (existing row)
            if t in source_row_set_dict and t in target_row_set_dict:
                    
                # Add to existing rows
                stats['existing_rows'] += 1
                
                # Check if the values fully match
                if source_row_set_dict[t] == target_row_set_dict[t]:
                    stats['match'] += 1

                # Else, analyse the differences
                else:

                    # Initialise flags
                    exact_row_mismatch_detected = False
                    custom_column_mismatch_detected = False
                    set_to_null_detected = False

                    # Loop over non-primary key columns
                    for k in source_row_set_dict[t]:

                        # Check if the key is in both source and target
                        if k not in source_row_set_dict[t] or k not in target_row_set_dict[t]:

                            # Add column existance mismatch to list
                            column_missing_or_renamed_list += [k]
                            column_missing_or_renamed_list = sorted(list(set(column_missing_or_renamed_list)))

                        # Else, analyse values in matching columns
                        else: 

                            # Check if column exists in stats dictionary
                            if k not in stats['mismatch_by_column']:
                                stats['mismatch_by_column'][k] = 0

                            # Check if the values are different in matching columns
                            if source_row_set_dict[t][k] != target_row_set_dict[t][k]:

                                # Flag mismatch detected
                                exact_row_mismatch_detected = True

                                # Increment the mismatch counter
                                stats['mismatch_by_column'][k] += 1

                                # Check if custom column mismatch detected
                                if k not in ['row_rank', 'row_score', 'semantic_score', 'degree_score', 'object_created', 'object_updated']:

                                    # Flag custom column mismatch detected
                                    custom_column_mismatch_detected = True

                                    # Append the mismatch changes stack
                                    mismatch_changes_stack += [(f'{k}: [S] {source_row_set_dict[t][k]} ... [T] {target_row_set_dict[t][k]}')]
                                    
                                # Check if the value is set to NULL from source to target
                                if source_row_set_dict[t][k] is None:
                                    set_to_null_detected = True

                            # Append score and rank differences to list
                            if k in score_rank_diffs:
                                score_rank_diffs[k] += [source_row_set_dict[t][k] - target_row_set_dict[t][k]]

                    # Increment the mismatch counters based on flags
                    if exact_row_mismatch_detected:
                        stats['mismatch'] += 1
                    if custom_column_mismatch_detected:
                        stats['custom_column_mismatch'] += 1
                    if set_to_null_detected:
                        stats['set_to_null'] += 1

        # Calculate the percentages
        try:
            stats['percent_existing_rows'] = stats['existing_rows'] / sample_size * 100
            stats['percent_new_rows']      = stats['new_rows'     ] / sample_size * 100
            stats['percent_deleted_rows']  = stats['deleted_rows' ] / sample_size * 100
            
            if stats['existing_rows'] > 0:
                stats['percent_mismatch']      = stats['mismatch'     ] / stats['existing_rows'] * 100
                stats['percent_match']         = stats['match'        ] / stats['existing_rows'] * 100
                # stats['percent_set_to_null']   = stats['set_to_null'  ] / stats['existing_rows'] * 100
            else:
                stats['percent_mismatch']    = 0
                stats['percent_match']       = 0
                # stats['percent_set_to_null'] = 0
            
            if stats['mismatch'] > 0:
                stats['percent_custom_column_mismatch'] = stats['custom_column_mismatch'] / stats['mismatch'] * 100
                stats['percent_set_to_null'] = stats['set_to_null'] / stats['mismatch'] * 100
            else:
                stats['percent_custom_column_mismatch'] = 0
                stats['percent_set_to_null'] = 0
        except ZeroDivisionError:
            print('ZeroDivisionError')
            print('sample_size:', sample_size)
            print('stats dict:')
            rich.print_json(data=stats)
            exit()

        # print("\033[31mThis is red text\033[0m")
        # print("\033[32mThis is green text\033[0m")
        # print("\033[34mThis is blue text\033[0m")
        # print("\033[33mThis is yellow text\033[0m")
        # print("\033[35mThis is purple text\033[0m")
        # print("\033[36mThis is cyan text\033[0m")
        # print("\033[37mThis is white text\033[0m")
        # print("\033[1;31mThis is bold red text\033[0m")


        # Flawless match test
        if stats['percent_match'] == 100:
            test_results['flawless_match_test'] = True
            print(f"🚀 \033[32mFlawless match test passed for {target_table_name}.\033[0m")
            return

        # Generate print colours
        if stats['percent_deleted_rows'] >= 25:
            percent_deleted_rows_colour = '\033[31m'
            test_results['deleted_rows_test'] = False
        elif stats['percent_deleted_rows'] >= 10:
            percent_deleted_rows_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_deleted_rows_colour = '\033[37m'

        if stats['percent_mismatch'] >= 10:
            percent_mismatch_colour = '\033[33m'
        elif stats['percent_mismatch'] >= 5:
            percent_mismatch_colour = '\033[33m'
        else:
            percent_mismatch_colour = '\033[37m'

        if stats['percent_custom_column_mismatch'] >= 10:
            percent_custom_column_mismatch_colour = '\033[31m'
            test_results['custom_column_mismatch_test'] = False
        elif stats['percent_custom_column_mismatch'] >= 5:
            percent_custom_column_mismatch_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_custom_column_mismatch_colour = '\033[37m'

        if stats['percent_set_to_null'] >= 10:
            percent_set_to_null_colour = '\033[31m'
            test_results['set_to_null_test'] = False
        elif stats['percent_set_to_null'] >= 5:
            percent_set_to_null_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_set_to_null_colour = '\033[37m'
        
        # Print the stats
        print('')
        print('==============================================================================================')
        print('')
        print(f"Results for \033[36m{target_table_name}:\033[0m")
        print('')
        print(f" - Sample size ....... {sample_size}")
        print(f" - Existing rows ..... {stats['existing_rows']} {' '*(8-len(str(stats['existing_rows'])))} {stats['percent_existing_rows']:.1f}%")
        print(f" - New rows .......... {stats['new_rows']     } {' '*(8-len(str(stats['new_rows'])))     } {stats['percent_new_rows'     ]:.1f}%")
        print(f"{percent_deleted_rows_colour} - Deleted rows ...... {stats['deleted_rows'] } {' '*(8-len(str(stats['deleted_rows']))) } {stats['percent_deleted_rows' ]:.1f}% \033[0m")
        print('')
        print(f" - Match ............. {stats['match']        } {' '*(8-len(str(stats['match'])))        } {stats['percent_match'        ]:.1f}%")
        print(f"{percent_mismatch_colour} - Mismatch .......... {stats['mismatch']     } {' '*(8-len(str(stats['mismatch'])))     } {stats['percent_mismatch'     ]:.1f}% \033[0m")
        print(f"{percent_custom_column_mismatch_colour} - (custom columns) .. {stats['custom_column_mismatch']  } {' '*(8-len(str(stats['custom_column_mismatch']))  )} {stats['percent_custom_column_mismatch'  ]:.1f}% \033[0m")
        print(f"{percent_set_to_null_colour} - Set to NULL ....... {stats['set_to_null']  } {' '*(8-len(str(stats['set_to_null'])))  } {stats['percent_set_to_null'  ]:.1f}% \033[0m")
        print('')
        if len(stats['mismatch_by_column']) > 0:
            print('Mismatch(s) by column:')
            for column in stats['mismatch_by_column']:
                if stats['mismatch_by_column'][column] == 0:
                    print(f"\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}")
                else:
                    if column in ['row_rank', 'row_score', 'semantic_score', 'degree_score', 'object_created', 'object_updated']:
                        print(f"\033[33m\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}\033[0m")
                    else:
                        print(f"\033[31m\t- {column} {'.'*(64-len(column))} {stats['mismatch_by_column'][column]}\033[0m")
            print('')
        
        # Print score and rank average differences
        if len(score_rank_diffs['semantic_score'])>0 or len(score_rank_diffs['degree_score'])>0 or len(score_rank_diffs['row_rank'])>0:
            print('Median score and rank differences:')
            for k in score_rank_diffs:
                if score_rank_diffs[k]:
                    # avg_val = sum(score_rank_diffs[k])/len(score_rank_diffs[k])
                    med_val = np.median(score_rank_diffs[k])
                    if   k in ['semantic_score', 'degree_score'] and abs(med_val)>=0.2:
                        test_results['median_score_diff_test'] = False
                        print(f"\033[31m\t- {k}: {med_val:.2f}\033[0m")
                    elif k in ['semantic_score', 'degree_score'] and abs(med_val)>=0.1:
                        test_results['warning_flag'] = True
                        print(f"\033[33m\t- {k}: {med_val:.2f}\033[0m")
                    else:
                        print(f"\t- {k}: {med_val:.2f}")
            print('')

        if len(column_missing_or_renamed_list) > 0:
            test_results['column_missing_or_renamed_test'] = False
            print(f"\033[31mColumn mismatch(s) detected:\033[0m {column_missing_or_renamed_list}")
            print('')

        # Print the first 3 mismatch changes
        if len(mismatch_changes_stack) > 0:
            mismatch_changes_stack = list(set(mismatch_changes_stack))
            # randomize
            mismatch_changes_stack = random.sample(mismatch_changes_stack, len(mismatch_changes_stack))
            print('Example mismatch changes:')
            for n,r in enumerate(mismatch_changes_stack):
                print('\t-', r)
                if n==32:
                    break
            print('')

        #----------------------------------------------------#
        # Calculate conditions for passing the test (or not) #
        #----------------------------------------------------#

        print('')
        if test_results['deleted_rows_test'] and test_results['column_missing_or_renamed_test'] and test_results['custom_column_mismatch_test'] and test_results['set_to_null_test'] and test_results['median_score_diff_test']:
            if test_results['warning_flag']:
                print("Test result: \033[33mMinor changes detected.\033[0m")
            else:
                print("Test result: \033[32mNo significant changes detected.\033[0m")
        else:
            print("Test result: \033[31mMajor changes detected!\033[0m")
        print('')

        time.sleep(1)

    #----------------------------------#
    # Method:
    #----------------------------------#
    def compare_databases():



        pass

#----------------------------------#
# Class definition: Graph Registry #
#----------------------------------#
class GraphRegistry():
    
    # Constructor
    def __init__(self):
        self.db = GraphDB()
        self.orchestrator = self.Orchestration()
        self.cachemanager = self.CacheManagement()
        self.indexdb = self.IndexDB()
        self.indexes = self.IndexES()

    # Help function
    def help(self):
        instructions = """

        gr = GraphRegistry()

        # Config object types to process
        gr.orchestrator.config(
            node_types = [('EPFL', 'Widget')],
            edge_types = [('EPFL', 'Widget', 'EPFL', 'Lecture' ),
                          ('EPFL', 'Widget', 'EPFL', 'Widget'  ),
                          ('EPFL', 'Widget', 'Ont' , 'Category'),
                          ('EPFL', 'Widget', 'Ont' , 'Concept' )],
            sync  = False,
            reset = False,
            print = True
        )

        gr.orchestrator.propagate()

        gr.cachemanager.eval()

        gr.cachemanager.apply_views()
        gr.cachemanager.apply_formulas()

        gr.cachemanager.calculate_scores_matrix(  from_object_type='Widget', to_object_type='Widget')
        gr.cachemanager.consolidate_scores_matrix(from_object_type='Widget', to_object_type='Widget')

        gr.indexdb.cachebuilder.build_all(actions=('eval', 'commit'))

        pp = gr.indexdb.PageProfile()
        pp.patch(actions=('eval'))
        pp.patch(actions=('commit'))

        idoc = gr.indexdb.IndexDocs(doc_type='Widget')
        idoc.create_table(actions=('print'))
        idoc.patch(actions=('eval'))
        idoc.patch(actions=('commit'))

        doc_type     = 'Widget'
        link_type    = 'Category'
        link_subtype = 'SEM'

        idoclink = gr.indexdb.IndexDocLinks(doc_type, link_type, link_subtype)
        idoclink.set_engine('prod')
        idoclink.create_table(actions=('commit'))
        idoclink.vertical_patch(actions=('eval'))
        idoclink.vertical_patch(actions=('commit'))
        idoclink.horizontal_patch(actions=('print'))
        idoclink.horizontal_patch(actions=('eval'))
        idoclink.horizontal_patch(actions=('commit'))


        """
        print(instructions)
        pass

    #--------------------------------------------------#
    # Subclass definition: GraphRegistry Orchestration #
    #--------------------------------------------------#
    class Orchestration():

        # Class constructor
        def __init__(self):
            self.db = GraphDB()
            self.typeflags = self.TypeFlags()
            self.fieldschanged = self.FieldsChanged()
            self.scoresexpired = self.ScoresExpired()

        # Print current settings (fields and scores)
        def status(self):
            self.typeflags.status()
            self.fieldschanged.status()
            self.scoresexpired.status()
        
        # Reset all flags on cache
        def reset_all_cache_flags(self):

            # Print status
            sysmsg.info("🧹 📝 Clean all cache 'to_process' flags.")

            # Build tables list for updates of type:
            # UPDATE schema.table SET to_process = 0 WHERE to_process = 1
            list_of_tables = [
                ('graph_cache', 'Data_N_Object_T_PageProfile'),
                ('graph_cache', 'Edges_N_Object_N_Object_T_ParentChildSymmetric'),
                ('graph_cache', 'Edges_N_Object_N_Object_T_ScoresMatrix_AS'),
                ('graph_cache', 'Edges_N_Object_N_Object_T_ScoresMatrix_GBC'),
                ('graph_cache', 'Nodes_N_Object_T_DegreeScores')
            ]

            # Print status
            sysmsg.trace("Processing 'graph_cache' fields and scores tables ...")

            # Loop over 'graph_cache' tables
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name = 'test', 
                        query = f"UPDATE {schema_name}.{table_name} SET to_process = 0 WHERE to_process = 1;")

            # Print status
            sysmsg.trace("Processing 'graph_cache' IndexBuildup Doc tables ...")

            # Reset flags on index buildup tables
            with tqdm(index_doc_types_list, unit='doc type') as pb:
                for dummy, doc_type in pb:
                    pb.set_description(f"⚙️  Doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name='test',
                        query = f"UPDATE graph_cache.IndexBuildup_Fields_Docs_{doc_type} SET to_process = 0 WHERE to_process = 1;")

            # Print status
            sysmsg.trace("Processing 'graph_cache' IndexBuildup Doc-Link tables ...")

            # Reset to_process flags - TODO: get list from config file / also turn config file into object
            with tqdm([('Course', 'Person'), ('Person', 'Unit')], unit='doc-link type') as pb:
                for source_doc_type, target_doc_type in pb:
                    pb.set_description(f"⚙️  Doc-link type: {source_doc_type}-{target_doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name='test',
                        query = f"UPDATE graph_cache.IndexBuildup_Fields_Links_ParentChild_{source_doc_type}_{target_doc_type} SET to_process = 0 WHERE to_process = 1;")

            # # Print status
            # sysmsg.trace("Processing 'Operations_N_Object_T_ToProcess' table ...")

            # # Truncate table: objects to process
            # self.db.execute_query_in_shell(engine_name='test', query=f"TRUNCATE TABLE graph_cache.Operations_N_Object_T_ToProcess;")

            # Print status
            sysmsg.success("🧹 ✅ All cache 'to_process' flags have been reset to 0.\n")

        # Propagate flags to cache tables
        def propagate_object_flags_to_cache(self):

            # Print status
            sysmsg.info("⛳️ 📝 Propagate 'to_process' flags throughout the cache.")

            # Build list for updates in nodes and data tables
            list_of_tables = [
                ('graph_cache', 'Data_N_Object_T_PageProfile'),
                ('graph_cache', 'Nodes_N_Object_T_DegreeScores')
            ]

            # Print status
            sysmsg.trace("Processing 'graph_cache' page profile and degree scores tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name = 'test', 
                        query = f"""UPDATE {schema_name}.{table_name} p
                                INNER JOIN graph_airflow.Operations_N_Object_T_FieldsChanged fc
                                     USING (institution_id, object_type, object_id)
                                INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
                                     USING (institution_id, object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process > 0.5
                                       AND tf.to_process > 0.5
                                       AND  p.to_process < 0.5;
                        """)
                
            # Build list for updates in edge tables
            list_of_tables = [
                ('graph_cache', 'Edges_N_Object_N_Object_T_ParentChildSymmetric')
            ]

            # Print status
            sysmsg.trace("Processing 'graph_cache' parent-child tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    for d1,d2 in [('from', 'to'), ('to', 'from')]:
                        self.db.execute_query_in_shell(engine_name = 'test', 
                            query = f"""UPDATE {schema_name}.{table_name} p
                                    INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged AS fc
                                            ON ( p.{d1}_institution_id,  p.{d1}_object_type,  p.{d1}_object_id, p.{d2}_institution_id, p.{d2}_object_type, p.{d2}_object_id)
                                             = (fc.from_institution_id, fc.from_object_type, fc.from_object_id,  fc.to_institution_id,  fc.to_object_type,  fc.to_object_id)
                                    INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                            ON ( p.{d1}_institution_id,  p.{d1}_object_type, p.{d2}_institution_id, p.{d2}_object_type)
                                             = (tf.from_institution_id, tf.from_object_type,  tf.to_institution_id,  tf.to_object_type)
                                           SET  p.to_process = 1
                                         WHERE fc.to_process > 0.5
                                           AND tf.to_process > 0.5
                                           AND  p.to_process < 0.5;
                            """)
                    
           # Build list for updates in edge tables
            list_of_tables = [
                ('graph_cache', 'Edges_N_Object_N_Object_T_ScoresMatrix_AS'),
                ('graph_cache', 'Edges_N_Object_N_Object_T_ScoresMatrix_GBC')
            ]

            # Print status
            sysmsg.trace("Processing 'graph_cache' score matrix tables ...")

            # Loop over tables and propagate flags
            with tqdm(list_of_tables, unit='table') as pb:
                for schema_name, table_name in pb:
                    pb.set_description(f"⚙️  {table_name}".ljust(PBWIDTH)[:PBWIDTH])
                    for d in ['from', 'to']:
                        self.db.execute_query_in_shell(engine_name = 'test', 
                            query = f"""UPDATE {schema_name}.{table_name} p
                                    INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired AS se
                                            ON (p.{d}_institution_id, p.{d}_object_type, p.{d}_object_id) = (se.institution_id, se.object_type, se.object_id)
                                    INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                            ON ( p.from_institution_id,  p.from_object_type,  p.to_institution_id,  p.to_object_type)
                                             = (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type)
                                           SET  p.to_process = 1
                                         WHERE se.to_process > 0.5 
                                           AND tf.to_process > 0.5
                                           AND  p.to_process < 0.5;
                            """)
                    
            # Print status
            sysmsg.trace("Processing 'graph_cache' IndexBuildup Doc tables ...")

            # Propagate flags on index buildup tables
            with tqdm(index_doc_types_list, unit='doc type') as pb:
                for dummy, doc_type in pb:
                    pb.set_description(f"⚙️  Doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name = 'test', 
                        query = f"""UPDATE graph_cache.IndexBuildup_Fields_Docs_{doc_type} p
                                INNER JOIN graph_airflow.Operations_N_Object_T_FieldsChanged fc
                                        ON (p.doc_institution, p.doc_type, p.doc_id) = (fc.institution_id, fc.object_type, fc.object_id)
                                INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
                                     USING (institution_id, object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process > 0.5
                                       AND tf.to_process > 0.5
                                       AND  p.to_process < 0.5;
                        """)
                    
            # Print status
            sysmsg.trace("Processing 'graph_cache' IndexBuildup Doc-Link tables ...")

            # Propagate to_process flags - TODO: get list from config file / also turn config file into object
            with tqdm([('Course', 'Person'), ('Person', 'Unit')], unit='doc-link type') as pb:
                for source_doc_type, target_doc_type in pb:
                    pb.set_description(f"⚙️  Doc-link type: {source_doc_type}-{target_doc_type}".ljust(PBWIDTH)[:PBWIDTH])
                    self.db.execute_query_in_shell(engine_name='test',
                        query = f"""UPDATE graph_cache.IndexBuildup_Fields_Links_ParentChild_{source_doc_type}_{target_doc_type} p
                                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged AS fc
                                        ON ( p.doc_institution,  p.doc_type,  p.doc_id, p.link_institution, p.link_type, p.link_id)
                                         = (fc.from_institution_id, fc.from_object_type, fc.from_object_id,  fc.to_institution_id,  fc.to_object_type,  fc.to_object_id)
                                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags AS tf
                                        ON ( p.doc_institution,  p.doc_type, p.link_institution, p.link_type)
                                         = (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type)
                                       SET  p.to_process = 1
                                     WHERE fc.to_process > 0.5
                                       AND tf.to_process > 0.5
                                       AND  p.to_process < 0.5;
                        """)

            # # Truncate table: Operations/ Object / ToProcess
            # self.db.execute_query_in_shell(engine_name='test', query=f"TRUNCATE TABLE graph_cache.Operations_N_Object_T_ToProcess;")

            # Print status
            sysmsg.success("⛳️ ✅ All 'to_process' flags have been propagated throughout cache.\n")

        # Get fields for input object type or id (fields and scores)
        def get(self, object_key):
            self.typeflags.get(object_key, print_output=True)
            self.fieldschanged.get(object_key, print_output=True)
            self.scoresexpired.get(object_key, print_output=True)

        # Set fields for input object type or id (fields and scores)
        def set(self, object_key, flag_type=None, to_process=None):
            if flag_type == 'fields':
                self.fieldschanged.set(object_key, to_process=to_process)
            elif flag_type == 'scores':
                self.scoresexpired.set(object_key, to_process=to_process)
            else:
                self.fieldschanged.set(object_key, to_process=to_process)
                self.scoresexpired.set(object_key, to_process=to_process)

        # Sync new objects to operations table
        def sync(self, to_process=1):
            self.fieldschanged.sync(to_process=to_process)
            self.scoresexpired.sync(to_process=to_process)

        # Randomize airflow fields [OPTIONAL: For testing purposes]
        def randomize(self, time_period=182):
            self.fieldschanged.randomize(time_period=time_period)
            self.scoresexpired.randomize(time_period=time_period)

        # Set expiration dates
        def expire(self, time_period=90, limit=100):
            self.fieldschanged.expire(time_period=time_period, limit=limit)
            self.scoresexpired.expire(time_period=time_period, limit=limit)

        # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
        def refresh(self, refresh_checksums=False):
            self.fieldschanged.refresh(refresh_checksums=refresh_checksums)
            self.scoresexpired.refresh()

        # === Object Type Flags ===
        class TypeFlags():

            # Class constructor
            def __init__(self):
                self.db = GraphDB()

            # Help function
            def list_types(self):
                
                # Fetch and print object types
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT DISTINCT object_type
                    FROM graph_airflow.Operations_N_Object_T_TypeFlags
                    ORDER BY object_type;
                """)
                print('\nNode / Object / Document types:')
                for o in out:
                    print(f" • {o[0]}")
                
                # Fetch and print object to object types
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT DISTINCT from_object_type, to_object_type
                    FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                    ORDER BY from_object_type, to_object_type;
                """)
                print('\nEdge / Object-to-Object / Link types:')
                for o in out:
                    print(f" • {o[0]} -> {o[1]}")

            # Print object type flags
            def print_for_object(self):
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT institution_id, object_type, flag_type, to_process
                      FROM graph_airflow.Operations_N_Object_T_TypeFlags
                     WHERE to_process = 1
                  ORDER BY institution_id, object_type, flag_type;
                """)
                df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'flag_type', 'to_process'])
                if not df.empty:
                    print_dataframe(df, title='⛳️ TYPE FLAGS: Object')

            # Print object to object type flags
            def print_for_object_to_object(self):
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type, to_process
                      FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                     WHERE to_process = 1
                  ORDER BY from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type;
                """)
                df = pd.DataFrame(out, columns=['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type', 'flag_type', 'to_process'])
                if not df.empty:
                    print_dataframe(df, title='⛳️ TYPE FLAGS: Object-to-Object')

            # Print type flags (graph unit as input)
            def status(self, unit=None, types_only=False):
                
                if not types_only:
                    if unit == 'obj':
                        self.print_for_object()
                    elif unit == 'obj2obj':
                        self.print_for_object_to_object()
                        
                    elif unit == 'all' or unit is None:
                        self.print_for_object()
                        self.print_for_object_to_object()
                    else:
                        print('Please specify a valid graph unit: obj, obj2obj, all')

                output_json = {'nodes':[], 'edges':[]}
                if unit in ['obj', 'all', None]:
                    output_json['nodes'] = self.db.execute_query(engine_name='test', query=f"SELECT institution_id, object_type, flag_type FROM graph_airflow.Operations_N_Object_T_TypeFlags WHERE to_process > 0.5")
                if unit in ['obj2obj', 'all', None]:
                    output_json['edges'] = self.db.execute_query(engine_name='test', query=f"SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags WHERE to_process > 0.5")
                return output_json

            # Reset object type flags
            def reset_for_object(self):
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_T_TypeFlags
                    SET to_process = 0
                    WHERE to_process = 1;
                """)

            # Reset object to object type flags
            def reset_for_object_to_object(self):
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                    SET to_process = 0
                    WHERE to_process = 1;
                """)

            # Reset type flags (graph unit as input)
            def reset(self, unit=None):
                if unit == 'obj':
                    self.reset_for_object()
                elif unit == 'obj2obj':
                    self.reset_for_object_to_object()
                elif unit == 'all' or unit is None:
                    self.reset_for_object()
                    self.reset_for_object_to_object()
                else:
                    print('Please specify a valid graph unit: obj, obj2obj, all')

            # Set object type flag (1 key only)
            def set_for_object(self, object_type_key, flag_type=None, to_process=None):
                if to_process is None:
                    return
                if flag_type in ['fields', 'scores']:
                    self.db.execute_query_in_shell(engine_name='test', query=f"""
                        UPDATE graph_airflow.Operations_N_Object_T_TypeFlags
                        SET to_process = {to_process}
                        WHERE (institution_id, object_type, flag_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{flag_type}");
                    """)
                else:
                    self.db.execute_query_in_shell(engine_name='test', query=f"""
                        UPDATE graph_airflow.Operations_N_Object_T_TypeFlags
                        SET to_process = {to_process}
                        WHERE (institution_id, object_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}");
                    """)

            # Set object to object type flag (1 key only)
            def set_for_object_to_object(self, object_type_key, flag_type=None, to_process=None):
                if to_process is None:
                    return
                if flag_type in ['fields', 'scores']:
                    sql_query=f"""
                        UPDATE graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                        SET to_process = {to_process}
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{object_type_key[2]}", "{object_type_key[3]}", "{flag_type}");
                    """
                else:
                    sql_query=f"""
                        UPDATE graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                        SET to_process = {to_process}
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{object_type_key[2]}", "{object_type_key[3]}");
                    """ 
                print(sql_query)
                self.db.execute_query_in_shell(engine_name='test', query=sql_query)

            # Set type flags (1 key only)
            def set(self, object_type_key, flag_type=None, to_process=None):
                if to_process is None:
                    return
                if len(object_type_key) == 2:
                    self.set_for_object(object_type_key, flag_type, to_process)
                elif len(object_type_key) == 4:
                    self.set_for_object_to_object(object_type_key, flag_type, to_process)
                else:
                    msg = 'Invalid key length.'
                    print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)

            # Get object type flag (1 key only)
            def get_for_object(self, object_type_key, flag_type=False, print_output=False):
                if flag_type in ['fields', 'scores']:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT to_process
                        FROM graph_airflow.Operations_N_Object_T_TypeFlags
                        WHERE (institution_id, object_type, flag_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{flag_type}");
                    """)
                else:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT flag_type, to_process
                        FROM graph_airflow.Operations_N_Object_T_TypeFlags
                        WHERE (institution_id, object_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}");
                    """)
                if print_output:
                    print(object_type_key, '-->', out)
                return out

            # Get object to object type flag (1 key only)
            def get_for_object_to_object(self, object_type_key, flag_type=False, print_output=False):
                if flag_type in ['fields', 'scores']:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{object_type_key[2]}", "{object_type_key[3]}", "{flag_type}");
                    """)
                else:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT flag_type, to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                            = ("{object_type_key[0]}", "{object_type_key[1]}", "{object_type_key[2]}", "{object_type_key[3]}");
                    """)
                if print_output:
                    print(object_type_key, '-->', out)
                return out
            
            # Get type flags (1 key only)
            def get(self, object_type_key, flag_type=False, print_output=False):
                if len(object_type_key) == 2:
                    return self.get_for_object(object_type_key, flag_type, print_output)
                elif len(object_type_key) == 4:
                    return self.get_for_object_to_object(object_type_key, flag_type, print_output)
                else:
                    msg = 'Invalid key length.'
                    print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)

            # Quick configuration for input list of node and edge types
            def config(self, config_json, reset=False, verbose=False):
                
                # Reset airflow flags
                if reset:
                    self.reset()

                # Node types
                for d in config_json['nodes']:
                    node_type, process_fields, process_scores = d
                    institution_id = object_type_to_institution_id[node_type]
                    if process_fields:
                        self.set(object_type_key=(institution_id, node_type), flag_type='fields', to_process=1)
                    if process_scores:
                        self.set(object_type_key=(institution_id, node_type), flag_type='scores', to_process=1)

                # Edge types
                for d in config_json['edges']:
                    source_node_type, target_node_type, process_fields, process_scores = d
                    source_institution_id = object_type_to_institution_id[source_node_type]
                    target_institution_id = object_type_to_institution_id[target_node_type]
                    if process_fields:
                        self.set(object_type_key=(source_institution_id, source_node_type, target_institution_id, target_node_type), flag_type='fields', to_process=1)
                    if process_scores:
                        self.set(object_type_key=(source_institution_id, source_node_type, target_institution_id, target_node_type), flag_type='scores', to_process=1)

                # Print status
                if verbose:
                    self.status()
                print('Done.')

        # === Fields Changed Flags ===
        class FieldsChanged():

            # Class constructor
            def __init__(self):
                self.db = GraphDB()

            # Print current settings
            def status(self, object_key=None):
                if object_key is not None:

                    if len(object_key) == 2:
                        sql_query = f"""
                            SELECT institution_id, object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM graph_airflow.Operations_N_Object_T_FieldsChanged
                             WHERE (institution_id, object_type)
                                 = ("{object_key[0]}", "{object_key[1]}")
                        """
                        
                    elif len(object_key) == 3:
                        sql_query = f"""
                            SELECT institution_id, object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM graph_airflow.Operations_N_Object_T_FieldsChanged
                             WHERE (institution_id, object_type, object_id)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")
                        """
                        
                    elif len(object_key) == 4:
                        sql_query = f"""
                            SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                             WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}")
                        """

                    elif len(object_key) == 6:
                        sql_query = f"""
                            SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                              FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                             WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                                 = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}", "{object_key[4]}", "{object_key[5]}")
                        """

                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return
                    
                    out = self.db.execute_query(engine_name='test', query=sql_query)
                    df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object [by type or key]')

                else:

                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT institution_id, object_type, COUNT(*) AS n_to_process
                          FROM graph_airflow.Operations_N_Object_T_FieldsChanged
                         WHERE to_process = 1
                      GROUP BY institution_id, object_type
                    """)
                    df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object [stats]')

                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, COUNT(*) AS n_to_process
                          FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                         WHERE to_process = 1
                      GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type
                    """)
                    df = pd.DataFrame(out, columns=['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🪪  FIELDS CHANGED: Object-to-Object [stats]')

            # Reset current settings
            def reset(self):
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_T_FieldsChanged
                    SET to_process = 0
                    WHERE to_process = 1;
                """)
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                    SET to_process = 0
                    WHERE to_process = 1;
                """)

            # Quick configuration for input list of node and edge types
            def config(self, config_json, reset=False, verbose=False):
                
                # Reset airflow flags
                if reset:
                    self.reset()

                # Node types
                for d in config_json['nodes']:
                    node_type, process_fields, process_scores = d
                    institution_id = object_type_to_institution_id[node_type]
                    if process_fields:
                        self.set(object_key=(institution_id, node_type), to_process=1)

                # Edge types
                for d in config_json['edges']:
                    source_node_type, target_node_type, process_fields, process_scores = d
                    source_institution_id = object_type_to_institution_id[source_node_type]
                    target_institution_id = object_type_to_institution_id[target_node_type]
                    if process_fields:
                        self.set(object_key=(source_institution_id, source_node_type, target_institution_id, target_node_type), to_process=1)

                # Print status
                if verbose:
                    self.status()
                print('Done.')

            # Set fields for input object type or id
            def set(self, object_key, checksum_current=None, checksum_previous=None, has_changed=None, last_date_cached=None, has_expired=None, to_process=None):

                columns_to_update = []
                for column, value in zip(['checksum_current', 'checksum_previous', 'has_changed', 'last_date_cached', 'has_expired', 'to_process'], [checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process]):
                    if value is not None:
                        columns_to_update.append(f"{column} = '{value}'")

                if len(columns_to_update) > 0:
                    if len(object_key) == 2:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_T_FieldsChanged
                            SET {', '.join(columns_to_update)}
                            WHERE (institution_id, object_type) = ("{object_key[0]}", "{object_key[1]}");
                        """
                    elif len(object_key) == 3:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_T_FieldsChanged
                            SET {', '.join(columns_to_update)}
                            WHERE (institution_id, object_type, object_id) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}");
                        """
                    elif len(object_key) == 4:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                            SET {', '.join(columns_to_update)}
                            WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}");
                        """
                    elif len(object_key) == 6:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                            SET {', '.join(columns_to_update)}
                            WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}", "{object_key[4]}", "{object_key[5]}");
                        """
                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return                        

                    self.db.execute_query_in_shell(engine_name='test', query=sql_query)
                else:
                    print('No columns to update.')
                    return

            # Get fields for input object id
            def get(self, object_key, time_period=None, has_expired=None):
                
                # Generate time period condition
                if time_period is not None:
                    # Only rows where last_date_cached is newer than time_period (in days) with respect to current date
                    time_condition = f"AND last_date_cached >= NOW() - INTERVAL {time_period} DAY"
                else:
                    time_condition = ""

                # Generate has_expired condition
                if has_expired is not None:
                    # Only rows where has_expired is True
                    has_expired_condition = f"AND has_expired = {has_expired}"
                else:
                    has_expired_condition = ""                

                if len(object_key) == 2:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT institution_id, object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_T_FieldsChanged
                        WHERE (institution_id, object_type)
                            = ("{object_key[0]}", "{object_key[1]}")
                          {time_condition} {has_expired_condition}
                    """)
                elif len(object_key) == 3:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT institution_id, object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_T_FieldsChanged
                        WHERE (institution_id, object_type, object_id)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")
                          {time_condition} {has_expired_condition}
                    """)
                elif len(object_key) == 4:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}")
                          {time_condition} {has_expired_condition}
                    """)
                elif len(object_key) == 6:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                        WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}", "{object_key[4]}", "{object_key[5]}")
                          {time_condition} {has_expired_condition}
                    """)
                else:
                    msg = 'Invalid key length.'
                    print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                    return None

                return out
            
            # Sync new objects to operations table
            def sync(self, to_process=1):

                # Print status
                sysmsg.info("♻️  📝 Synching new objects added to the registry with 'FieldsChanged' airflow tables.")

                # Loop over registry data schemas
                for schema_name in ['graph_lectures', 'graph_registry']:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing nodes on schema '{schema_name}' ...")

                    # Count new object nodes to sync
                    sql_query = f"""
                              SELECT cp.object_type, COUNT(*) AS n
                                FROM {schema_name}.Nodes_N_Object cp
                           LEFT JOIN graph_airflow.Operations_N_Object_T_FieldsChanged fc
                               USING (institution_id, object_type, object_id)
                               WHERE fc.object_id IS NULL
                                 AND cp.object_type != 'Transcript'
                                 AND cp.object_type != 'Slide'
                            GROUP BY cp.object_type
                    """
                    out = self.db.execute_query(engine_name='test', query=sql_query)

                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO graph_airflow.Operations_N_Object_T_FieldsChanged
                                    (institution_id, object_type, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process)
                              SELECT cp.institution_id, cp.object_type, cp.object_id, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Nodes_N_Object cp
                           LEFT JOIN graph_airflow.Operations_N_Object_T_FieldsChanged fc
                               USING (institution_id, object_type, object_id)
                               WHERE fc.object_id IS NULL
                                 AND cp.object_type != 'Transcript'
                                 AND cp.object_type != 'Slide';
                    """
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query)

                    # Print status
                    sysmsg.trace(f"New objects synched: {out}'")
                    
                    # Print status
                    sysmsg.trace(f"⚙️  Processing edges on schema '{schema_name}' ...")

                    # Count new object-to-object edges to sync
                    sql_query = f"""
                              SELECT cp.from_object_type, cp.to_object_type, COUNT(*) AS n
                                FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent cp
                           LEFT JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged fc
                               USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                               WHERE fc.from_object_id IS NULL
                                 AND cp.from_object_type != 'Transcript' AND cp.to_object_type != 'Transcript'
                                 AND cp.from_object_type != 'Slide'      AND cp.to_object_type != 'Slide'
                            GROUP BY cp.from_object_type, cp.to_object_type
                    """
                    out = self.db.execute_query(engine_name='test', query=sql_query)
                    
                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged
                                    (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process)
                              SELECT cp.from_institution_id, cp.from_object_type, cp.from_object_id, cp.to_institution_id, cp.to_object_type, cp.to_object_id, NULL AS checksum_current, NULL AS checksum_previous, NULL AS has_changed, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent cp
                           LEFT JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged fc
                               USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                               WHERE fc.from_object_id IS NULL
                                 AND cp.from_object_type != 'Transcript' AND cp.to_object_type != 'Transcript'
                                 AND cp.from_object_type != 'Slide'      AND cp.to_object_type != 'Slide'
                    """
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query)
                    
                    # Print status
                    sysmsg.trace(f"New object tuples synched: {out}'")
            
                # Print status
                sysmsg.success("♻️  ✅ Done synching new objects between registry and 'FieldsChanged' airflow tables.\n")

            # Randomize airflow fields [OPTIONAL: For testing purposes]
            def randomize(self, time_period=182):

                # Print status
                sysmsg.info("🎲 📝 Randomize 'last_date_cached' field in 'FieldsChanged' airflow tables.")

                # Loop over airflow tables
                for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                    # Set random date for "last_date_cached" column
                    self.db.execute_query_in_chunks(
                        engine_name = 'test',
                        schema_name = 'graph_airflow',
                        table_name  = table_name,
                        query       = f"""
                            UPDATE graph_airflow.{table_name}
                            SET last_date_cached = CURDATE() - INTERVAL FLOOR(RAND() * {time_period}) DAY
                        """,
                        chunk_size  = 1000000)
                    
                # Print status
                sysmsg.success("🎲 ✅ Done randomizing dates in 'FieldsChanged' airflow tables.\n")

            # Set expiration dates
            def expire(self, older_than=90, limit_per_type=100):

                # Print status
                sysmsg.info("⌛️ 📝 Set 'has_expired' flag to 1 for expired dates in 'FieldsChanged' airflow tables.")

                # Print parameters
                sysmsg.trace(f"Input parameters: older_than={older_than} (days), limit_per_type={limit_per_type} (rows).")

                # Loop over airflow tables
                for u, table_name in [('n', 'Operations_N_Object_T_FieldsChanged'), ('e', 'Operations_N_Object_N_Object_T_FieldsChanged')]:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table '{table_name}' - resetting all 'has_expired' flags ...")

                    # Reset all expiration flags
                    self.db.execute_query_in_shell(
                        engine_name = 'test',
                        query       = f"""
                            UPDATE graph_airflow.{table_name}
                               SET has_expired = 0
                             WHERE has_expired > 0.5
                        """)
                    
                    # Print status
                    sysmsg.trace(f"⚙️  Processing table '{table_name}' - setting 'has_expired' flags to 1 ...")

                    # Set has_expired=1 for dates older than time_period (and NULL dates if include_new=True)
                    self.db.execute_query_in_shell(
                        engine_name = 'test',
                        query       = f"""
                            WITH ranked_rows AS (
                                SELECT row_id
                                FROM (
                                    SELECT row_id,
                                           ROW_NUMBER() OVER (PARTITION BY {'object_type' if u=='n' else 'from_object_type, to_object_type'} ORDER BY row_id) AS rn
                                      FROM graph_airflow.{table_name}
                                     WHERE has_expired IS NULL
                                        OR last_date_cached < CURDATE() - INTERVAL {older_than} DAY
                                        OR last_date_cached IS NULL
                                ) AS ranked
                                WHERE rn <= {limit_per_type}
                            )
                            UPDATE graph_airflow.{table_name}
                            JOIN ranked_rows USING (row_id)
                            SET has_expired = 1
                        """)

                # Print status
                sysmsg.success("⌛️ ✅ Done updating 'has_expired' flags in 'FieldsChanged' airflow tables.\n")

            # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
            def refresh(self, refresh_checksums=False):

                # Print status
                sysmsg.info("🧩 🏁 📝 Refresh checksums and set 'to_process' flags to 1 in 'FieldsChanged' airflow tables.")

                #---------------------------------------#
                # Refresh checksums and flags for NODEs #
                #---------------------------------------#

                # Print status
                sysmsg.trace(f"Re-calculate checksums and set 'has_changed' flag for graph nodes.")

                # Process checksums only if 'refresh_checksums' is True
                if refresh_checksums:

                    # Loop over node types tables
                    with tqdm(object_type_to_institution_id.items(), unit='Node type') as pb:
                        for object_type, institution_id in pb:
                            
                            # Print status
                            pb.set_description(f"⚙️  Node type: {object_type}".ljust(PBWIDTH)[:PBWIDTH])

                            # Get all objects with expired checksums
                            out = self.get(object_key=(institution_id, object_type), has_expired=1)

                            # If no edges found, continue to next iteration
                            if len(out) == 0:
                                continue

                            # Loop over objects with expired checksums
                            for dmy1, dmy2, object_id, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process in tqdm(out, desc=f"⚙️  Updating checksums for type '{object_type}'".ljust(PBWIDTH)[:PBWIDTH]):

                                # Get node (by which it calculates a new checksum)
                                node = GraphRegistry.Node(object_key=(institution_id, object_type, object_id))

                                # Assign new checksum and compare with previous
                                checksum_current = node.checksum
                                has_changed = 1 if checksum_current != checksum_previous else 0

                                # Set last_date_calculated to current date
                                # last_date_calculated = datetime.datetime.now().strftime('%Y-%m-%d')

                                # Commit new checksum and flag
                                self.set(
                                    object_key           = (institution_id, object_type, object_id),
                                    checksum_current     = checksum_current,
                                    has_changed          = has_changed,
                                    # last_date_calculated = last_date_calculated
                                )

                # Otherwise, move on
                else:
                    sysmsg.warning(f"Flag 'refresh_checksums' is set to False. Skipping checksum refresh for nodes.")

                #---------------------------------------#
                # Refresh checksums and flags for EDGEs #
                #---------------------------------------#

                # Print status
                sysmsg.trace(f"Re-calculate checksums and set 'has_changed' flag for graph edges.")

                # Process checksums only if 'refresh_checksums' is True
                if refresh_checksums:

                    # Generate all tuple combinations of object types
                    from_to_object_type_pairs = list(itertools.product(object_type_to_institution_id.items(), repeat=2))
                    from_to_object_type_pairs = [(e[0][1],e[0][0],e[1][1],e[1][0]) for e in list(from_to_object_type_pairs)]

                    # Loop over edge types tables
                    with tqdm(from_to_object_type_pairs, unit='Edge type') as pb:
                        for from_institution_id, from_object_type, to_institution_id, to_object_type in pb:

                            # Print status
                            pb.set_description(f"⚙️  Edge type: {from_object_type} -> {to_object_type}".ljust(PBWIDTH)[:PBWIDTH])

                            # Get all edges with expired checksums
                            out = self.get(object_key=(from_institution_id, from_object_type, to_institution_id, to_object_type), has_expired=1)

                            # If no edges found, continue to next iteration
                            if len(out) == 0:
                                continue

                            # Loop over edges with expired checksums
                            for dmy1, dmy2, from_object_id, dmy3, dmy4, to_object_id, context, checksum_current, checksum_previous, has_changed, last_date_cached, has_expired, to_process in tqdm(out, desc=f"⚙️  Updating checksums for edge '{from_object_type} -> {to_object_type}'".ljust(PBWIDTH)[:PBWIDTH]):

                                # Get edge (by which it calculates a new checksum)
                                edge = GraphRegistry.Edge(object_key=(from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context))
                                
                                # Assign new checksum and compare with previous
                                checksum_current = edge.checksum
                                has_changed = 1 if checksum_current != checksum_previous else 0

                                # Set last_date_calculated to current date
                                # last_date_calculated = datetime.datetime.now().strftime('%Y-%m-%d')

                                # Commit new checksum and flag
                                self.set(
                                    object_key           = (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id),
                                    checksum_current     = checksum_current,
                                    has_changed          = has_changed,
                                    # last_date_calculated = last_date_calculated
                                )

                # Otherwise, move on
                else:
                    sysmsg.warning(f"Flag 'refresh_checksums' is set to False. Skipping checksum refresh for edges.")

                #------------------------------------------#
                # Update 'to_process' flags in both tables #
                #------------------------------------------#

                # Print status
                sysmsg.trace(f"Set 'to_process' flags to 1.")

                # Loop over airflow tables
                for table_name in ['Operations_N_Object_T_FieldsChanged', 'Operations_N_Object_N_Object_T_FieldsChanged']:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing table '{table_name}' ...")

                    # Reset to_process flags for all nodes
                    self.db.execute_query_in_shell(engine_name='test', query=f"""
                        UPDATE graph_airflow.{table_name}
                           SET to_process = 0
                         WHERE to_process > 0.5
                    """)

                    # Update to_process flags for nodes
                    self.db.execute_query_in_shell(engine_name='test', query=f"""
                        UPDATE graph_airflow.{table_name}
                           SET to_process = 1
                         WHERE has_changed > 0.5
                            OR has_expired > 0.5
                            OR last_date_cached IS NULL
                    """)

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
                               SUM(NOT ISNULL(last_date_cached) AND NOT has_changed AND has_expired) AS cache_expired
                          FROM graph_airflow.{table_name}
                      GROUP BY {'object_type' if u=='n' else 'from_object_type, to_object_type'}
                        HAVING new_or_never_cached + checksum_changed + cache_expired > 0
                    """

                    # Execute evaluation query
                    out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                    df = pd.DataFrame(out, columns=[['object_type'] if u=='n' else ['from_object_type', 'to_object_type']][0]+['new_or_never_cached', 'checksum_changed', 'cache_expired'])
                    print_dataframe(df, title=f'🔍 Evaluation results for table: "{table_name}"')

                    # Generate evaluation query
                    sql_query_eval = f"""
                        SELECT 'Total' AS c,
                               SUM(    ISNULL(last_date_cached)                                    ) AS new_or_never_cached,
                               SUM(NOT ISNULL(last_date_cached) AND     has_changed                ) AS checksum_changed,
                               SUM(NOT ISNULL(last_date_cached) AND NOT has_changed AND has_expired) AS cache_expired
                          FROM graph_airflow.{table_name}
                    """

                    # Execute evaluation query
                    out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                    df = pd.DataFrame(out, columns=['TOTAL', 'new_or_never_cached', 'checksum_changed', 'cache_expired'])
                    print_dataframe(df, title=f'🔍 Evaluation results for table: "{table_name}"')

                # Print status
                sysmsg.success("🧩 🏁 ✅ Done refreshing checksums and setting 'to_process' flags in 'FieldsChanged' airflow tables.\n")

        # === Scores Expired Flags ===
        class ScoresExpired():

            # Class constructor
            def __init__(self):
                self.db = GraphDB()

            # Print current settings
            def status(self, object_key=None):
                if object_key is not None:
                    sql_query = f"""
                        SELECT institution_id, object_type, object_id, last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                    """
                    if len(object_key) == 2:
                        sql_query += f"""WHERE (institution_id, object_type) = ("{object_key[0]}", "{object_key[1]}")"""
                    elif len(object_key) == 3:
                        sql_query += f"""WHERE (institution_id, object_type, object_id) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")"""
                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return
                    out = self.db.execute_query(engine_name='test', query=sql_query)
                    df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'object_id', 'last_date_cached', 'has_expired', 'to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🧮 SCORES EXPIRED: Object [by key or id]')
                else:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT institution_id, object_type, COUNT(*) AS n_to_process
                        FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                        WHERE to_process = 1
                    GROUP BY institution_id, object_type
                    """)
                    df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'n_to_process'])
                    if not df.empty:
                        print_dataframe(df, title='🧮 SCORES EXPIRED: Object [stats]')

            # Reset current settings
            def reset(self):
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                    SET to_process = 0
                    WHERE to_process = 1;
                """)

            # Quick configuration for input list of node and edge types
            def config(self, config_json, reset=False, verbose=False):
                
                # Reset airflow flags
                if reset:
                    self.reset()

                # Node types
                for d in config_json['nodes']:
                    node_type, process_fields, process_scores = d
                    institution_id = object_type_to_institution_id[node_type]
                    if process_scores:
                        self.set(object_key=(institution_id, node_type), to_process=1)

                # Print status
                if verbose:
                    self.status()
                print('Done.')

            # Set fields for input object type or id
            def set(self, object_key, last_date_cached=None, has_expired=None, to_process=None):
                
                columns_to_update = []
                for column, value in zip(['last_date_cached', 'has_expired', 'to_process'], [last_date_cached, has_expired, to_process]):
                    if value is not None:
                        columns_to_update.append(f"{column} = {value}")

                if len(columns_to_update) > 0:
                    if len(object_key) == 2:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                            SET {', '.join(columns_to_update)}
                            WHERE (institution_id, object_type) = ("{object_key[0]}", "{object_key[1]}");
                        """
                    elif len(object_key) == 3:
                        sql_query = f"""
                            UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                            SET {', '.join(columns_to_update)}
                            WHERE (institution_id, object_type, object_id) = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}");
                        """
                    else:
                        msg = 'Invalid key length.'
                        print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                        return                        
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query)
                else:
                    print('No columns to update.')
                    return

            # Get fields for input object id
            def get(self, object_key, print_output=False):
                if len(object_key) == 2:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT institution_id, object_type, COUNT(*) AS n_to_process
                        FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                        WHERE (institution_id, object_type)
                            = ("{object_key[0]}", "{object_key[1]}")
                     GROUP BY institution_id, object_type
                    """)
                elif len(object_key) == 3:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                        WHERE (institution_id, object_type, object_id)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}")
                    """)
                elif len(object_key) == 4:
                    return
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, COUNT(*) AS n_to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_ScoresExpired
                        WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}")
                        GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type
                    """)
                elif len(object_key) == 6:
                    out = self.db.execute_query(engine_name='test', query=f"""
                        SELECT last_date_cached, has_expired, to_process
                        FROM graph_airflow.Operations_N_Object_N_Object_T_ScoresExpired
                        WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                            = ("{object_key[0]}", "{object_key[1]}", "{object_key[2]}", "{object_key[3]}", "{object_key[4]}", "{object_key[5]}")
                    """)
                else:
                    msg = 'Invalid key length.'
                    print_colour(msg, colour='magenta', background='black', style='normal', display_method=True)
                    return
                if print_output:
                    print(out[0] if len(out) > 0 else [])
                return out[0] if len(out) > 0 else []

            # Sync new objects to operations table -> TODO: optimise queries and include graph_lectures (done?)
            def sync(self, to_process=1):
                
                # Print status
                sysmsg.info("♻️  📝 Synching new objects added to the registry with 'ScoresExpired' airflow tables.")

                # Loop over registry data schemas
                for schema_name in ['graph_lectures', 'graph_registry']:

                    # Print status
                    sysmsg.trace(f"⚙️  Processing nodes on schema '{schema_name}' ...")

                    # Count new object nodes to sync
                    sql_query = f"""
                              SELECT n.object_type, COUNT(*) AS n
                                FROM {schema_name}.Nodes_N_Object n
                           LEFT JOIN graph_airflow.Operations_N_Object_T_ScoresExpired o
                               USING (institution_id, object_type, object_id)
                               WHERE o.institution_id IS NULL
                                 AND n.object_type != 'Transcript'
                                 AND n.object_type != 'Slide'
                            GROUP BY n.object_type
                    """
                    out = self.db.execute_query(engine_name='test', query=sql_query)
                    
                    # Execute object sync
                    sql_query = f"""
                         INSERT INTO graph_airflow.Operations_N_Object_T_ScoresExpired
                                    (institution_id, object_type, object_id, last_date_cached, has_expired, to_process)
                              SELECT n.institution_id, n.object_type, n.object_id, NULL AS last_date_cached, NULL AS has_expired, {to_process} AS to_process
                                FROM {schema_name}.Nodes_N_Object n
                           LEFT JOIN graph_airflow.Operations_N_Object_T_ScoresExpired o
                               USING (institution_id, object_type, object_id)
                               WHERE o.institution_id IS NULL
                                 AND n.object_type != 'Transcript'
                                 AND n.object_type != 'Slide'
                    """
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query)
                    
                    # Print status
                    sysmsg.trace(f"New objects synched: {out}'")

                # Print status
                sysmsg.success("♻️  ✅ Done synching new objects between registry and 'ScoresExpired' airflow tables.\n")

            # Randomize airflow fields [OPTIONAL: For testing purposes]
            def randomize(self, time_period=182):

                # Print status
                sysmsg.info("🎲 📝 Randomize 'last_date_cached' field in 'ScoresExpired' airflow table.")

                # Print status
                sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' ...")
                
                # Set random date for "last_date_cached" column
                self.db.execute_query_in_chunks(
                    engine_name = 'test',
                    schema_name = 'graph_airflow',
                    table_name  = 'Operations_N_Object_T_ScoresExpired',
                    query       = f"""
                        UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                        SET last_date_cached = CURDATE() - INTERVAL FLOOR(RAND() * {time_period}) DAY
                    """,
                    chunk_size  = 100000)
                
                # Print status
                sysmsg.success("🎲 ✅ Done randomizing dates in 'ScoresExpired' airflow table.\n")

            # Set expiration dates
            def expire(self, older_than=90, limit_per_type=100):

                # Print status
                sysmsg.info("⌛️ 📝 Set 'has_expired' flag to 1 for expired dates in 'ScoresExpired' airflow table.")

                # Print parameters
                sysmsg.trace(f"Input parameters: older_than={older_than} (days), limit_per_type={limit_per_type} (rows).")

                # Print status
                sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' - resetting all 'has_expired' flags ...")

                # Reset all expiration flags
                self.db.execute_query_in_shell(
                    engine_name = 'test',
                    query       = f"""
                        UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                           SET has_expired = 0
                         WHERE has_expired > 0.5
                    """)
                
                # Print status
                sysmsg.trace(f"⚙️  Processing table 'Operations_N_Object_T_ScoresExpired' - setting 'has_expired' flags to 1 ...")

                # Set has_expired=1 for dates older than time_period
                self.db.execute_query_in_shell(
                    engine_name = 'test',
                    query       = f"""
                        WITH ranked_rows AS (
                            SELECT row_id
                            FROM (
                                SELECT row_id,
                                       ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY row_id) AS rn
                                  FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                                 WHERE has_expired IS NULL
                                    OR last_date_cached < CURDATE() - INTERVAL {older_than} DAY
                                    OR last_date_cached IS NULL
                            ) AS ranked
                            WHERE rn <= {limit_per_type}
                        )
                        UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                        JOIN ranked_rows USING (row_id)
                        SET has_expired = 1
                    """)

                # Print status
                sysmsg.success("⌛️ ✅ Done updating 'has_expired' flags in 'ScoresExpired' airflow table.\n")

            # Refresh to_process flags based on changed checksums, expired dates, and never processed objects
            def refresh(self):

                # Print status
                sysmsg.info("🏁 📝 Set 'to_process' flags to 1 in 'ScoresExpired' airflow tables.")

                #------------------------------------------#
                # Update 'to_process' flags in both tables #
                #------------------------------------------#

                # Print status
                sysmsg.trace(f"⚙️  Processing 'Operations_N_Object_T_ScoresExpired' table ...")

                # Reset to_process flags for all nodes
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                       SET to_process = 0
                     WHERE to_process > 0.5
                """)

                # Update to_process flags for nodes
                self.db.execute_query_in_shell(engine_name='test', query=f"""
                    UPDATE graph_airflow.Operations_N_Object_T_ScoresExpired
                       SET to_process = 1
                     WHERE has_expired > 0.5
                        OR last_date_cached IS NULL
                """)

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
                      FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                  GROUP BY object_type
                    HAVING new_or_never_cached + cache_expired > 0
                """

                # Execute evaluation query
                out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                df = pd.DataFrame(out, columns=['object_type', 'new_or_never_cached', 'cache_expired'])
                print_dataframe(df, title=f'🔍 Evaluation results for table: "Operations_N_Object_T_ScoresExpired"')

                # Generate evaluation query
                sql_query_eval = f"""
                    SELECT 'Total' AS c,
                            SUM(    ISNULL(last_date_cached)                ) AS new_or_never_cached,
                            SUM(NOT ISNULL(last_date_cached) AND has_expired) AS cache_expired
                      FROM graph_airflow.Operations_N_Object_T_ScoresExpired
                """

                # Execute evaluation query
                out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                df = pd.DataFrame(out, columns=['TOTAL', 'new_or_never_cached', 'cache_expired'])
                print_dataframe(df, title=f'🔍 Evaluation results for table: "Operations_N_Object_T_ScoresExpired"')








                # Print status
                sysmsg.success("🏁 ✅ Done setting 'to_process' flags in 'ScoresExpired' airflow tables.\n")
            
    #---------------------------------#
    # Subclass definition: Graph Node #
    #---------------------------------#
    class Node():

        # Class constructor
        def __init__(self, object_key=(None, None, None)):
            self.db = GraphDB()
            self.object_key = object_key
            self.institution_id, self.object_type, self.object_id = object_key
            self.object_title, self.raw_text, self.record_created_date, self.record_updated_date, self.custom_fields, self.page_profile, self.checksum = None, None, None, None, [], {}, None
            self.concepts_detection = None
            self.text_source = None
            self.set_from_existing()
                
        # Check if object exists
        def exists(self):
            schema = object_type_to_schema.get(self.object_type, 'graph_registry')
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT COUNT(*)
                FROM {schema}.Nodes_N_Object
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)[0][0] > 0.5
            return out

        # Print object info
        def info(self):
            out_json = self.to_json()
            out_json['checksum'] = self.checksum
            rich.print_json(data=out_json)

        # Output object as JSON
        def to_json(self):
            doc_json = deepcopy({
                'institution_id'      : self.institution_id,
                'object_type'         : self.object_type,
                'object_id'           : self.object_id,
                'object_title'        : self.object_title,
                'raw_text'            : self.raw_text,
                'record_created_date' : self.record_created_date if type(self.record_created_date)!=datetime.datetime else self.record_created_date.strftime('%Y-%m-%d %H:%M:%S'),
                'record_updated_date' : self.record_updated_date if type(self.record_updated_date)!=datetime.datetime else self.record_updated_date.strftime('%Y-%m-%d %H:%M:%S'),
                'custom_fields'       : self.custom_fields,
                'page_profile'        : self.page_profile
            })
            return doc_json

        # Set all object fields
        def set(self, object_key, object_title=None, raw_text=None, custom_fields=None, page_profile=None):

            # Set object fields from input
            self.object_key = object_key
            self.institution_id, self.object_type, self.object_id = object_key
            if object_title is not None:
                self.object_title = object_title
            if raw_text is not None:
                self.raw_text = raw_text
            if custom_fields is not None:
                self.custom_fields = custom_fields
            if page_profile is not None:
                self.page_profile = {k: v for k, v in page_profile.items() if v is not None}

            # Re-calculate checksum
            self.update_checksum()

        # Set object title field
        def set_title(self, object_title):

            # Set object title field
            self.object_title = object_title

            # Re-calculate checksum
            self.update_checksum()

        # Set raw text field
        def set_text(self, raw_text):

            # Set raw text field
            self.raw_text = raw_text

            # Re-calculate checksum
            self.update_checksum()

        # Set text source field for concept detection
        def set_text_source(self, text_source):

            # Set text source field for concept detection
            self.text_source = text_source

        # Set custom fields list
        def set_custom_fields(self, custom_fields):

            # Set custom fields list
            self.custom_fields = custom_fields

            # Re-calculate checksum
            self.update_checksum()

        # Set page profile field
        def set_page_profile(self, page_profile):

            # Set page profile field
            self.page_profile = page_profile

            # Re-calculate checksum
            self.update_checksum()

        # Set inner fields from existing object in database
        def set_from_existing(self):
            
            # Get schema name based on object type
            schema_name = object_type_to_schema.get(self.object_type, 'graph_registry')
            
            # Get basic object info
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT object_title, raw_text, record_created_date, record_updated_date
                FROM {schema_name}.Nodes_N_Object
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)
            if len(out) > 0:
                self.object_title        = out[0][0]
                self.raw_text            = out[0][1]
                self.record_created_date = out[0][2]
                self.record_updated_date = out[0][3]
            else:
                return

            # Get custom fields
            list_of_columns = ['field_language', 'field_name', 'field_value', 'record_created_date', 'record_updated_date']
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT {', '.join(list_of_columns)}
                FROM {schema_name}.Data_N_Object_T_CustomFields
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)
            self.custom_fields = []
            for doc in out:
                doc_json = {}
                for column_name in list_of_columns:
                    val = doc[list_of_columns.index(column_name)]
                    doc_json[column_name] = val if type(val)!=datetime.datetime else val.strftime('%Y-%m-%d %H:%M:%S')
                self.custom_fields.append(doc_json)

            # Get page profile
            list_of_columns = ['numeric_id_en', 'numeric_id_fr', 'numeric_id_de', 'numeric_id_it', 'short_code', 'subtype_en', 'subtype_fr', 'subtype_de', 'subtype_it', 'name_en_is_auto_generated', 'name_en_is_auto_corrected', 'name_en_is_auto_translated', 'name_en_translated_from', 'name_en_value', 'name_fr_is_auto_generated', 'name_fr_is_auto_corrected', 'name_fr_is_auto_translated', 'name_fr_translated_from', 'name_fr_value', 'name_de_is_auto_generated', 'name_de_is_auto_corrected', 'name_de_is_auto_translated', 'name_de_translated_from', 'name_de_value', 'name_it_is_auto_generated', 'name_it_is_auto_corrected', 'name_it_is_auto_translated', 'name_it_translated_from', 'name_it_value', 'description_short_en_is_auto_generated', 'description_short_en_is_auto_corrected', 'description_short_en_is_auto_translated', 'description_short_en_translated_from', 'description_short_en_value', 'description_short_fr_is_auto_generated', 'description_short_fr_is_auto_corrected', 'description_short_fr_is_auto_translated', 'description_short_fr_translated_from', 'description_short_fr_value', 'description_short_de_is_auto_generated', 'description_short_de_is_auto_corrected', 'description_short_de_is_auto_translated', 'description_short_de_translated_from', 'description_short_de_value', 'description_short_it_is_auto_generated', 'description_short_it_is_auto_corrected', 'description_short_it_is_auto_translated', 'description_short_it_translated_from', 'description_short_it_value', 'description_medium_en_is_auto_generated', 'description_medium_en_is_auto_corrected', 'description_medium_en_is_auto_translated', 'description_medium_en_translated_from', 'description_medium_en_value', 'description_medium_fr_is_auto_generated', 'description_medium_fr_is_auto_corrected', 'description_medium_fr_is_auto_translated', 'description_medium_fr_translated_from', 'description_medium_fr_value', 'description_medium_de_is_auto_generated', 'description_medium_de_is_auto_corrected', 'description_medium_de_is_auto_translated', 'description_medium_de_translated_from', 'description_medium_de_value', 'description_medium_it_is_auto_generated', 'description_medium_it_is_auto_corrected', 'description_medium_it_is_auto_translated', 'description_medium_it_translated_from', 'description_medium_it_value', 'description_long_en_is_auto_generated', 'description_long_en_is_auto_corrected', 'description_long_en_is_auto_translated', 'description_long_en_translated_from', 'description_long_en_value', 'description_long_fr_is_auto_generated', 'description_long_fr_is_auto_corrected', 'description_long_fr_is_auto_translated', 'description_long_fr_translated_from', 'description_long_fr_value', 'description_long_de_is_auto_generated', 'description_long_de_is_auto_corrected', 'description_long_de_is_auto_translated', 'description_long_de_translated_from', 'description_long_de_value', 'description_long_it_is_auto_generated', 'description_long_it_is_auto_corrected', 'description_long_it_is_auto_translated', 'description_long_it_translated_from', 'description_long_it_value', 'external_key_en', 'external_key_fr', 'external_key_de', 'external_key_it', 'external_url_en', 'external_url_fr', 'external_url_de', 'external_url_it', 'is_visible', 'record_created_date', 'record_updated_date']
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT {', '.join(list_of_columns)}
                FROM {schema_name}.Data_N_Object_T_PageProfile
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)
            self.page_profile = {}
            if len(out) > 0:
                for column_name in list_of_columns:
                    val = out[0][list_of_columns.index(column_name)]
                    if val is not None:
                        self.page_profile[column_name] = val if type(val)!=datetime.datetime else val.strftime('%Y-%m-%d %H:%M:%S')
            
            # Re-calculate checksum
            self.update_checksum()

        # Set object from JSON
        def set_from_json(self, doc_json):

            # Set object fields from input
            self.object_key     = (doc_json['institution_id'], doc_json['object_type'], doc_json['object_id'])
            self.institution_id = doc_json['institution_id']
            self.object_type    = doc_json['object_type']
            self.object_id      = doc_json['object_id']
            self.object_title   = doc_json['object_title']  if 'object_title'  in doc_json else None
            self.raw_text       = doc_json['raw_text']      if 'raw_text'      in doc_json else None
            self.custom_fields  = doc_json['custom_fields'] if 'custom_fields' in doc_json else []
            self.page_profile   = doc_json['page_profile']  if 'page_profile'  in doc_json else {}
            schema = object_type_to_schema.get(self.object_type, 'graph_registry')

            # Fetch record dates (node table)
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT record_created_date, record_updated_date
                FROM {schema}.Nodes_N_Object
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)
            if len(out) > 0:
                self.record_created_date = out[0][0] if type(out[0][0])!=datetime.datetime else out[0][0].strftime('%Y-%m-%d %H:%M:%S')
                self.record_updated_date = out[0][1] if type(out[0][1])!=datetime.datetime else out[0][1].strftime('%Y-%m-%d %H:%M:%S')

            # Fetch record dates (custom fields table)
            for k in range(len(self.custom_fields)):
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT record_created_date, record_updated_date
                    FROM {schema}.Data_N_Object_T_CustomFields
                    WHERE (institution_id, object_type, object_id, field_language, field_name)
                        = ("{self.institution_id}", "{self.object_type}", "{self.object_id}", "{self.custom_fields[k]['field_language']}", "{self.custom_fields[k]['field_name']}");
                """)
                if len(out) > 0:
                    self.custom_fields[k]['record_created_date'] = out[0][0] if type(out[0][0])!=datetime.datetime else out[0][0].strftime('%Y-%m-%d %H:%M:%S')
                    self.custom_fields[k]['record_updated_date'] = out[0][1] if type(out[0][1])!=datetime.datetime else out[0][1].strftime('%Y-%m-%d %H:%M:%S')

            # Fetch record dates (page profile table)
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT record_created_date, record_updated_date
                FROM {schema}.Data_N_Object_T_PageProfile
                WHERE (institution_id, object_type, object_id)
                    = ("{self.institution_id}", "{self.object_type}", "{self.object_id}");
            """)
            if len(out) > 0:
                self.page_profile['record_created_date'] = out[0][0] if type(out[0][0])!=datetime.datetime else out[0][0].strftime('%Y-%m-%d %H:%M:%S')
                self.page_profile['record_updated_date'] = out[0][1] if type(out[0][1])!=datetime.datetime else out[0][1].strftime('%Y-%m-%d %H:%M:%S')

            # Re-calculate checksum
            self.update_checksum()

        # Update global checksum
        def update_checksum(self):

            # Convert to JSON
            doc_json = self.to_json()

            # Drop all datetime fields
            doc_json.pop('record_created_date', None)
            doc_json.pop('record_updated_date', None)
            for k in range(len(doc_json['custom_fields'])):
                doc_json['custom_fields'][k].pop('record_created_date', None)
                doc_json['custom_fields'][k].pop('record_updated_date', None)
            doc_json['page_profile'].pop('record_created_date', None)
            doc_json['page_profile'].pop('record_updated_date', None)
            
            # Convert to a sorted JSON string
            serialized = json.dumps(doc_json, sort_keys=True, separators=(',', ':'))

            # Compute 32 char MD5 hash
            self.checksum = hashlib.md5(serialized.encode()).hexdigest()
    
        # Commit basic node data to database
        def commit_node_object(self, actions=('eval',)):
            schema = object_type_to_schema.get(self.object_type, 'graph_registry')
            eval_results = registry_insert(
                schema_name=schema,
                table_name='Nodes_N_Object',
                key_column_names=['institution_id', 'object_type', 'object_id'],
                key_column_values=[self.institution_id, self.object_type, self.object_id],
                upd_column_names=['object_title', 'raw_text'],
                upd_column_values=[self.object_title, self.raw_text],
                actions=actions,
                db_connector=self.db
            )
            return eval_results

        # Commit custom fields data to database
        def commit_custom_fields(self, actions=('eval',)):
            schema = object_type_to_schema.get(self.object_type, 'graph_registry')
            eval_results = []
            for doc in self.custom_fields:
                eval_results += [registry_insert(
                    schema_name=schema,
                    table_name='Data_N_Object_T_CustomFields',
                    key_column_names=['institution_id', 'object_type', 'object_id', 'field_language', 'field_name'],
                    key_column_values=[self.institution_id, self.object_type, self.object_id, doc['field_language'],
                                       doc['field_name']],
                    upd_column_names=['field_value'],
                    upd_column_values=[doc['field_value']],
                    actions=actions,
                    db_connector=self.db
                )]
            return eval_results

        # Commit page profile data to database
        def commit_page_profile(self, actions=('eval',)):
            schema = object_type_to_schema.get(self.object_type, 'graph_registry')
            # Prepare column names and values
            upd_column_names = []
            upd_column_values = []
            for f, v in self.page_profile.items():
                if f not in ['record_created_date', 'record_updated_date']:
                    upd_column_names.append(f)
                    upd_column_values.append(v)

            # Execute actions
            eval_results = registry_insert(
                schema_name=schema,
                table_name='Data_N_Object_T_PageProfile',
                key_column_names=['institution_id', 'object_type', 'object_id'],
                key_column_values=[self.institution_id, self.object_type, self.object_id],
                upd_column_names=upd_column_names,
                upd_column_values=upd_column_values,
                actions=actions,
                db_connector=self.db
            )
            return eval_results

        # Commit all to database
        def commit(self, actions=('eval',), verbose=True):

            # Print actions info
            if actions == ('eval',):
                print(f'\n🔍 Running on evaluation mode. No commits will be made to the database ...')

            # Commit all to database
            eval_results = {
                'node_object'   : self.commit_node_object(actions=actions),
                'custom_fields' : self.commit_custom_fields(actions=actions),
                'page_profile'  : self.commit_page_profile(actions=actions)
            }

            # Print actions info
            if verbose and 'commit' in actions:
                print(f'\n✅ All data committed to the database.')

            # Return evaluation results
            return eval_results

        # Get object's detected concepts
        def detect_concepts(self):
            if self.concepts_detection is None:
                self.concepts_detection = extract_concepts_from_text(self.raw_text, login_info=graphai_login_info)
            rich.print_json(data=self.concepts_detection)

        # Commit detected concepts to database
        def commit_concepts(self, actions=('eval',), delete_existing=False):
            schema_name = object_type_to_schema.get(self.object_type, 'graph_registry')
            eval_results = []
            for doc in self.concepts_detection:
                eval_results += [registry_insert(
                    schema_name       = schema_name,
                    table_name        = 'Edges_N_Object_N_Concept_T_ConceptDetection',
                    key_column_names  = ['institution_id', 'object_type', 'object_id', 'concept_id', 'text_source'],
                    key_column_values = [self.institution_id, self.object_type, self.object_id, doc['concept_id'], self.text_source],
                    upd_column_names  = ['score'],
                    upd_column_values = [doc['mixed_score']],
                    actions           = actions,
                    db_connector      = self.db
                )]

        # Refine detected concepts
        def refine_concepts(self):
            pass

        # Manually map concepts
        def manually_map_concepts(self, mode='interactive'):
            pass

    #-------------------------------------#
    # Subclass definition: Graph NodeList #
    #-------------------------------------#
    class NodeList():

        # Class constructor
        def __init__(self, object_key_list=()):
            self.db = GraphDB()
            self.object_list = [GraphRegistry.Node(object_key) for object_key in object_key_list]

        # Check if objects in list exist
        def exists(self):
            out = []
            for node in self.object_list:
                out.append(node.exists())
            return out
        
        # Print object info
        def info(self):
            for node in self.object_list:
                node.info()

        # Output object as JSON
        def to_json(self):
            doc_json_list = []
            for node in self.object_list:
                doc_json_list.append(node.to_json())
            return doc_json_list
        
        # Set object list from input JSON
        def set_from_json(self, doc_json_list=()):
            self.object_list = []
            for doc in doc_json_list:
                node = GraphRegistry.Node()
                node.set_from_json(doc)
                self.object_list.append(node)

        # Commit all objects to database
        def commit(self, actions=('eval',)):
            eval_results = []
            for node in self.object_list:
                eval_result = node.commit(actions, verbose=False)
                eval_results.append(eval_result)
            return eval_results

    #---------------------------------#
    # Subclass definition: Graph Edge #
    #---------------------------------#
    class Edge():

        # Class constructor
        def __init__(self, object_key=(None, None, None, None, None, None, None)):
            self.db = GraphDB()
            self.from_institution_id, self.from_object_type, self.from_object_id, self.to_institution_id, self.to_object_type, self.to_object_id, self.context = object_key
            self.record_created_date, self.record_updated_date, self.custom_fields = None, None, []
            self.set_from_existing()

        # get the schema based on the type of nodes at the ends of the edge
        @staticmethod
        def get_schema(from_object_type, to_object_type):
            schema_from = object_type_to_schema.get(from_object_type, 'graph_registry')
            schema_to = object_type_to_schema.get(to_object_type, 'graph_registry')
            if schema_from == 'graph_lectures' or schema_to == 'graph_lectures':
                return 'graph_lectures'
            elif schema_from == schema_to:
                return schema_from
            else:
                return 'graph_registry'

        def _get_schema(self):
            return self.get_schema(self.from_object_type, self.to_object_type)

        # Check if object exists
        def exists(self):
            schema = self._get_schema()
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT COUNT(*)
                FROM {schema}.Edges_N_Object_N_Object_T_ChildToParent
                WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
                    = ("{self.from_institution_id}", "{self.from_object_type}", "{self.from_object_id}", "{self.to_institution_id}", "{self.to_object_type}", "{self.to_object_id}", "{self.context}");
            """)[0][0] > 0.5
            return out

        # Print object info
        def info(self):
            out_json = self.to_json()
            out_json['checksum'] = self.checksum
            rich.print_json(data=out_json)

        # Output object as JSON
        def to_json(self):
            doc_json = deepcopy({
                'from_institution_id' : self.from_institution_id,
                'from_object_type'    : self.from_object_type,
                'from_object_id'      : self.from_object_id,
                'to_institution_id'   : self.to_institution_id,
                'to_object_type'      : self.to_object_type,
                'to_object_id'        : self.to_object_id,
                'context'             : self.context,
                'record_created_date' : self.record_created_date if type(self.record_created_date)!=datetime.datetime else self.record_created_date.strftime('%Y-%m-%d %H:%M:%S'),
                'record_updated_date' : self.record_updated_date if type(self.record_updated_date)!=datetime.datetime else self.record_updated_date.strftime('%Y-%m-%d %H:%M:%S'),
                'custom_fields'       : self.custom_fields
            })
            return doc_json

        # Set all object fields
        def set(self, object_key, custom_fields=None):

            # Set object fields from input
            self.from_institution_id, self.from_object_type, self.from_object_id, self.to_institution_id, self.to_object_type, self.to_object_id, self.context = object_key
            if custom_fields is not None:
                self.custom_fields = custom_fields

            # Re-calculate checksum
            self.update_checksum()

        # Set custom fields list
        def set_custom_fields(self, custom_fields):

            # Set custom fields list
            self.custom_fields = custom_fields

            # Re-calculate checksum
            self.update_checksum()

        # Set inner fields from existing object in database
        def set_from_existing(self):
            schema = self._get_schema()
            # Get custom fields
            list_of_columns = ['field_language', 'field_name', 'field_value', 'record_created_date', 'record_updated_date']
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT {', '.join(list_of_columns)}
                FROM {schema}.Data_N_Object_N_Object_T_CustomFields
                WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
                    = ("{self.from_institution_id}", "{self.from_object_type}", "{self.from_object_id}", "{self.to_institution_id}", "{self.to_object_type}", "{self.to_object_id}", "{self.context}");
            """)
            self.custom_fields = []
            for doc in out:
                doc_json = {}
                for column_name in list_of_columns:
                    val = doc[list_of_columns.index(column_name)]
                    doc_json[column_name] = val if type(val)!=datetime.datetime else val.strftime('%Y-%m-%d %H:%M:%S')
                self.custom_fields.append(doc_json)

            # Re-calculate checksum
            self.update_checksum()

        # Set object from JSON
        def set_from_json(self, doc_json):

            # Set object fields from input
            self.from_institution_id = doc_json['from_institution_id']
            self.from_object_type    = doc_json['from_object_type']
            self.from_object_id      = doc_json['from_object_id']
            self.to_institution_id   = doc_json['to_institution_id']
            self.to_object_type      = doc_json['to_object_type']
            self.to_object_id        = doc_json['to_object_id']
            self.context             = doc_json['context']
            self.custom_fields       = doc_json['custom_fields'] if 'custom_fields' in doc_json else []
            schema = self._get_schema()

            # Fetch record dates (node table)
            out = self.db.execute_query(engine_name='test', query=f"""
                SELECT record_created_date, record_updated_date
                FROM {schema}.Edges_N_Object_N_Object_T_ChildToParent
                WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
                    = ("{self.from_institution_id}", "{self.from_object_type}", "{self.from_object_id}", "{self.to_institution_id}", "{self.to_object_type}", "{self.to_object_id}", "{self.context}");
            """)
            if len(out) > 0:
                self.record_created_date = out[0][0] if type(out[0][0])!=datetime.datetime else out[0][0].strftime('%Y-%m-%d %H:%M:%S')
                self.record_updated_date = out[0][1] if type(out[0][1])!=datetime.datetime else out[0][1].strftime('%Y-%m-%d %H:%M:%S')

            # Fetch record dates (custom fields table)
            for k in range(len(self.custom_fields)):
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT record_created_date, record_updated_date
                    FROM {schema}.Data_N_Object_N_Object_T_CustomFields
                    WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, field_language, field_name)
                        = ("{self.from_institution_id}", "{self.from_object_type}", "{self.from_object_id}", "{self.to_institution_id}", "{self.to_object_type}", "{self.to_object_id}", "{self.context}", "{self.custom_fields[k]['field_language']}", "{self.custom_fields[k]['field_name']}");
                """)
                if len(out) > 0:
                    self.custom_fields[k]['record_created_date'] = out[0][0] if type(out[0][0])!=datetime.datetime else out[0][0].strftime('%Y-%m-%d %H:%M:%S')
                    self.custom_fields[k]['record_updated_date'] = out[0][1] if type(out[0][1])!=datetime.datetime else out[0][1].strftime('%Y-%m-%d %H:%M:%S')

            # Re-calculate checksum
            self.update_checksum()

        # Update global checksum
        def update_checksum(self):

            # Convert to JSON
            doc_json = self.to_json()

            # Drop all datetime fields
            doc_json.pop('record_created_date', None)
            doc_json.pop('record_updated_date', None)
            for k in range(len(doc_json['custom_fields'])):
                doc_json['custom_fields'][k].pop('record_created_date', None)
                doc_json['custom_fields'][k].pop('record_updated_date', None)
            
            # Convert to a sorted JSON string
            serialized = json.dumps(doc_json, sort_keys=True, separators=(',', ':'))

            # Compute 32 char MD5 hash
            self.checksum = hashlib.md5(serialized.encode()).hexdigest()

        # Commit basic edge data to database
        def commit_edge_object(self, actions=('eval',)):
            schema = self._get_schema()
            eval_results = registry_insert(
                schema_name=schema,
                table_name='Edges_N_Object_N_Object_T_ChildToParent',
                key_column_names=['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id',
                                  'to_object_type', 'to_object_id', 'context'],
                key_column_values=[self.from_institution_id, self.from_object_type, self.from_object_id,
                                   self.to_institution_id, self.to_object_type, self.to_object_id, self.context],
                upd_column_names=[],
                upd_column_values=[],
                actions=actions,
                db_connector=self.db
            )
            return eval_results

        # Commit custom fields data to database
        def commit_custom_fields(self, actions=('eval',)):
            schema = self._get_schema()
            eval_results = []
            for doc in self.custom_fields:
                eval_results += [registry_insert(
                    schema_name=schema,
                    table_name='Data_N_Object_N_Object_T_CustomFields',
                    key_column_names=['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id',
                                      'to_object_type', 'to_object_id', 'field_language', 'field_name', 'context'],
                    key_column_values=[self.from_institution_id, self.from_object_type, self.from_object_id,
                                       self.to_institution_id, self.to_object_type, self.to_object_id,
                                       doc['field_language'], doc['field_name'], self.context],
                    upd_column_names=['field_value'],
                    upd_column_values=[doc['field_value']],
                    actions=actions,
                    db_connector=self.db
                )]
            return eval_results

        # Commit all to database
        def commit(self, actions=('eval',), verbose=True):

            # Print actions info
            if actions == ('eval',):
                print(f'\n🔍 Running on evaluation mode. No commits will be made to the database ...')

            # Commit all to database
            eval_results = {
                'edge_object'   : self.commit_edge_object(actions=actions),
                'custom_fields' : self.commit_custom_fields(actions=actions)
            }

            # Print actions info
            if verbose and 'commit' in actions:
                print(f'\n✅ All data committed to the database.')

            # Return evaluation results
            return eval_results


        # # Commit object to database
        # def commit(self, update_existing=True, test_mode=False):

        #     if not update_existing:
        #         if self.exists():
        #             print('Object exists. Update existing is set to OFF.')
        #             return
            
        #     # Update object node table
        #     t = 'graph_registry.Edges_N_Object_N_Object_T_ChildToParent'
        #     sql_query = f"""
        #         INSERT IGNORE INTO {t} (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
        #         VALUES ('{self.from_institution_id}', '{self.from_object_type}', '{self.from_object_id}', '{self.to_institution_id}', '{self.to_object_type}', '{self.to_object_id}', '{self.context}');
        #     """
        #     # Execute commit
        #     if test_mode:
        #         print(sql_query)
        #     else:
        #         self.db.execute_query_in_shell(engine_name='test', query=sql_query)

        #     # Update custom fields table
        #     t = 'graph_registry.Data_N_Object_N_Object_T_CustomFields'
        #     custom_columns = ['field_language', 'field_name', 'field_value']
        #     date_update_conditions  = ' OR '.join([f"{t}.{c} != d.{c}" for c in custom_columns])
        #     value_update_conditions =   ', '.join([f"{c} = IF({t}.{c} != d.{c}, d.{c}, {t}.{c})" for c in custom_columns])
        #     for doc in self.custom_fields:
        #         sql_query = f"""
        #              INSERT INTO {t}
        #                         (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, field_language, field_name, field_value)
        #                   SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, field_language, field_name, field_value
        #                     FROM (SELECT '{self.from_institution_id}' AS from_institution_id,
        #                                  '{self.from_object_type}'    AS from_object_type,
        #                                  '{self.from_object_id}'      AS from_object_id,
        #                                  '{self.to_institution_id}'   AS to_institution_id,
        #                                  '{self.to_object_type}'      AS to_object_type,
        #                                  '{self.to_object_id}'        AS to_object_id,
        #                                  '{self.context}'             AS context,
        #                                  '{doc['field_language']}'    AS field_language,
        #                                  '{doc['field_name']}'        AS field_name,
        #                                  '{doc['field_value']}'       AS field_value
        #                          ) AS d
        #         ON DUPLICATE KEY
        #                   UPDATE record_updated_date = IF(
        #                          {date_update_conditions},
        #                          CURRENT_TIMESTAMP, {t}.record_updated_date),
        #                          {value_update_conditions};
        #         """
        #         # Execute commit
        #         if test_mode:
        #             print(sql_query)
        #         else:
        #             self.db.execute_query_in_shell(engine_name='test', query=sql_query)

    #-------------------------------------#
    # Subclass definition: Graph EdgeList #
    #-------------------------------------#
    class EdgeList():

        # Class constructor
        def __init__(self, object_key_list=[]):
            self.db = GraphDB()
            self.object_list = [GraphRegistry.Edge(object_key) for object_key in object_key_list]

        # Check if objects in list exist
        def exists(self):
            out = []
            for edge in self.object_list:
                out.append(edge.exists())
            return out

        # Print object info
        def info(self):
            for edge in self.object_list:
                edge.info()

        # Output object as JSON
        def to_json(self):
            doc_json_list = []
            for edge in self.object_list:
                doc_json_list.append(edge.to_json())
            return doc_json_list

        # Set object list from input JSON
        def set_from_json(self, doc_json_list=[]):
            self.object_list = []
            for doc in doc_json_list:
                edge = GraphRegistry.Edge()
                edge.set_from_json(doc)
                self.object_list.append(edge)

        # Commit all objects to database
        def commit(self, actions=('eval',)):
            eval_results = []
            for edge in self.object_list:
                eval_result = edge.commit(actions, verbose=False)
                eval_results.append(eval_result)
            return eval_results

    #-----------------------------------------------------#
    # Subclass definition: GraphRegistry Cache Management #
    #-----------------------------------------------------#
    class CacheManagement():

        # Class constructor
        def __init__(self):
            self.db = GraphDB()

        # Commit for all views
        def apply_views(self, actions=()):

            # Print status
            sysmsg.info(f"🚀 📝 Execute views and commit updated data to 'graph_cache' [actions: {actions}].")

            # List of views to execute
            list_of_views = [
                'obj2obj: all fields symmetric',
                'obj: page profile',
                'obj: all fields',
                'obj2obj: parent-child symmetric',
                'obj2obj: parent-child symmetric (ontology)'
            ]
            
            # Execute and commit all views
            for view_name in list_of_views:
                self.cache_update_from_view(view_name, actions=actions)

            # Print status
            sysmsg.success(f"🚀 ✅ Done executing views and committing updated data to 'graph_cache'.\n")

        # Compute and cache scores
        def apply_formulas(self, verbose=True):

            # Print status
            sysmsg.info(f"🚀 📝 Apply formulas commit updated data to 'graph_cache' [verbose: {verbose}].")

            # List of formulas to execute
            list_of_formulas = [
                'Traversal_*',
                'Edges_N_Object_N_Concept_T_CalculatedScores',
                'Edges_N_Object_N_Concept_T_FinalScores',
                'Edges_N_Object_N_Category_T_CalculatedScores',
                'Nodes_N_Object_T_DegreeScores'
            ]

            # Execute and commit all formulas
            for formula_name in list_of_formulas:
                self.cache_update_from_formula(formula_name, verbose=verbose)
            
            # Print status
            sysmsg.success(f"🚀 ✅ Done applying formulas and committing updated data to 'graph_cache'.\n")

        # Update cache table from registry view
        def cache_update_from_view(self, view_name, actions=()):

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            if view_name == 'obj2obj: all fields symmetric':

                # Target cache table
                target_table = 'Data_N_Object_N_Object_T_AllFieldsSymmetric'

                # List of evaluation columns
                eval_columns = ['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type', 'field_name']

                # Initialise query stack
                sql_query_stack = []

                # Query template
                sql_query_template = """
                    SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                           cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                           cf.field_language, cf.field_name, cf.field_value
                      FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_N_Object_T_%s cf
                     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                     WHERE tp.to_process = 1
                       AND tf.flag_type = 'fields'
                       AND tf.to_process = 1

                 UNION ALL

                    SELECT cf.to_institution_id   AS from_institution_id,
                           cf.to_object_type      AS from_object_type,
                           cf.to_object_id        AS from_object_id,
                           cf.from_institution_id AS to_institution_id,
                           cf.from_object_type    AS to_object_type,
                           cf.from_object_id      AS to_object_id,
                           cf.field_language, cf.field_name, cf.field_value
                      FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_N_Object_T_%s cf
                     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                     WHERE tp.to_process = 1
                       AND tf.flag_type = 'fields'
                       AND tf.to_process = 1
                """

                # Append queries for custom fields
                for schema_name in ['graph_registry', 'graph_lectures', 'graph_ontology']:
                    sql_query_stack += [sql_query_template % (schema_name, 'CustomFields', schema_name, 'CustomFields')]

                # Append query for cached calculated fields
                sql_query_stack += [sql_query_template % ('graph_cache', 'CalculatedFields', 'graph_cache', 'CalculatedFields')]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj: page profile':

                # Target cache table
                target_table = 'Data_N_Object_T_PageProfile'

                # List of evaluation columns
                eval_columns = ['institution_id', 'object_type']

                # Initialise query stack
                sql_query_stack = []

                # Loop over schemas
                for schema_name in ['graph_registry', 'graph_lectures', 'graph_ontology']:

                    # Append query
                    sql_query_stack += [f"""
                        SELECT pp.institution_id, pp.object_type, pp.object_id, pp.numeric_id_en, pp.numeric_id_fr, pp.numeric_id_de, pp.numeric_id_it, pp.short_code, pp.subtype_en, pp.subtype_fr, pp.subtype_de, pp.subtype_it, pp.name_en_is_auto_generated, pp.name_en_is_auto_corrected, pp.name_en_is_auto_translated, pp.name_en_translated_from, pp.name_en_value, pp.name_fr_is_auto_generated, pp.name_fr_is_auto_corrected, pp.name_fr_is_auto_translated, pp.name_fr_translated_from, pp.name_fr_value, pp.name_de_is_auto_generated, pp.name_de_is_auto_corrected, pp.name_de_is_auto_translated, pp.name_de_translated_from, pp.name_de_value, pp.name_it_is_auto_generated, pp.name_it_is_auto_corrected, pp.name_it_is_auto_translated, pp.name_it_translated_from, pp.name_it_value, pp.description_short_en_is_auto_generated, pp.description_short_en_is_auto_corrected, pp.description_short_en_is_auto_translated, pp.description_short_en_translated_from, pp.description_short_en_value, pp.description_short_fr_is_auto_generated, pp.description_short_fr_is_auto_corrected, pp.description_short_fr_is_auto_translated, pp.description_short_fr_translated_from, pp.description_short_fr_value, pp.description_short_de_is_auto_generated, pp.description_short_de_is_auto_corrected, pp.description_short_de_is_auto_translated, pp.description_short_de_translated_from, pp.description_short_de_value, pp.description_short_it_is_auto_generated, pp.description_short_it_is_auto_corrected, pp.description_short_it_is_auto_translated, pp.description_short_it_translated_from, pp.description_short_it_value, pp.description_medium_en_is_auto_generated, pp.description_medium_en_is_auto_corrected, pp.description_medium_en_is_auto_translated, pp.description_medium_en_translated_from, pp.description_medium_en_value, pp.description_medium_fr_is_auto_generated, pp.description_medium_fr_is_auto_corrected, pp.description_medium_fr_is_auto_translated, pp.description_medium_fr_translated_from, pp.description_medium_fr_value, pp.description_medium_de_is_auto_generated, pp.description_medium_de_is_auto_corrected, pp.description_medium_de_is_auto_translated, pp.description_medium_de_translated_from, pp.description_medium_de_value, pp.description_medium_it_is_auto_generated, pp.description_medium_it_is_auto_corrected, pp.description_medium_it_is_auto_translated, pp.description_medium_it_translated_from, pp.description_medium_it_value, pp.description_long_en_is_auto_generated, pp.description_long_en_is_auto_corrected, pp.description_long_en_is_auto_translated, pp.description_long_en_translated_from, pp.description_long_en_value, pp.description_long_fr_is_auto_generated, pp.description_long_fr_is_auto_corrected, pp.description_long_fr_is_auto_translated, pp.description_long_fr_translated_from, pp.description_long_fr_value, pp.description_long_de_is_auto_generated, pp.description_long_de_is_auto_corrected, pp.description_long_de_is_auto_translated, pp.description_long_de_translated_from, pp.description_long_de_value, pp.description_long_it_is_auto_generated, pp.description_long_it_is_auto_corrected, pp.description_long_it_is_auto_translated, pp.description_long_it_translated_from, pp.description_long_it_value, pp.external_key_en, pp.external_key_fr, pp.external_key_de, pp.external_key_it, pp.external_url_en, pp.external_url_fr, pp.external_url_de, pp.external_url_it, pp.is_visible, 1 AS to_process
                          FROM graph_airflow.Operations_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Data_N_Object_T_PageProfile pp
                         USING (institution_id, object_type, object_id)
                    INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
                         USING (institution_id, object_type)
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
                eval_columns = ['institution_id', 'object_type', 'field_name']

                # Initialise query stack
                sql_query_stack = []

                # Query template
                sql_query_template = """
                    SELECT cf.institution_id, cf.object_type, cf.object_id,
                           cf.field_language, cf.field_name, cf.field_value
                      FROM graph_airflow.Operations_N_Object_T_FieldsChanged tp
                INNER JOIN %s.Data_N_Object_T_%s cf
                     USING (institution_id, object_type, object_id)
                INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
                     USING (institution_id, object_type)
                     WHERE tp.to_process = 1
                       AND tf.flag_type = 'fields'
                       AND tf.to_process = 1
                """

                # Append queries for custom fields
                for schema_name in ['graph_registry', 'graph_lectures', 'graph_ontology']:
                    sql_query_stack += [sql_query_template % (schema_name, 'CustomFields')]

                # Append query for cached calculated fields
                sql_query_stack += [sql_query_template % ('graph_cache', 'CalculatedFields')]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj2obj: parent-child symmetric':

                # Target cache table
                target_table = 'Edges_N_Object_N_Object_T_ParentChildSymmetric'

                # List of evaluation columns
                eval_columns = ['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type']

                # Initialise query stack
                sql_query_stack = []

                # Loop over schemas
                for schema_name in ['graph_registry', 'graph_lectures']:

                    # Append query
                    sql_query_stack += [f"""
                        SELECT 'Child-to-Parent' AS edge_type,
                               c2p.from_institution_id, c2p.from_object_type, c2p.from_object_id,
                               c2p.to_institution_id,   c2p.to_object_type,   c2p.to_object_id,
                               COALESCE(c2p.context, 'n/a') AS context, 1 AS to_process
                          FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                    INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                         WHERE tp.to_process = 1
                           AND tf.flag_type = 'fields'
                           AND tf.to_process = 1
            
                     UNION ALL

                        SELECT 'Parent-to-Child' AS edge_type,
                               c2p.to_institution_id   AS from_institution_id,
                               c2p.to_object_type      AS from_object_type,
                               c2p.to_object_id        AS from_object_id,
                               c2p.from_institution_id AS to_institution_id,
                               c2p.from_object_type    AS to_object_type,
                               c2p.from_object_id      AS to_object_id,
                               COALESCE(CONCAT(c2p.context, ' (mirror)'), 'n/a') AS context, 1 AS to_process
                          FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
                    INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                         WHERE tp.to_process = 1
                           AND tf.flag_type = 'fields'
                           AND tf.to_process = 1
                    """]

                # Build query (base)
                sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'obj2obj: parent-child symmetric (ontology)':

                # Target cache table
                target_table = 'Edges_N_Object_N_Object_T_ParentChildSymmetric'

                # List of evaluation columns
                eval_columns = ['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type']

                # Build query (base)
                sql_query = f"""

                    SELECT 'Child-to-Parent' AS edge_type,
                        'Ont' AS from_institution_id, 'Category' AS from_object_type, c2p.from_id AS from_object_id,
                        'Ont' AS   to_institution_id, 'Category' AS   to_object_type,   c2p.to_id AS   to_object_id,
                        'n/a' AS context, 1 AS to_process
                    FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                    USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent c2p
                        ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                        = ('Ont', 'Category', c2p.from_id, 'Ont', 'Category', c2p.to_id)
                    WHERE tp.to_process = 1
                    AND tf.flag_type = 'fields'
                    AND tf.to_process = 1
                
                UNION ALL
                
                    SELECT 'Parent-to-Child' AS edge_type,
                        'Ont' AS from_institution_id, 'Category' AS from_object_type,   to_id AS from_object_id,
                        'Ont' AS   to_institution_id, 'Category' AS   to_object_type, from_id AS   to_object_id,
                        'n/a' AS context, 1 AS to_process
                    FROM graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                    USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent c2p
                        ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                        = ('Ont', 'Category', c2p.to_id, 'Ont', 'Category', c2p.from_id)
                    WHERE tp.to_process = 1
                    AND tf.flag_type = 'fields'
                    AND tf.to_process = 1
                    
                UNION ALL
                    
                    SELECT 'Parent-to-Child' AS edge_type,
                        'Ont' AS from_institution_id, 'Category' AS from_object_type, c.from_id AS from_object_id,
                        'Ont' AS   to_institution_id, 'Concept'  AS   to_object_type,   l.to_id AS   to_object_id,
                        'n/a' AS context, 1 AS to_process
                    FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild c
                INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild l
                        ON c.to_id = l.from_id
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                        ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                        = ('Ont', 'Category', c.from_id, 'Ont', 'Concept', l.to_id)
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                    USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                    WHERE tp.to_process = 1
                    AND tf.flag_type = 'fields'
                    AND tf.to_process = 1
                        
                UNION ALL

                    SELECT 'Child-to-Parent' AS edge_type,
                        'Ont' AS from_institution_id, 'Concept'  AS from_object_type,   l.to_id AS from_object_id,
                        'Ont' AS   to_institution_id, 'Category' AS   to_object_type, c.from_id AS   to_object_id,
                        'n/a' AS context, 1 AS to_process
                    FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild c
                INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild l
                        ON c.to_id = l.from_id
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_FieldsChanged tp
                        ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                        = ('Ont', 'Category', l.to_id, 'Ont', 'Concept', c.from_id)
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                    USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                    WHERE tp.to_process = 1
                    AND tf.flag_type = 'fields'
                    AND tf.to_process = 1
                """
           
            #------------------------------#
            # Process query for input name #
            #------------------------------#
            elif view_name == 'template':

                # Target cache table
                target_table = 'template'

                # List of evaluation columns
                eval_columns = ['from_institution_id', 'from_object_type', 'to_institution_id', 'to_object_type']

                # Build query (base)
                sql_query = f"""
                """

            #-------------------------#
            # Process resulting query #
            #-------------------------#

            # Print base query
            if 'print' in actions:
                print(sql_query)

            # Evaluate query
            if 'eval' in actions:

                # Build evaluation query
                sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"

                # Print evaluation query
                if 'print' in actions:
                    print(sql_query_eval)

                # Execute evaluation query
                out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                if len(df) > 0:
                    print_dataframe(df, title=f'🔍 Evaluation results for view: "{view_name}"')

            # Execute commit
            if 'commit' in actions:

                # Print status
                sysmsg.trace(f"⚙️  Processing view: '{view_name}' ...")

                # Fetch target table column names
                target_table_columns = self.db.get_column_names(engine_name='test', schema_name='graph_cache', table_name=target_table)

                # Remove row_id (if exists)
                if 'row_id' in target_table_columns:
                    target_table_columns.remove('row_id')

                # Build commit query                
                sql_query_commit = f"\tREPLACE INTO graph_cache.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"
                
                # Print commit query
                if 'print' in actions:
                    print(sql_query_commit)

                # Execute commit
                self.db.execute_query_in_shell(engine_name='test', query=sql_query_commit)

        # Update cache table from SQL formula
        def cache_update_from_formula(self, formula_name, verbose=False):

            # Print status
            sysmsg.trace(f"⚙️  Processing formula: '{formula_name}' ...")

            # Define the file path
            config_mysql_formula_path = global_config['mysql']['data_path']['formula']
            if os.path.isabs(config_mysql_formula_path):
                file_path = os.path.join(config_mysql_formula_path, f'formula.{formula_name}.sql')
            else:
                file_path = os.path.join(package_dir, config_mysql_formula_path, f'formula.{formula_name}.sql')

            # Read the SQL formula
            with open(file_path, 'r') as file:
                sql_formula = file.read()

            # Execute the SQL formula
            self.db.execute_query_in_shell(engine_name='test', query=sql_formula, verbose=verbose)

        # Update cached lecture timestamps
        def cache_lecture_timestamps(self):

            sql_query = f"""
          REPLACE INTO graph_cache.Edges_N_Lecture_N_Concept_T_Timestamps AS

                SELECT t2.from_institution_id    AS institution_id, 
                       t2.from_object_type       AS object_type, 
                       t2.from_object_id         AS object_id, 
                       t3.concept_id             AS concept_id,
                       MAX(t3.score)             AS detection_score, 
                       t4.field_value            AS detection_time_hms, 
                       t5.field_value            AS detection_timestamp
                       
                  FROM graph_airflow.Operations_N_Object_T_FieldsChanged t1

            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
                    ON (   t1.institution_id,    t1.object_type,    t1.object_id)
                     = (t2.to_institution_id, t2.to_object_type, t2.to_object_id)

            INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t3
                    ON (t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (     t3.institution_id,      t3.object_type,      t3.object_id)
                     
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t4
                    ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id, t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (t4.from_institution_id, t4.from_object_type, t4.from_object_id,  t4.to_institution_id,   t4.to_object_type,   t4.to_object_id)
                     
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t5
                    ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id, t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (t5.from_institution_id, t5.from_object_type, t5.from_object_id, t5.to_institution_id,   t5.to_object_type,   t5.to_object_id)
            
                 WHERE t1.object_type = 'Lecture'
                   AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
                   AND t3.object_type = 'Slide'
                   AND (t4.from_object_type, t4.to_object_type, t4.field_name) = ('Lecture', 'Slide', 'time_hms')
                   AND (t5.from_object_type, t5.to_object_type, t5.field_name) = ('Lecture', 'Slide', 'timestamp')
                   AND t1.to_process = 1

              GROUP BY t2.from_institution_id,
                       t2.from_object_type, 
                       t2.from_object_id, 
                       t3.concept_id, 
                       t5.field_value
            """

        # Core function that updates the object-to-object scores matrix
        # TODO: Widget-Concept/Catagory tables
        def calculate_scores_matrix(self, from_object_type, to_object_type, actions=()):

            # Check if from-to order should be reversed
            if (from_object_type, to_object_type) not in object_to_object_types_scoring_list:
                from_object_type, to_object_type = to_object_type, from_object_type

            # Initialise SQL queries
            sql_eval_query, sql_commit_query = None, None

            #---------------------------------------#
            # Ontology Category-to-Category scoring #
            #---------------------------------------#
            if (from_object_type, to_object_type) == ('Category', 'Category'):

                sql_query = f"""
                REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                            (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                        SELECT 'Ont' AS from_institution_id, 'Category' AS from_object_type, l1.from_id AS from_object_id,
                                'Ont' AS   to_institution_id, 'Category' AS   to_object_type, l2.from_id AS   to_object_id,
                                SUM(t.score) AS score, 1 AS to_process
                        FROM graph_ontology.Edges_N_Concept_N_Concept_T_Symmetric t
                STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild  c1 ON  t.from_id = c1.to_id
                STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild  c2 ON    t.to_id = c2.to_id
                STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l1 ON c1.from_id = l1.to_id
                STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l2 ON c2.from_id = l2.to_id
                        WHERE l1.from_id < l2.from_id
                    GROUP BY l1.from_id, l2.from_id
                        HAVING score >= 1;
                """ # ADD flags

            #--------------------------------------#
            # Ontology Category-to-Concept scoring #
            #--------------------------------------#
            elif (from_object_type, to_object_type) == ('Category', 'Concept'):

                sql_query = f"""
                REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                            (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                     SELECT 'Ont' AS from_institution_id, 'Category' AS from_object_type, object_id  AS from_object_id,
                            'Ont' AS   to_institution_id, 'Concept'  AS   to_object_type, concept_id AS   to_object_id,
                            score, 1 AS to_process
                       FROM graph_ontology.Edges_N_Object_N_Concept_T_CalculatedScores
                      WHERE (institution_id, object_type, calculation_type) = ('Ont', 'Category', 'concept sum-scores aggregation (bounded)')
                        AND score >= 0.1;
                """ # ADD flags

            #-------------------------------------#
            # Ontology Concept-to-Concept scoring #
            #-------------------------------------#
            elif (from_object_type, to_object_type) == ('Concept', 'Concept'):

                # Build evaluation query
                sql_eval_query = f"""
                    SELECT 'Concept' AS from_object_type, 'Concept' AS to_object_type, COUNT(*) AS n_to_process
                      FROM graph_ontology.Edges_N_Concept_N_Concept_T_Undirected
                     WHERE normalised_score >= 0.1
                """

                # Build commit/calculation query
                sql_commit_query = f"""
                REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                            (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                      SELECT 'Ont' AS from_institution_id, 'Concept' AS from_object_type, from_id AS from_object_id,
                             'Ont' AS   to_institution_id, 'Concept' AS   to_object_type,   to_id AS   to_object_id,
                             normalised_score AS score, 1 AS to_process
                        FROM graph_ontology.Edges_N_Concept_N_Concept_T_Undirected
                       WHERE normalised_score >= 0.1
                """ # ADD flags
            
            #--------------------------------------#
            # Ontology Concept-to-Category scoring #
            #--------------------------------------#
            elif to_object_type == ['Concept', 'Category']:
                pass

            #-----------------------------------#
            # Registry Object-to-Object scoring #
            #-----------------------------------#
            else:

                # Build evaluation query
                sql_eval_query = f"""
                    SELECT s1.object_type AS from_object_type, s2.object_type AS to_object_type, COUNT(*) AS n_to_process
                      FROM graph_airflow.Operations_N_Object_T_ScoresExpired s1
                
                INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired s2
                        ON (s1.institution_id, s1.object_type, s1.object_id)
                         = (s2.institution_id, s2.object_type, s2.object_id)
                
                INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                        ON (s1.institution_id, s1.object_type, s2.institution_id, s2.object_type)
                         = (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type)

                     WHERE s1.to_process = 1
                       AND tf.flag_type = 'scores'
                       AND tf.to_process = 1
                
                  GROUP BY s1.object_type, s2.object_type
                """
                
                # Build commit/calculation query
                sql_query_stack = []
                for n in [1,2]:
                    sql_query_stack += [f"""
                    REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                                (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                            
                          SELECT e1.institution_id  AS from_institution_id,
                                 e1.object_type     AS from_object_type,
                                 e1.object_id       AS from_object_id,
                                 e2.institution_id  AS to_institution_id,
                                 e2.object_type     AS to_object_type,
                                 e2.object_id       AS to_object_id,
                                 SUM(e1.score*e2.score) AS score, 1 AS to_process
                            
                            FROM graph_airflow.Operations_N_Object_T_ScoresExpired s1
                            
                      INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_FinalScores e1
                              ON (s1.institution_id, s1.object_type, s1.object_id)
                               = (e1.institution_id, e1.object_type, e1.object_id)
                            
                      INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_FinalScores e2 
                              ON e1.concept_id = e2.concept_id
                            
                      INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired s2
                              ON (s2.institution_id, s2.object_type, s2.object_id)
                               = (e2.institution_id, e2.object_type, e2.object_id)
                            
                      INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags tf
                              ON (e1.institution_id, e1.object_type, e2.institution_id, e2.object_type)
                               = (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type)
                            
                      INNER JOIN graph_registry.Nodes_N_Object n1
                              ON (e1.institution_id, e1.object_type, e1.object_id)
                               = (n1.institution_id, n1.object_type, n1.object_id)
                            
                      INNER JOIN graph_registry.Nodes_N_Object n2
                              ON (e2.institution_id, e2.object_type, e2.object_id)
                               = (n2.institution_id, n2.object_type, n2.object_id)
                            
                           WHERE ((e1.object_type = e2.object_type AND e1.object_id < e2.object_id) OR (e1.object_type != e2.object_type))
                            
                             AND e1.score >= 0.1
                             AND e2.score >= 0.1
                            
                             AND s{n}.to_process = 1
                             AND tf.flag_type = 'scores'
                             AND tf.to_process = 1
                            
                             AND e1.object_type = "{from_object_type if n==1 else to_object_type}"
                             AND e2.object_type = "{from_object_type if n==2 else to_object_type}"
                            
                        GROUP BY e1.object_id, e2.object_id
                            
                          HAVING COUNT(DISTINCT e1.concept_id) >= 4
                             AND SUM(e1.score*e2.score) >= 0.1
                    """]
                sql_commit_query = ';\n'.join(sql_query_stack)+';'
            
            # Evaluate query
            if 'eval' in actions and sql_eval_query is not None:

                # Print evaluation query
                if 'print' in actions:
                    print(sql_eval_query)

                # Execute evaluation query
                out = self.db.execute_query(engine_name='test', query=sql_eval_query)
                df = pd.DataFrame(out, columns=['from_object_type', 'to_object_type', 'n_to_process'])
                if len(df) > 0:
                    print_dataframe(df, title=f'🔍 Evaluation results for ({from_object_type}, {to_object_type})')

            # Commit query
            if 'commit' in actions and sql_commit_query is not None:

                # Print commit query
                if 'print' in actions:
                    print(sql_commit_query)

                # Execute commit query
                self.db.execute_query_in_shell(engine_name='test', query=sql_commit_query)
                print(f'✅ Successfully updated scores matrix for ({from_object_type}, {to_object_type})')

        # Core function that consolidates the object-to-object scores matrix (adjusted/bounded scores)
        def consolidate_scores_matrix(self, from_object_type=None, to_object_type=None, test_mode=False, verbose=False):



            if to_object_type in ['Concept', 'Category']:
                sql_query = f"""
                    REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                          SELECT institution_id, object_type, object_id, 'Ont', '{to_object_type}', {to_object_type.lower()}_id, score, 1 AS to_process
                            FROM graph_cache.Edges_N_Object_N_{to_object_type}_T_FinalScores fs
                      INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
						   USING (institution_id, object_type)
                      INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
						   USING (institution_id, object_type, object_id)
                           WHERE (institution_id, object_type) = ('EPFL', '{from_object_type}')
                             AND tf.to_process = 1 AND se.to_process = 1
                             AND score >= 0.1;
                """
            else:

                sql_query_check = f"""
                    SELECT * FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                    WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type)
                        = ('EPFL', '{from_object_type}', 'EPFL', '{to_object_type}');
                """
                out = self.db.execute_query(engine_name='test', query=sql_query_check)
                if len(out) == 0:
                    print(f'No average score calculation available for ({from_object_type}, {to_object_type})')
                    return

                sql_query = f"""
                    REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, score, to_process)
                          SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id,
                                 (2/(1 + EXP(-score/(4 * avg_score))) - 1) AS score, to_process
                            FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                       LEFT JOIN graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                           USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                           WHERE to_process = 1
                             AND (from_institution_id, from_object_type, to_institution_id, to_object_type)
                               = ('EPFL', '{from_object_type}', 'EPFL', '{to_object_type}')
                             AND (2/(1 + EXP(-score/(4 * avg_score))) - 1) >= 0.1;
                """
            if test_mode:
                print(sql_query)
            else:
                if verbose:
                    print(sql_query)
                self.db.execute_query_in_shell(engine_name='test', query=sql_query)

            pass
            
    #-----------------------------------------------------------#
    # Subclass definition: GraphIndex Management (SQL Database) #
    #-----------------------------------------------------------#
    class IndexDB():

        # Class constructor
        def __init__(self, engine_name='test'):
            self.db = GraphDB()
            self.engine_name = engine_name
            self.cachebuilder = self.CacheBuildup()
            self.pageprofile = self.PageProfile()
            self.idocs = {}
            self.idoclinks = {}
            self.list_of_index_tables = self.db.get_tables_in_schema(engine_name=self.engine_name, schema_name=f'graphsearch_{self.engine_name}')

            # Initialize IndexDoc objects for all doc types
            for doc_type in [t[0] for t in [re.findall(r'Index_D_([^_]*)$', table_name) for table_name in self.list_of_index_tables] if len(t)>0]:
                self.idocs[doc_type] = self.IndexDocs(doc_type=doc_type)

            # Initialize IndexDocLinks objects for all doc-link types
            for doc_type, link_type, link_subtype in [t[0] for t in [re.findall(r'Index_D_([^_]*)_L_([^_]*)_T_([^_]*)$', table_name) for table_name in self.list_of_index_tables] if len(t)>0]:
                if doc_type not in self.idoclinks:
                    self.idoclinks[doc_type] = {}
                if link_type not in self.idoclinks[doc_type]:
                    self.idoclinks[doc_type][link_type] = {}
                self.idoclinks[doc_type][link_type][link_subtype] = self.IndexDocLinks(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype)

        # Create mixed (org+sem) views for ElasticSearch indexing
        def create_mixed_views(self, drop_existing=False, test_mode=False):

            # Get the list of tables in graphsearch_test
            list_of_tables = self.db.get_tables_in_schema(engine_name='test', schema_name='graphsearch_test', use_regex=[r'.*_ORG'], include_views=False)

            # Loop over all tables
            for table_name_org in tqdm(list_of_tables):

                print(table_name_org)

                # Generate SEM and MIX table names
                table_name_sem = table_name_org.replace('_ORG', '_SEM')
                table_name_mix = table_name_org.replace('_ORG', '_MIX')

                # TODO: FIX THIS...
                if table_name_sem == 'Index_D_Lecture_L_Concept_T_SEM_Search':
                    continue

                # Check if view already exists
                if self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=table_name_mix) and not drop_existing:
                    continue

                # Check if SEM table exists
                if self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=table_name_sem):

                    # Get list of columns for SEM table
                    list_of_columns_sem = self.db.get_column_names(engine_name='test', schema_name='graphsearch_test', table_name=table_name_sem)

                    # Remove row_id
                    list_of_columns_sem.remove('row_id')

                    # Fix list of columns for ORG table
                    list_of_columns_org = ['degree_score' if c == 'semantic_score' else c for c in list_of_columns_sem]

                    # Generate SQL query
                    SQLQuery = f"""
                    CREATE OR REPLACE VIEW graphsearch_test.{table_name_mix} AS      

                                    SELECT {', '.join(list_of_columns_org)}, (s.row_rank) AS adjusted_row_rank
                                      FROM graphsearch_test.{table_name_org} s
                                INNER JOIN (SELECT doc_institution, doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                              FROM graphsearch_test.{table_name_org}
                                          GROUP BY doc_institution, doc_type, doc_id) o
                                     USING (doc_institution, doc_type, doc_id)

                                 UNION ALL

                                    SELECT {', '.join(list_of_columns_sem)}, (s.row_rank + COALESCE(o.max_row_rank, 0)) AS adjusted_row_rank
                                      FROM graphsearch_test.{table_name_sem} s
                                 LEFT JOIN (SELECT doc_institution, doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                              FROM graphsearch_test.{table_name_org}
                                          GROUP BY doc_institution, doc_type, doc_id) o
                                     USING (doc_institution, doc_type, doc_id)
                                     WHERE (s.doc_institution, s.doc_type, s.doc_id, s.link_institution, s.link_type, s.link_id)
                                    NOT IN (SELECT doc_institution, doc_type, doc_id, link_institution, link_type, link_id FROM graphsearch_test.{table_name_org})
                                        
                                  ORDER BY doc_id ASC, adjusted_row_rank ASC;
                    """

                else:

                    # Get list of columns for ORG table
                    list_of_columns_org = self.db.get_column_names(engine_name='test', schema_name='graphsearch_test', table_name=table_name_org)

                    # Remove row_id
                    list_of_columns_org.remove('row_id')

                    # Generate SQL query
                    SQLQuery = f"""
                    CREATE OR REPLACE VIEW graphsearch_test.{table_name_mix} AS      

                                    SELECT {', '.join(list_of_columns_org)}, (s.row_rank) AS adjusted_row_rank
                                      FROM graphsearch_test.{table_name_org} s
                                INNER JOIN (SELECT doc_institution, doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                              FROM graphsearch_test.{table_name_org}
                                          GROUP BY doc_institution, doc_type, doc_id) o
                                     USING (doc_institution, doc_type, doc_id)
                    """

                if test_mode:
                    print(SQLQuery)
                else:
                    self.db.execute_query_in_shell(engine_name='test', query=SQLQuery)

            pass
        
        # Copy patched data to production cache schema
        def copy_patches_to_prod(self):

            list_of_table = self.db.get_tables_in_schema(
                engine_name   = 'test',
                schema_name   = 'graph_cache',
                include_views = False,
                use_regex     = [r'^IndexBuildup_Fields_Docs_[^_]*', r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*']
            )

            list_of_table += ['Data_N_Object_T_PageProfile', 'Edges_N_Object_N_Object_T_ParentChildSymmetric', 'Edges_N_Object_N_Object_T_ScoresMatrix_AS']

            for table_name in list_of_table:

                table_type = get_table_type_from_name(table_name)
                
                self.db.copy_table_across_engines(
                    source_engine_name = 'test',
                    source_schema_name = 'graph_cache',
                    source_table_name  = table_name,
                    target_engine_name = 'prod',
                    target_schema_name = 'graph_cache',
                    keys_json  = table_keys_json[table_type],
                    filter_by  = 'to_process > 0.5',
                    chunk_size = 100000,
                    drop_table = True
                )

        # Delete loose ends in index tables
        def delete_loose_ends(self):

            sql_template_docs = """
              %s
              FROM graphsearch_test.Index_D_%s;
            """

            sql_template_doclinks = """
              %s
              FROM graphsearch_test.Index_D_%s_L_%s_T_%s;
            """
            
            for dmy,doc_type in index_doc_types_list:

                # Delete loose ends in doc fields
                sql_query = sql_template_docs % (
                    f'SELECT "{doc_type}", COUNT(*) AS n_total',
                    doc_type
                )
                print(sql_query)
                

                for dmy,link_type in index_doc_types_list:

                    for link_subtype in ['SEM','ORG']:

                        if not self.db.table_exists(
                            engine_name   = 'test',
                            schema_name   = 'graphsearch_test',
                            table_name    = f'Index_D_{doc_type}_L_{link_type}_T_{link_subtype}'
                        ):
                            continue

                        sql_query = sql_template_doclinks % (
                            f'SELECT "{doc_type}-{link_type}", COUNT(*) AS n_total',
                            doc_type,
                            link_type,
                            link_subtype
                        )
                        print(sql_query)


            return


            sql_template_docs = """
              %s
              FROM graphsearch_test.Index_D_%s
             WHERE doc_id NOT IN (SELECT object_id FROM graphsearch_test.Data_N_Object_T_PageProfile WHERE object_type='%s');
            """

            sql_template_doclinks = """
              %s
              FROM graphsearch_test.Index_D_%s_L_%s_T_%s
             WHERE doc_id  NOT IN (SELECT object_id FROM graphsearch_test.Data_N_Object_T_PageProfile WHERE object_type='%s')
                OR link_id NOT IN (SELECT object_id FROM graphsearch_test.Data_N_Object_T_PageProfile WHERE object_type='%s');
            """
            
            for dmy,doc_type in index_doc_types_list:

                # Delete loose ends in doc fields
                sql_query = sql_template_docs % (
                    f'SELECT "{doc_type}", COUNT(*) AS n_to_delete',
                    doc_type,
                    doc_type
                )
                print(sql_query)
                

                for dmy,link_type in index_doc_types_list:

                    for link_subtype in ['SEM','ORG']:

                        if not self.db.table_exists(
                            engine_name   = 'test',
                            schema_name   = 'graphsearch_test',
                            table_name    = f'Index_D_{doc_type}_L_{link_type}_T_{link_subtype}'
                        ):
                            continue

                        sql_query = sql_template_doclinks % (
                            f'SELECT "{doc_type}-{link_type}", COUNT(*) AS n_to_delete',
                            doc_type,
                            link_type,
                            link_subtype,
                            doc_type,
                            link_type
                        )
                        print(sql_query)

            pass

        #-----------------------------------------------------#
        # Sub-subclass definition: Index Cache Buildup Tables #
        #-----------------------------------------------------#
        class CacheBuildup():

            # Class constructor
            def __init__(self):
                self.db = GraphDB()

            # info
            def info(self):
                list_of_tables = self.db.get_tables_in_schema(
                    engine_name   = 'test',
                    schema_name   = 'graph_cache',
                    include_views = False,
                    filter_by     = False,
                    use_regex     = [r'^IndexBuildup_Fields_Docs_[^_]*', r'^IndexBuildup_Fields_Links_ParentChild_[^_]*_[^_]*']
                )
                print('\nList of index buildup tables:')
                print(' - '+'\n - '.join(sorted(list_of_tables)))

            # Update index buildup tables: doc fields
            def build_docs_fields(self, doc_type, actions=()):
                
                # Fetch doc options
                include_code_in_name = index_config['options']['docs'][doc_type]['include_code_in_name']

                # Fetch object custom fields
                obj_fields = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['docs'][doc_type]]

                # Modify according to field languages
                obj_fields_with_lang = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in obj_fields]

                # Generate SQL slices to replace placeholders
                sql_slice_field_names           = ', '.join(obj_fields_with_lang)
                sql_slice_field_values_as_names = ', '.join([f"t{k+1}.field_value AS {field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for k, (field_language, field_name) in enumerate(obj_fields)])
                sql_slice_joins_doc             = '\n'.join([f"{' '*6}LEFT JOIN graph_cache.Data_N_Object_T_AllFields t{k+1} ON (t{k+1}.institution_id, t{k+1}.object_type, t{k+1}.object_id, t{k+1}.field_language, t{k+1}.field_name) = ('EPFL', '{doc_type}', p.object_id, '{field_language}', '{field_name}')" for k, (field_language, field_name) in enumerate(obj_fields)])
                
                # Add trailing comma if necessary
                if len(sql_slice_field_names) > 0:
                    sql_slice_field_names += ', '
                if len(sql_slice_field_values_as_names) > 0:
                    sql_slice_field_values_as_names += ', '

                # Generate SQL query for replacing scores and fields
                sql_query = f"""
                    SELECT p.institution_id AS doc_institution, p.object_type AS doc_type, p.object_id AS doc_id,
                           {include_code_in_name} AS include_code_in_name,
                           {sql_slice_field_values_as_names}
                           COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score,
                           1 AS to_process
                      FROM graph_cache.Data_N_Object_T_PageProfile p\n{sql_slice_joins_doc}
                 LEFT JOIN graph_cache.Nodes_N_Object_T_DegreeScores d
                        ON (p.institution_id, p.object_type, p.object_id)
                         = (d.institution_id, d.object_type, d.object_id)
                     WHERE (p.institution_id, p.object_type) = ('EPFL', '{doc_type}')
                       AND p.to_process > 0.5
                """

                # Target cache table
                target_table = f'IndexBuildup_Fields_Docs_{doc_type}'

                # List of evaluation columns
                eval_columns = ['doc_institution', 'doc_type']

                #-------------------------#
                # Process resulting query #
                #-------------------------#

                # Evaluate query
                if 'eval' in actions:
                    sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"

                    # Print query
                    if 'print' in actions:
                        print(sql_query_eval, '\n')

                    # Execute evaluation query
                    out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                    df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                    if len(df) > 0:
                        print_dataframe(df, title=f'🔍 Evaluation results for doc type: "{doc_type}"')

                # Execute commit
                if 'commit' in actions:

                    # Fetch target table column names
                    target_table_columns = ['doc_institution', 'doc_type', 'doc_id', 'include_code_in_name'] + obj_fields_with_lang + ['degree_score', 'to_process']

                    # Remove row_id (if exists)
                    if 'row_id' in target_table_columns:
                        target_table_columns.remove('row_id')

                    # Build commit query                
                    sql_query_commit = f"\tREPLACE INTO graph_cache.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"

                    # Print query
                    if 'print' in actions:
                        print(sql_query_commit)

                    # Execute commit
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query_commit)

            # Update index buildup tables: link parent-child type
            def build_links_parentchild(self, doc_type, link_type, actions=()):

                # Fetch object custom fields
                obj2obj_fields = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['links']['parent-child'][doc_type][link_type]['obj2obj']]

                # Modify according to field languages
                obj2obj_fields_with_lang = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in obj2obj_fields]

                # Generate SQL slices to replace placeholders
                sql_slice_field_names           = ', '.join(obj2obj_fields_with_lang)
                sql_slice_field_values_as_names = ', '.join([f"t{k+1}.field_value AS {field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for k, (field_language, field_name) in enumerate(obj2obj_fields)])
                sql_slice_joins_obj2obj         = '\n'.join([f"{' '*6}LEFT JOIN graph_cache.Data_N_Object_N_Object_T_AllFieldsSymmetric t{k+1} ON (t{k+1}.from_institution_id, t{k+1}.from_object_type, t{k+1}.from_object_id, t{k+1}.to_institution_id, t{k+1}.to_object_type, t{k+1}.to_object_id, t{k+1}.field_language, t{k+1}.field_name) = ('EPFL', '{doc_type}', s.from_object_id, 'EPFL', '{link_type}',   s.to_object_id, '{field_language}', '{field_name}')" for k, (field_language, field_name) in enumerate(obj2obj_fields)])

                # Add trailing comma if necessary
                if len(sql_slice_field_names) > 0:
                    sql_slice_field_names += ', '

                # Generate SQL query for replacing scores and fields
                sql_query = f"""
                  SELECT s.from_institution_id AS doc_institution, s.from_object_type AS  doc_type, s.from_object_id AS doc_id,
                         s.to_institution_id AS link_institution, s.to_object_type AS link_type, s.to_object_id AS link_id,
                         {sql_slice_field_values_as_names},
                         1 AS to_process
                    FROM graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric s\n{sql_slice_joins_obj2obj}
                   WHERE (s.from_institution_id, s.from_object_type, s.to_institution_id, s.to_object_type) = ('EPFL', '{doc_type}', 'EPFL', '{link_type}')
                     AND s.to_process > 0.5
                """

                # Target cache table
                target_table = f'IndexBuildup_Fields_Links_ParentChild_{doc_type}_{link_type}'

                # List of evaluation columns
                eval_columns = ['doc_institution', 'doc_type', 'link_institution', 'link_type']

                #-------------------------#
                # Process resulting query #
                #-------------------------#

                # Print query
                if 'print' in actions:
                    print(sql_query)

                # Evaluate query
                if 'eval' in actions:
                    sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"
                    out = self.db.execute_query(engine_name='test', query=sql_query_eval)
                    df = pd.DataFrame(out, columns=eval_columns+['n_to_process'])
                    if len(df) > 0:
                        print_dataframe(df, title=f'🔍 Evaluation results for doc-link type: "{doc_type}-{link_type}"')

                # Execute commit
                if 'commit' in actions:

                    # Fetch target table column names
                    target_table_columns = ['doc_institution', 'doc_type', 'doc_id', 'link_institution', 'link_type', 'link_id'] + obj2obj_fields_with_lang + ['to_process']

                    # Remove row_id (if exists)
                    if 'row_id' in target_table_columns:
                        target_table_columns.remove('row_id')

                    # Build commit query                
                    sql_query_commit = f"\tREPLACE INTO graph_cache.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"

                    # Execute commit
                    self.db.execute_query_in_shell(engine_name='test', query=sql_query_commit)

            # Update index buildup tables (all)
            def build_all(self, actions=()):

                # Loop over the doc types
                for inst_id, doc_type in tqdm(index_doc_types_list):
                    self.build_docs_fields(doc_type, actions)

                # Loop over the parent-child link types
                # TODO: get the "[('Course', 'Person'), ('Person', 'Unit')]" from config file
                for doc_type, link_type in tqdm([('Course', 'Person'), ('Person', 'Unit')]):
                    self.build_links_parentchild(doc_type, link_type, actions)

        #----------------------------------------------#
        # Sub-subclass definition: Page Profiles Table #
        #----------------------------------------------#
        class PageProfile():

            # Class constructor
            def __init__(self, engine_name='test'):

                # Assign DB pointer
                self.db = GraphDB()

                # Define internal variables
                self.engine_name      = engine_name
                self.table_name       = 'Data_N_Object_T_PageProfile'
                self.key_column_names = ['institution_id', 'object_type', 'object_id']

                # Fetch column names to update
                out = self.db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = mysql_schema_names[self.engine_name]['cache'],
                    table_name  = self.table_name
                )
                self.upd_column_names = [c for c in out if c not in self.key_column_names+['row_id', 'to_process']]

            # ...
            def info(self):
                out = self.db.execute_query(engine_name='test', query=f"""
                    SELECT institution_id, object_type, COUNT(*) AS n_to_process
                    FROM graph_cache.{self.table_name}
                    WHERE to_process > 0.5
                    GROUP BY institution_id, object_type
                """)
                df = pd.DataFrame(out, columns=['institution_id', 'object_type', 'n_to_process'])
                print_dataframe(df, title=f'🔍 Evaluation results for page profile')
                pass

            # Index > Page Profile > Get engine
            def get_engine(self):
                return self.engine_name

            # Index > Page Profile > Set engine
            def set_engine(self, engine_name):
                self.engine_name = engine_name

            # Index > Page Profile > Create table on selected engine
            def create_table(self, actions=()):
                
                return
                sql_query_create_table = f"""
                CREATE TABLE IF NOT EXISTS {mysql_schema_names[self.engine_name]['ui']}.{self.table_name} (
                    row_id int NOT NULL AUTO_INCREMENT,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.key_column_names])},
                    {', '.join([f'{c} VARCHAR(1)' for c in self.upd_column_names])},
                    UNIQUE KEY row_id (row_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """

                # Get table type
                table_type = get_table_type_from_name(self.table_name)

                # Get datatypes
                datatypes_json = table_datatypes_json[table_type]
                datatypes_json.update(index_config['data-types'])

                # Get keys
                keys_json = table_keys_json[table_type]
                keys_json.update(index_config['data-keys'])

                if 'print' in actions:
                    print(sql_query_create_table)
                    rich.print_json(data=datatypes_json)
                    rich.print_json(data=keys_json)

                if 'commit' in actions:
                    self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_create_table)
                    self.db.apply_datatypes(engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, datatypes_json=datatypes_json)
                    self.db.apply_keys(     engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, keys_json=keys_json)

            #==================#
            # General patching #
            #==================#

            # Index > Page Profile > General patching > Generate snapshot
            def snapshot(self, rollback_date=False, actions=()):

                return
                # # Generate SQL query
                # SQLQuery = f"""
                # INSERT IGNORE INTO graph_cache.IndexRollback_PageProfile
                #                 (rollback_date, {', '.join(column_names)})
                # SELECT DISTINCT '{rollback_date}' AS rollback_date, p.{', p.'.join(column_names)}
                #             FROM graphsearch_test.Data_N_Object_T_PageProfile p
                #         INNER JOIN graph_cache.Data_N_Object_T_PageProfile c
                #             USING (institution_id, object_type, object_id)
                #             WHERE c.to_process = 1
                #             AND ({' OR '.join([f'p.{c} != c.{c}' for c in column_names])});
                # """

            # Index > Page Profile > General patching > Insert new rows, update existing fields (graphsearch_test)
            def patch(self, actions=()):

                # Generate SQL query
                sql_query = f"""
                    \t\tSELECT {', '.join(self.key_column_names)}{', ' if len(self.upd_column_names)>0 else ''}{', '.join(self.upd_column_names)}
                    \t\tFROM {mysql_schema_names[self.engine_name]['cache']}.{self.table_name} WHERE to_process > 0.5
                """

                # Execute query
                self.db.execute_query_as_safe_inserts(
                    engine_name       = self.engine_name,
                    schema_name       = mysql_schema_names[self.engine_name]['ui'],
                    table_name        = self.table_name,
                    query             = sql_query,
                    key_column_names  = self.key_column_names,
                    upd_column_names  = self.upd_column_names,
                    eval_column_names = ['institution_id', 'object_type'],
                    actions           = actions
                )

            # Index > Page Profile > General patching > Roll back to previous state
            def rollback(self, actions=()):
                pass

        #-------------------------------------------#
        # Sub-subclass definition: Index Docs Table #
        #-------------------------------------------#
        class IndexDocs():

            # Class constructor
            def __init__(self, doc_type, engine_name='test'):

                # Assign DB pointer
                self.db = GraphDB()

                # Define internal variables
                self.engine_name        = engine_name
                self.doc_type           = doc_type
                self.buildup_table_name = f'IndexBuildup_Fields_Docs_{doc_type}'
                self.index_table_name   = f'Index_D_{doc_type}'
                self.key_column_names   = ['doc_institution', 'doc_type', 'doc_id']

                # Fetch column names to update
                out = self.db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = mysql_schema_names[self.engine_name]['cache'],
                    table_name  = self.buildup_table_name
                )
                self.upd_column_names = [c for c in out if c not in self.key_column_names+['row_id', 'to_process']]

                # Fetch doc fields
                self.obj_fields = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['docs'][doc_type]]
                self.obj_fields_with_lang = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in self.obj_fields]
                
            # Index > Docs > Table info
            def info(self):
                print('\nSelected table:', self.index_table_name)
                out = self.db.get_column_names(
                    engine_name = self.engine_name,
                    schema_name = mysql_schema_names[self.engine_name]['ui'],
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

            # Index > Docs > Create table on graphsearch_test
            def create_table(self, actions=()):
                
                sql_query_create_table = f"""
                CREATE TABLE IF NOT EXISTS {mysql_schema_names[self.engine_name]['ui']}.{self.index_table_name} (
                    row_id int NOT NULL AUTO_INCREMENT,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.key_column_names])},
                    include_code_in_name tinyint(1) NOT NULL,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                    degree_score float NOT NULL,
                    UNIQUE KEY row_id (row_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """

                # Get table type
                table_type = get_table_type_from_name(self.index_table_name)

                # Get datatypes
                datatypes_json = table_datatypes_json[table_type]
                datatypes_json.update(index_config['data-types'])

                # Get keys
                keys_json = table_keys_json[table_type]
                keys_json.update(index_config['data-keys'])

                if 'print' in actions:
                    print(sql_query_create_table)
                    rich.print_json(data=datatypes_json)
                    rich.print_json(data=keys_json)

                # if 'commit' in actions:
                #     self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_create_table)
                #     self.db.apply_datatypes(engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, datatypes_json=datatypes_json)
                #     self.db.apply_keys(     engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, keys_json=keys_json)

            # Index > Docs > Create table on elasticsearch_cache
            def create_table_elasticsearch(self, actions=()):
                
                sql_query_create_table = f"""
                CREATE TABLE IF NOT EXISTS elasticsearch_cache.Index_D_{self.doc_type} (
                    doc_type ENUM('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
                    doc_id VARCHAR(255) NOT NULL,
                    degree_score FLOAT NOT NULL,
                    short_code VARCHAR(32)  DEFAULT NULL,
                    subtype_en VARCHAR(255) DEFAULT NULL,
                    subtype_fr VARCHAR(255) DEFAULT NULL,
                    name_en MEDIUMTEXT,
                    name_fr MEDIUMTEXT,
                    short_description_en MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    short_description_fr MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    long_description_en  MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    long_description_fr  MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                    PRIMARY KEY     (doc_type, doc_id),
                    UNIQUE  KEY uid (doc_type, doc_id),
                    KEY doc_type (doc_type),
                    KEY doc_id   (doc_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """

                # Get table type
                table_type = get_table_type_from_name(f'Index_D_{self.doc_type}')

                # Get datatypes
                datatypes_json = table_datatypes_json[table_type]
                datatypes_json.update(index_config['data-types'])

                if 'print' in actions:
                    print(sql_query_create_table)
                    rich.print_json(data=datatypes_json)

                # if 'commit' in actions:
                #     self.db.execute_query_in_shell(engine_name='test', query=sql_query_create_table)
                #     self.db.apply_datatypes(engine_name='test', schema_name='elasticsearch_cache', table_name=f'Index_D_{self.doc_type}', datatypes_json=datatypes_json)

            #==================#
            # General patching #
            #==================#

            # Index > Docs > General patching > Generate snapshot
            def snapshot(self, rollback_date=False, actions=()):
                
                # Generate SQL query
                SQLQuery = f"""
                INSERT IGNORE INTO graph_cache.IndexRollback_Fields_Docs_{self.doc_type}
                                (rollback_date, doc_institution, doc_type, doc_id, include_code_in_name, {', '.join(self.custom_column_names_with_lang)}{',' if len(self.custom_column_names_with_lang)>0 else ''} degree_score)
                SELECT DISTINCT '{rollback_date}' AS rollback_date, i.doc_institution, i.doc_type, i.doc_id, i.include_code_in_name, {', '.join([f'i.{c}' for c in self.custom_column_names_with_lang])}{',' if len(self.custom_column_names_with_lang)>0 else ''} i.degree_score
                            FROM graphsearch_test.Index_D_{self.doc_type} i
                        INNER JOIN graph_cache.IndexBuildup_Fields_Docs_{self.doc_type} b
                            USING (doc_institution, doc_type, doc_id)
                            WHERE b.to_process > 0.5
                            AND ({' OR '.join([f'i.{c} != b.{c}' for c in self.custom_column_names_with_lang])} {'OR' if len(self.custom_column_names_with_lang)>0 else ''} i.degree_score != b.degree_score);
                """

            # Index > Docs > General patching > Insert new rows, update existing fields (graphsearch_test)
            def patch(self, actions=()):

                # Generate SQL query
                sql_query = f"""
                    \t\tSELECT {', '.join(self.key_column_names)}{', ' if len(self.upd_column_names)>0 else ''}{', '.join(self.upd_column_names)}
                    \t\tFROM {mysql_schema_names[self.engine_name]['cache']}.{self.buildup_table_name} WHERE to_process > 0.5
                """

                # Execute query
                self.db.execute_query_as_safe_inserts(
                    engine_name       = self.engine_name,
                    schema_name       = mysql_schema_names[self.engine_name]['ui'],
                    table_name        = self.index_table_name,
                    query             = sql_query,
                    key_column_names  = self.key_column_names,
                    upd_column_names  = self.upd_column_names,
                    eval_column_names = ['doc_institution', 'doc_type'],
                    actions           = actions
                )

            # Index > Docs > General patching > Insert new rows, update existing fields (elascticsearch_cache)
            def patch_elasticsearch(self, actions=()):

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
                ] + self.obj_fields_with_lang

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
                ] + [f'n.{c}' for c in self.obj_fields_with_lang]

                # Generate SQL query
                sql_query = f"""
                     SELECT n.doc_type, n.doc_id, {', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])}
                       FROM graph_cache.Data_N_Object_T_PageProfile p
                 INNER JOIN graph_cache.IndexBuildup_Fields_Docs_{self.doc_type} n
                         ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                      WHERE p.object_type = '{self.doc_type}'
                        AND (p.to_process > 0.5 OR n.to_process > 0.5)
                """

                # Execute the patch operation
                self.db.execute_query_as_safe_inserts(
                    schema_name       = 'elasticsearch_cache',
                    table_name        = self.index_table_name,
                    query             = sql_query,
                    key_column_names  = ['doc_type', 'doc_id'],
                    upd_column_names  = upd_column_names,
                    eval_column_names = ['doc_type'],
                    actions           = actions
                )

            # Index > Docs > General patching > Roll back to previous state
            def rollback(self, rollback_date, actions=()):

                # Generate SQL query
                sql_query = f"""
                        UPDATE graphsearch_test.Index_D_{self.doc_type} i
                    INNER JOIN graph_cache.IndexRollback_Fields_Docs_{self.doc_type} b
                        USING (doc_institution, doc_type, doc_id)
                            SET {', '.join([f'i.{c} = b.{c}' for c in self.obj_fields_with_lang])}, i.degree_score = b.degree_score
                        WHERE b.rollback_date = '{rollback_date}';
                """

        #------------------------------------------------#
        # Sub-subclass definition: Index Doc-Links Table #
        #------------------------------------------------#
        class IndexDocLinks():

            # Class constructor
            def __init__(self, doc_type, link_type, link_subtype, engine_name='test'):

                # Assign DB pointer
                self.db = GraphDB()

                # Define internal variables
                self.doc_type     = doc_type
                self.link_type    = link_type
                self.link_subtype = link_subtype
                self.engine_name  = engine_name
                self.buildup_doc_table_name  = f'IndexBuildup_Fields_Docs_{doc_type}'
                self.buildup_link_table_name = f'IndexBuildup_Fields_Docs_{link_type}'
                self.index_table_name        = f'Index_D_{doc_type}_L_{link_type}_T_{link_subtype.upper()}'
                self.key_column_names        = ['doc_institution', 'doc_type', 'doc_id', 'link_institution', 'link_type', 'link_subtype', 'link_id']

                # Fetch doc fields
                self.obj_fields = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['docs'][self.link_type]]
                self.obj_fields_with_lang = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in self.obj_fields]
                self.obj_fields_to_update = len(self.obj_fields_with_lang) > 0
                self.obj2obj_fields, self.obj2obj_fields_with_lang = [], []
                self.es_filters = []

                # Check if there are fields to update
                if not self.obj_fields_to_update:
                    return

                # TODO: fix this
                if self.link_type == 'Lecture':
                    self.obj_fields_with_lang = ["video_stream_url", "video_duration", "is_restricted"]

                # Fetch doc fields
                if link_subtype.upper() == 'ORG':

                    # rich.print_json(data=index_config['fields']['links']['parent-child'])
                    if self.doc_type in index_config['fields']['links']['parent-child']:
                        self.obj2obj_fields = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['links']['parent-child'][self.doc_type][self.link_type]['obj2obj']]
                        self.obj2obj_fields_with_lang = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in self.obj2obj_fields]

                    # TODO Fix this / get from config
                    if (self.doc_type, self.link_type) in [('Person', 'Unit'), ('Unit', 'Person')]:
                        self.obj2obj_fields_with_lang += ['is_active_affiliation', 'current_position_rank']
                        
                    if (self.doc_type, self.link_type) in [('Person', 'Course'), ('Course', 'Person')]:
                        self.obj2obj_fields_with_lang += ['latest_teaching_assignment_year']

                # Fetch filters
                if self.link_type in  index_config['es-filters']['links']:
                    self.es_filters = index_config['es-filters']['links'][self.link_type]
                    
            # Index > Doc-Links > Table info
            def info(self):

                print('\nSelected table:', self.index_table_name)
                out = self.db.get_column_names(
                    engine_name = 'test',
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

            # Index > Doc-Links > Create table on graphsearch_test
            def create_table(self, actions=()):

                sql_query_create_table = f"""
                CREATE TABLE IF NOT EXISTS {mysql_schema_names[self.engine_name]['ui']}.{self.index_table_name} (
                    row_id int NOT NULL AUTO_INCREMENT,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.key_column_names])},
                    {', '.join([f'{c} VARCHAR(1)' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                    {', '.join([f'{c} VARCHAR(1)' for c in self.obj2obj_fields_with_lang])}{',' if len(self.obj2obj_fields_with_lang)>0 else ''}
                    {'semantic_score' if self.link_subtype.upper() == 'SEM' else 'degree_score'} FLOAT NOT NULL,
                    row_score FLOAT NOT NULL,
                    row_rank SMALLINT unsigned NOT NULL,
                    UNIQUE KEY row_id (row_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """

                # Get table type
                table_type = get_table_type_from_name(self.index_table_name)

                # Get datatypes
                datatypes_json = table_datatypes_json[table_type]
                datatypes_json.update(index_config['data-types'])

                # Get keys
                keys_json = table_keys_json[table_type]
                keys_json.update(index_config['data-keys'])

                if 'print' in actions:
                    print(sql_query_create_table)
                    rich.print_json(data=datatypes_json)
                    rich.print_json(data=keys_json)

                # if 'commit' in actions:
                #     self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_create_table)
                #     self.db.apply_datatypes(engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, datatypes_json=datatypes_json)
                #     self.db.apply_keys(     engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['ui'], table_name=self.index_table_name, keys_json=keys_json)

            # Index > Doc-Links > Create table on elasticsearch_cache
            def create_table_elasticsearch(self, actions=()):
                
                sql_query_create_table = f"""
                CREATE TABLE IF NOT EXISTS elasticsearch_cache.Index_D_{self.doc_type}_L_{self.link_type} (
                    doc_type       ENUM('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
                    doc_id         VARCHAR(255) NOT NULL,
                    link_type      ENUM('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
                    link_subtype   ENUM('Parent-to-Child','Child-to-Parent','Semantic') NOT NULL,
                    link_id        VARCHAR(255) NOT NULL,
                    link_rank      SMALLINT UNSIGNED NOT NULL,
                    link_name_en   MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    link_name_fr   MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    link_short_description_en MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    link_short_description_fr MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    {', '.join([f'{c} VARCHAR(1)' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                    PRIMARY KEY      (doc_type, doc_id, link_type, link_subtype, link_id),
                    UNIQUE  KEY uid  (doc_type, doc_id, link_type, link_subtype, link_id),
                    KEY doc_type     (doc_type),
                    KEY doc_id       (doc_id),
                    KEY link_type    (link_type),
                    KEY link_subtype (link_subtype),
                    KEY link_id      (link_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """

                # Get table type
                table_type = get_table_type_from_name(f'elasticsearch_cache.Index_D_{self.doc_type}_L_{self.link_type}')

                # Get datatypes
                datatypes_json = table_datatypes_json[table_type]
                datatypes_json.update(index_config['data-types'])

                if 'print' in actions:
                    print(sql_query_create_table)
                    rich.print_json(data=datatypes_json)

                # if 'commit' in actions:
                #     self.db.execute_query_in_shell(engine_name='test', query=sql_query_create_table)
                #     self.db.apply_datatypes(engine_name='test', schema_name='elasticsearch_cache', table_name=f'Index_D_{self.doc_type}_L_{self.link_type}', datatypes_json=datatypes_json)

            #===================#
            # Vertical patching #
            #===================#

            # ------- Snapshots ------- #

            # Index > Doc-Links > Vertical patching > Generate snapshot
            def vertical_snapshot_parentchild(self, rollback_date=False):

                # Generate the SQL query
                sql_query = f"""
                INSERT IGNORE INTO graph_cache.IndexRollback_Fields_Links_ParentChild_{self.doc_type}_{self.link_type}
                                (rollback_date, doc_institution, doc_type, doc_id, link_institution, link_type, link_id, {', '.join(self.obj2obj_fields_with_lang)})

                            SELECT '{rollback_date}' AS rollback_date,
                                t1.doc_institution, t1.doc_type, t1.doc_id, t1.link_institution, t1.link_type, t1.link_id,
                                {', '.join([f'COALESCE(t1.{c}, t2.{c}) AS {c}' for c in self.obj2obj_fields_with_lang])}
                
                            FROM (SELECT DISTINCT i.doc_institution, i.doc_type, i.doc_id,
                                                    i.link_institution, i.link_type, i.link_id,
                                                    {', '.join([f'i.{c}' for c in self.obj2obj_fields_with_lang])}
                                            FROM graphsearch_test.Index_D_{self.doc_type}_L_{self.link_type}_T_ORG i
                                        INNER JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b    
                                                ON (i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_id)
                                                = (b.doc_institution, b.doc_type, b.doc_id, b.link_institution, b.link_type, b.link_id)
                                            WHERE b.to_process > 0.5
                                                AND ({' OR '.join([f'i.{c} != b.{c}' for c in self.obj2obj_fields_with_lang])})
                                ) t1
                        INNER JOIN (SELECT DISTINCT i.link_institution AS doc_institution, i.link_type AS doc_type, i.link_id AS doc_id,
                                                    i.doc_institution AS link_institution, i.doc_type AS link_type, i.doc_id AS link_id,
                                                    {', '.join([f'i.{c}' for c in self.obj2obj_fields_with_lang])}
                                            FROM graphsearch_test.Index_D_{self.link_type}_L_{self.doc_type}_T_ORG i
                                        INNER JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b    
                                                ON (i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_id)
                                                = (b.link_institution, b.link_type, b.link_id, b.doc_institution, b.doc_type, b.doc_id)
                                            WHERE b.to_process > 0.5
                                                AND ({' OR '.join([f'i.{c} != b.{c}' for c in self.obj2obj_fields_with_lang])})
                                ) t2
                            USING (doc_institution, doc_type, doc_id, link_institution, link_type, link_id);
                """

            # ------- Patching ------- #

            # Index > Doc-Links > Vertical patching > Update custom fields (all types)
            def vertical_patch(self, actions=()):

                # Check if there are fields to update
                if len(self.obj_fields_with_lang) == 0:
                    print(f"No fields to update for link_type '{self.link_type}'.")
                    return

                # Full table paths
                buildup_link_table_path = f"{mysql_schema_names[self.engine_name]['cache']}.{self.buildup_link_table_name}"
                target_schema_name      = mysql_schema_names[self.engine_name]['ui']
                target_table_name       = self.index_table_name
                target_table_path       = f"{target_schema_name}.{target_table_name}"

                # Check if target table exists
                if not self.db.table_exists(
                    engine_name = self.engine_name,
                    schema_name = mysql_schema_names[self.engine_name]['ui'],
                    table_name  = self.index_table_name
                ):
                    print(f"Table {self.index_table_name} does not exist.")
                    return
                
                # Evaluate the patch operation
                if 'eval' in actions:

                    # Generate evaluation query
                    sql_query_eval = f"""
                        SELECT COUNT(*) AS n_total, COALESCE(SUM({' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.obj_fields_with_lang])}), 0) AS n_patch
                          FROM {target_table_path} i
                    INNER JOIN {buildup_link_table_path} b
                            ON (i.link_institution, i.link_type, i.link_id) = (b.doc_institution, b.doc_type, b.doc_id)
                         WHERE b.to_process > 0.5;
                    """

                    # Print the evaluation query
                    if 'print' in actions:
                        print(sql_query_eval)

                    # Execute the evaluation query and print the results
                    out = self.db.execute_query(engine_name=self.engine_name, query=sql_query_eval)
                    df = pd.DataFrame(out, columns=['rows to process', 'rows to patch'])
                    print_dataframe(df, title=f'🔍 Evaluation results for {target_table_path}:')

                # Generate SQL query
                sql_query_commit = f"""
                        UPDATE {target_table_path} i
                    INNER JOIN {buildup_link_table_path} b
                            ON (i.link_institution, i.link_type, i.link_id) = (b.doc_institution, b.doc_type, b.doc_id)
                           SET {   ', '.join([f'i.{c}  = b.{c}' for c in self.obj_fields_with_lang])}
                         WHERE b.to_process > 0.5
                           AND ({' OR '.join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in self.obj_fields_with_lang])});
                """

                # Print the commit query
                if 'print' in actions:
                    print(sql_query_commit)

                # Execute the commit query
                if 'commit' in actions:
                    # self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query_commit)
                    self.db.execute_query_in_chunks(engine_name=self.engine_name, schema_name=target_schema_name, table_name=target_table_name, query=sql_query_commit, chunk_size=10000, row_id_name='i.row_id')

            # Index > Doc-Links > Vertical patching > Update ORG-table specific custom fields
            def vertical_patch_parentchild(self, actions=()):
                
                # Generate SQL query
                SQLQuery = f"""
                         UPDATE graphsearch_test.Index_D_{self.doc_type}_L_{self.link_type}_T_ORG i
                     INNER JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b
                             ON (i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_id)
                              = (b.doc_institution, b.doc_type, b.doc_id, b.link_institution, b.link_type, b.link_id)
                            SET  {',   '.join([f'i.{c}  = b.{c}' for c in self.obj2obj_fields_with_lang])}
                          WHERE ({' OR '.join([f'i.{c} != b.{c}' for c in self.obj2obj_fields_with_lang])})
                            AND b.to_process > 0.5;
                """

                # self.db.execute_query_in_shell(engine_name='test', query=SQLQuery)

                # Generate SQL query
                SQLQuery = f"""
                         UPDATE graphsearch_test.Index_D_{self.link_type}_L_{self.doc_type}_T_ORG i
                     INNER JOIN graph_cache.IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b
                             ON ( i.doc_institution,  i.doc_type,  i.doc_id, i.link_institution, i.link_type, i.link_id)
                              = (b.link_institution, b.link_type, b.link_id,  b.doc_institution,  b.doc_type,  b.doc_id)
                            SET  {',   '.join([f'i.{c}  = b.{c}' for c in self.obj2obj_fields_with_lang])}
                          WHERE ({' OR '.join([f'i.{c} != b.{c}' for c in self.obj2obj_fields_with_lang])})
                            AND b.to_process > 0.5;
                """

                # self.db.execute_query_in_shell(engine_name='test', query=SQLQuery)

            # Index > Doc-Links > Vertical patching > Update ElasticSearch specific fields
            def vertical_patch_elasticsearch(self, actions=()):
                
                # Generate SQL query
                sql_query = f"""
                    UPDATE elasticsearch_cache.Index_D_{self.doc_type}_L_{self.link_type} t

                INNER JOIN graph_cache.Data_N_Object_T_PageProfile p 
                        ON (t.link_type, t.link_id) = (p.object_type, p.object_id)

                INNER JOIN graph_cache.IndexBuildup_Fields_Docs_{self.link_type} l
                        ON (t.link_type, t.link_id) = (l.doc_type, l.doc_id)

                        SET t.name_en = IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value),
                            t.name_fr = IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value),
                            t.short_description_en = p.description_short_en_value,
                            t.short_description_fr = p.description_short_fr_value
                            {', ' if len(self.obj_fields_with_lang)>0 else ''}{', '.join([f't.{c} = l.{c}' for c in self.obj_fields_with_lang])}

                    WHERE p.object_type = '{self.link_type}'
                        AND (p.to_process > 0.5 OR l.to_process > 0.5)

                        AND (   t.name_en != IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value)
                            OR t.name_fr != IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value)
                            OR t.short_description_en != p.description_short_en_value
                            OR t.short_description_fr != p.description_short_fr_value
                            {'OR ' if len(self.obj_fields_with_lang)>0 else ''}{' OR '.join([f't.{c} != l.{c}' for c in self.obj_fields_with_lang])}
                            );
                """

            # ------- Rollbacks ------- #

            # Index > Doc-Links > Vertical patching > Roll back to previous state
            def vertical_rollback_parentchild(self, rollback_date=False, actions=()):
                
                # Generate SQL query
                SQLQuery = f"""
                        UPDATE graphsearch_test.Index_D_{self.doc_type}_L_{self.link_type}_T_ORG i
                    INNER JOIN graph_cache.IndexRollback_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b
                            ON (i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_id)
                            = (b.doc_institution, b.doc_type, b.doc_id, b.link_institution, b.link_type, b.link_id)
                        SET {', '.join([f'i.{c} = b.{c}' for c in self.obj2obj_fields_with_lang])}
                        WHERE b.rollback_date = '{rollback_date}';
                """
                
                # Generate SQL query
                SQLQuery = f"""
                        UPDATE graphsearch_test.Index_D_{self.link_type}_L_{self.doc_type}_T_ORG i
                    INNER JOIN graph_cache.IndexRollback_Fields_Links_ParentChild_{self.doc_type}_{self.link_type} b
                            ON (i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_id)
                            = (b.link_institution, b.link_type, b.link_id, b.doc_institution, b.doc_type, b.doc_id)
                        SET {', '.join([f'i.{c} = b.{c}' for c in self.obj2obj_fields_with_lang])}
                        WHERE b.rollback_date = '{rollback_date}';
                """

            #=====================#
            # Horizontal patching #
            #=====================#

            # ------ Snapshots ------- #

            # Index > Doc-Links > Horizontal patching > Generate snapshot
            def horizontal_snapshot(self, rollback_date, actions=()):

                # Check if there's something to process
                if self.link_subtype.upper() == 'ORG':
                    if len(self.db.execute_query(
                        engine_name = 'test',
                        query = f"""
                            SELECT 1
                            FROM graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric 
                            WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                            AND to_process > 0.5 LIMIT 1"""
                        )) == 0:
                        # print(f"Nothing to process for {self.link_subtype.upper()} link types '{self.doc_type}' and '{self.link_type}'.")
                        return
                elif self.link_subtype.upper() == 'SEM':
                    if len(self.db.execute_query(
                        engine_name = 'test',
                        query = f"""
                            SELECT 1
                            FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                            WHERE ((from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                            OR     (to_object_type, from_object_type) = ("{self.doc_type}", "{self.link_type}"))
                            AND to_process > 0.5 LIMIT 1"""
                        )) == 0:
                        # print(f"Nothing to process for {self.link_subtype.upper()} link types '{self.doc_type}' and '{self.link_type}'.")
                        return

                # Check if table exists
                if not self.db.table_exists(engine_name='test', schema_name=self.test_schema_name, table_name=self.index_table_name):
                    return False

                # Organisational table?
                if self.link_subtype.upper() == 'ORG':

                    # Generate SQL query
                    SQLQuery = f"""
                    INSERT IGNORE INTO graph_cache.IndexRollback_ScoreRanks_Links
                                      (rollback_date, doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, score, row_score, row_rank)
                                SELECT '{rollback_date}' AS rollback_date, i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_subtype, i.link_id, i.degree_score AS score, i.row_score, i.row_rank
                                  FROM graphsearch_test.{self.index_table_name} i
                            INNER JOIN (SELECT DISTINCT from_institution_id AS doc_institution, from_object_type AS doc_type, from_object_id AS doc_id
                                                   FROM graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                                    AND to_process > 0.5) c
                                 USING (doc_institution, doc_type, doc_id);
                    """

                # Semantic table?
                elif self.link_subtype.upper() == 'SEM':

                    # Generate SQL query
                    SQLQuery = f"""
                    INSERT IGNORE INTO graph_cache.IndexRollback_ScoreRanks_Links
                                      (rollback_date, doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, score, row_score, row_rank)
                                SELECT '{rollback_date}' AS rollback_date, i.doc_institution, i.doc_type, i.doc_id, i.link_institution, i.link_type, i.link_subtype, i.link_id, i.semantic_score AS score, i.row_score, i.row_rank
                                  FROM graphsearch_test.{self.index_table_name} i
                            INNER JOIN (SELECT DISTINCT s.from_institution_id AS doc_institution, s.from_object_type AS doc_type, s.from_object_id AS doc_id
                                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS s
                                             INNER JOIN graph_cache.IndexBuildup_Fields_Docs_{self.link_type} i
                                                     ON (s.from_object_type,   s.to_object_type,   s.to_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                                                     OR (  s.to_object_type, s.from_object_type, s.from_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                                                  WHERE s.to_process > 0.5) c
                                 USING (doc_institution, doc_type, doc_id);
                    """

            # ------- Patching ------- #

            # Index > Doc-Links > Horizontal patching > Insert new, replace existing, re-rank (graphsearch_test)
            def horizontal_patch(self, row_rank_thr=32, actions=()):

                #---------------------------#
                # Convert order list to SQL #
                #---------------------------#
                index_to_score_type = {'ORG':'degree_score', 'SEM':'semantic_score'}
                if not self.link_type in index_config['fields']['links']['default']:
                    order_by = f'{index_to_score_type[self.link_subtype.upper()]} DESC, link_id ASC'
                else:
                    ord = index_config['fields']['links']['default'][self.link_type]['order']
                    cast_mapping = {
                        "TINYINT(1)"        : "CAST(%s AS UNSIGNED)",
                        "SMALLINT UNSIGNED" : "CAST(%s AS UNSIGNED)",
                        "YEAR"              : "CAST(%s AS UNSIGNED)",
                        "VARCHAR(16)"       : "CAST(%s AS CHAR)"
                    }
                    if len(ord)>0:
                        order_by = ', '.join([cast_mapping[index_config['data-types'][o]]%o+' '+d for o,d in ord]) + f', {index_to_score_type[self.link_subtype.upper()]} DESC, link_id ASC'
                    else:
                        order_by = f'{index_to_score_type[self.link_subtype.upper()]} DESC, link_id ASC'
                #---------------------------#

                # Full table paths
                parentchild_table_path  = f"{mysql_schema_names[self.engine_name]['cache']}.{'Edges_N_Object_N_Object_T_ParentChildSymmetric'}"
                scoresmatrix_table_path = f"{mysql_schema_names[self.engine_name]['cache']}.{'Edges_N_Object_N_Object_T_ScoresMatrix_AS'}"
                buildup_doc_table_path  = f"{mysql_schema_names[self.engine_name]['cache']}.{self.buildup_doc_table_name}"
                buildup_link_table_path = f"{mysql_schema_names[self.engine_name]['cache']}.{self.buildup_link_table_name}"
                target_table_path       = f"{mysql_schema_names[self.engine_name]['ui'   ]}.{self.index_table_name}"

                # Does the buildup table exist?
                buildup_table_exists_direct  = self.db.table_exists(engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['cache'], table_name=f'IndexBuildup_Fields_Links_ParentChild_{self.doc_type}_{self.link_type}')
                buildup_table_exists_flipped = self.db.table_exists(engine_name=self.engine_name, schema_name=mysql_schema_names[self.engine_name]['cache'], table_name=f'IndexBuildup_Fields_Links_ParentChild_{self.link_type}_{self.doc_type}')
                buildup_table_exists = buildup_table_exists_direct or buildup_table_exists_flipped

                # Cross-engine collate correction
                colate_correct = 'COLLATE utf8mb4_unicode_ci' if self.engine_name=='prod' else ''

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
                                    (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{', ' if len(self.obj_fields_with_lang)>0 else ' '}{', '.join(self.obj2obj_fields_with_lang)}{',' if len(self.obj2obj_fields_with_lang)>0 else ''} degree_score, row_score, row_rank)
                              SELECT p.from_institution_id AS doc_institution, p.from_object_type AS doc_type, p.from_object_id AS doc_id,
                                     p.to_institution_id AS link_institution, p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                     {', '.join([f'bd.{c}' for c in self.obj_fields_with_lang])}{', ' if len(self.obj_fields_with_lang)>0 else ' '}{', '.join([f'bl.{c}' for c in self.obj2obj_fields_with_lang])}{',' if len(self.obj2obj_fields_with_lang)>0 else ''}
                                     bd.degree_score, 0 AS row_score, 99 AS row_rank
                                FROM {parentchild_table_path} p
                          INNER JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                          INNER JOIN {mysql_schema_names[self.engine_name]['cache']}.IndexBuildup_Fields_Links_ParentChild_{self.doc_type if buildup_table_exists_direct else self.link_type}_{self.link_type if buildup_table_exists_direct else self.doc_type} bl
                                  ON (p.{'from' if buildup_table_exists_direct else 'to'}_object_type, p.{'from' if buildup_table_exists_direct else 'to'}_object_id, p.{'to' if buildup_table_exists_direct else 'from'}_object_type, p.{'to' if buildup_table_exists_direct else 'from'}_object_id) = (bl.doc_type, bl.doc_id, bl.link_type, bl.link_id)
                               WHERE p.from_object_type {colate_correct} = '{self.doc_type}'
                                 AND p.to_object_type   {colate_correct} = '{self.link_type}'
                                 AND p.to_process > 0.5;
                        """
                    
                    # No buildup table
                    else:

                        # Generate SQL query 1
                        SQLQuery1 = f"""
                        REPLACE INTO {target_table_path}
                                    (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{', ' if len(self.obj_fields_with_lang)>0 else ' '} degree_score, row_score, row_rank)
                              SELECT p.from_institution_id AS doc_institution, p.from_object_type AS doc_type, p.from_object_id AS doc_id,
                                     p.to_institution_id AS link_institution, p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                     {', '.join([f'bd.{c}' for c in self.obj_fields_with_lang])}{', ' if len(self.obj_fields_with_lang)>0 else ' '}
                                     bd.degree_score, 0 AS row_score, 99 AS row_rank
                                FROM {parentchild_table_path} p
                          INNER JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                               WHERE p.from_object_type {colate_correct} = '{self.doc_type}'
                                 AND p.to_object_type   {colate_correct} = '{self.link_type}'
                                 AND p.to_process > 0.5;
                        """

                    # Generate SQL query 3
                    SQLQuery3 = f"""
                    REPLACE INTO {target_table_path}
                                        (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{', ' if len(self.obj_fields_with_lang)>0 else ' '}{', '.join(self.obj2obj_fields_with_lang)}{',' if len(self.obj2obj_fields_with_lang)>0 else ''} degree_score, row_score, row_rank)
                          SELECT         doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{', ' if len(self.obj_fields_with_lang)>0 else ' '}{', '.join(self.obj2obj_fields_with_lang)}{',' if len(self.obj2obj_fields_with_lang)>0 else ''} degree_score, row_score, row_rank
                            FROM (SELECT doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{', ' if len(self.obj_fields_with_lang)>0 else ' '}{', '.join(self.obj2obj_fields_with_lang)}{',' if len(self.obj2obj_fields_with_lang)>0 else ''} degree_score,
                                        CAST(1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})) AS FLOAT) AS row_score,
                                                        row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})            AS row_rank
                                    FROM {target_table_path}
                              INNER JOIN (SELECT DISTINCT IF(from_object_type='{self.doc_type}', from_object_id, to_object_id) AS doc_id
                                                     FROM {parentchild_table_path}
                                                    WHERE from_object_type {colate_correct} = '{self.doc_type}'
                                                      AND to_object_type   {colate_correct} = '{self.link_type}'
                                                      AND to_process > 0.5) t
                                   USING (doc_id)
                                 ) tt 
                           WHERE row_rank <= {row_rank_thr};
                    """

                # Semantic table?
                elif self.link_subtype.upper() == 'SEM':

                    # Generate SQL query 1
                    SQLQuery1 = f"""
                    REPLACE INTO {target_table_path}
                                (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{',' if len(self.obj_fields_with_lang)>0 else ''} semantic_score, row_score, row_rank)
                          SELECT s.from_institution_id AS doc_institution, s.from_object_type AS doc_type, s.from_object_id AS doc_id,
                                 s.to_institution_id AS link_institution, s.to_object_type AS link_type, 'Semantic' AS link_subtype, s.to_object_id AS link_id,
                                 {', '.join([f'i.{c}' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                                 s.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                            FROM {scoresmatrix_table_path} s
                      INNER JOIN {buildup_link_table_path} i
                              ON (s.from_object_type, s.to_object_type, s.to_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                           WHERE s.to_process > 0.5;
                    """

                    # Generate SQL query 2
                    SQLQuery2 = f"""
                    REPLACE INTO {target_table_path}
                                (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{',' if len(self.obj_fields_with_lang)>0 else ''} semantic_score, row_score, row_rank)
                          SELECT s.to_institution_id AS doc_institution, s.to_object_type AS doc_type, s.to_object_id AS doc_id,
                                 s.from_institution_id AS link_institution, s.from_object_type AS link_type, 'Semantic' AS link_subtype, s.from_object_id AS link_id,
                                 {', '.join([f'i.{c}' for c in self.obj_fields_with_lang])}{',' if len(self.obj_fields_with_lang)>0 else ''}
                                 s.score AS semantic_score, 0 AS row_score, 99 AS row_rank
                            FROM {scoresmatrix_table_path} s
                      INNER JOIN {buildup_link_table_path} i
                              ON (s.to_object_type, s.from_object_type, s.from_object_id) = ("{self.doc_type}", "{self.link_type}", i.doc_id)
                           WHERE s.to_process > 0.5;
                    """

                    # Generate SQL query 3
                    # TODO: Verify new query          
                    SQLQuery3 = f"""
                    REPLACE INTO {target_table_path}
                                        (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{',' if len(self.obj_fields_with_lang)>0 else ''} semantic_score, row_score, row_rank)
                          SELECT         doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{',' if len(self.obj_fields_with_lang)>0 else ''} semantic_score, row_score, row_rank
                            FROM (SELECT doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, {', '.join(self.obj_fields_with_lang)}{',' if len(self.obj_fields_with_lang)>0 else ''} semantic_score,
                                         CAST(1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})) AS FLOAT) AS row_score,
                                                         row_number() OVER (PARTITION BY doc_id ORDER BY {order_by})            AS row_rank
                                    FROM {target_table_path}
                              INNER JOIN (
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
                                                      AND to_process > 0.5

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
                                                      AND to_process > 0.5
                                         ) t
                                   USING (doc_id)
                                 ) tt 
                           WHERE row_rank <= {row_rank_thr};
                    """

                # Evaluate the patch operation
                if 'eval' in actions:

                    # Generate evaluation query (#1)
                    sql_query_eval_1 = f"""
                        SELECT COUNT(*) AS n_total
                        FROM {SQLQuery1.split('FROM')[1]}
                    """

                    # Generate evaluation query (#2)
                    sql_query_eval_2 = f"""
                        SELECT COUNT(*) AS n_total
                        FROM {SQLQuery2.split('FROM')[1]}
                    """

                    # Generate evaluation query (#3)
                    sql_query_eval_3 = f"""
                        SELECT COUNT(*) AS n_total
                        FROM (SELECT {SQLQuery3.split('FROM (SELECT')[1]}
                    """

                    # Print the evaluation queries
                    if 'print' in actions:
                        print(sql_query_eval_1)
                        print(sql_query_eval_2)
                        print(sql_query_eval_3)

                    # Execute the evaluation queries
                    out_1 = self.db.execute_query(engine_name=self.engine_name, query=sql_query_eval_1)
                    out_2 = self.db.execute_query(engine_name=self.engine_name, query=sql_query_eval_2)
                    out_3 = self.db.execute_query(engine_name=self.engine_name, query=sql_query_eval_3)

                    # Sum up the results
                    out = [[out_1[0][0] + out_2[0][0], out_3[0][0]]]

                    # Print the results
                    df = pd.DataFrame(out, columns=['rows to insert/replace', 'rows to re-score'])
                    print_dataframe(df, title=f'🔍 Evaluation results for {target_table_path}:')

                # Print SQL query
                if 'print' in actions:
                    if SQLQuery1:
                        print('')
                        print(SQLQuery1)
                    if SQLQuery2:
                        print('')
                        print(SQLQuery2)
                    if SQLQuery3:
                        print('')
                        print(SQLQuery3)
                
                # Execute SQL query
                if 'commit' in actions:
                    if SQLQuery1:
                        self.db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery1)
                    if SQLQuery2:
                        self.db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery2)
                    if SQLQuery3:
                        self.db.execute_query_in_shell(engine_name=self.engine_name, query=SQLQuery3)

            # Index > Doc-Links > Horizontal patching > Insert new, replace existing, re-rank (elasticseach_cache)
            def horizontal_patch_elasticsearch(self, row_rank_thr=16, actions=()):
                
                # Resolve table name or return if it doesn't exist
                # Table type: MIX
                if self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_MIX", exclude_views=False):
                    
                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_MIX"
                    
                    # Generate score column name
                    score_column_name = 'adjusted_row_rank'

                    # Generate SQL query segment for fetching rows to process
                    to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                                  UNION
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                                  UNION
                        SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                  WHERE (to_object_type, from_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                    """

                # Table type: ORG
                elif self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_ORG", exclude_views=True):
                    
                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_ORG"
                    
                    # Generate score column name
                    score_column_name = 'row_rank'

                    # Generate SQL query segment for fetching rows to process
                    to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                    """

                # Table type: SEM
                elif self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=f"Index_D_{self.doc_type}_L_{self.link_type}_T_SEM", exclude_views=True):
                    
                    # Generate table name
                    table_name = f"Index_D_{self.doc_type}_L_{self.link_type}_T_SEM"
                    
                    # Generate score column name
                    score_column_name = 'row_rank'

                    # Generate SQL query segment for fetching rows to process
                    to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                  WHERE (from_object_type, to_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                                  UNION
                        SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AS
                                  WHERE (to_object_type, from_object_type) = ("{self.doc_type}", "{self.link_type}")
                                    AND to_process > 0.5
                    """
                else:
                    return False

                # Genenerate table name
                t = f"elasticsearch_cache.Index_D_{self.doc_type}_L_{self.link_type}"

                # Generate SQL query
                SQLQuery = f"""
                    INSERT INTO {t}
                                (doc_type, doc_id, link_type, link_subtype, link_id, link_rank, link_name_en, link_name_fr, link_short_description_en, link_short_description_fr{', ' if len(self.obj_fields_with_lang)>0 else ''}{', '.join([f'{c}' for c in self.obj_fields_with_lang])})
                         SELECT d.doc_type, d.doc_id, dl.link_type, dl.link_subtype, dl.link_id, dl.{score_column_name} AS link_rank,
                                IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value) AS link_name_en,
                                IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value) AS link_name_fr,
                                p.description_short_en_value AS link_short_description_en, p.description_short_fr_value AS link_short_description_fr{',' if len(self.obj_fields_with_lang)>0 else ''}
                                {', '.join([f'l.{c}' for c in self.obj_fields_with_lang])}
                           FROM graphsearch_test.Index_D_{self.doc_type} d
                     INNER JOIN graphsearch_test.{table_name} dl
                          USING (doc_type, doc_id)
                     INNER JOIN graphsearch_test.Index_D_{self.link_type} l
                             ON (dl.link_type, dl.link_id) = (l.doc_type, l.doc_id)
                     INNER JOIN graphsearch_test.Data_N_Object_T_PageProfile p 
                             ON (p.object_type, p.object_id) = (l.doc_type, l.doc_id)
                     INNER JOIN (
                                {to_process_sql_statement}
                                ) tp
                             ON (dl.doc_type, dl.doc_id) = (tp.doc_type, tp.doc_id)
                          WHERE dl.row_rank <= {row_rank_thr}
                                {'AND' if len(self.es_filters)>0 else ''} {' AND '.join([f'l.{f}' for f in self.es_filters])}
               ON DUPLICATE KEY
                         UPDATE {t}.link_rank = IF(COALESCE({t}.link_rank, "__null__") != COALESCE(dl.{score_column_name}, "__null__"), dl.{score_column_name}, {t}.link_rank);
                """

                # Print SQL query
                if 'print' in actions:
                    print(SQLQuery)

                # Execute SQL query
                if 'commit' in actions:
                    self.db.execute_query_in_shell(engine_name='test', query=SQLQuery)

            # ------- Rollbacks ------- #

            # Index > Doc-Links > Horizontal patching > Roll back to previous state
            def horizontal_rollback(self, source_doc_type, target_doc_type, index_type, rollback_date, test_mode=False):

                # Check if there's something to process
                if len(self.db.execute_query(
                    engine_name = 'test',
                    query = f"""
                        SELECT 1
                        FROM graph_cache.IndexRollback_ScoreRanks_Links
                        WHERE (doc_type, link_type) = ("{source_doc_type}", "{target_doc_type}")
                        AND link_subtype IN ({"'Parent-to-Child', 'Child-to-Parent'" if index_type=='ORG' else "'Semantic'"})
                        AND rollback_date = "{rollback_date}" LIMIT 1"""
                    )) == 0:
                    # print(f"Nothing to process for link types '{source_doc_type}' and '{target_doc_type}'.")
                    return

                # Generate table name
                table_name = f'Index_D_{source_doc_type}_L_{target_doc_type}_T_{index_type}'

                # Check if table exists
                if not self.db.table_exists(engine_name='test', schema_name='graphsearch_test', table_name=table_name):
                    # print(f"Table 'graphsearch_test.{table_name}' does not exist.")
                    return False
                
                # Generate SQL query
                SQLQuery = f"""
                        UPDATE graphsearch_test.{table_name} i
                    INNER JOIN graph_cache.IndexRollback_ScoreRanks_Links b
                         USING (doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id)
                           SET i.{'semantic' if index_type=='SEM' else 'degree'}_score = b.score, i.row_score = b.row_score, i.row_rank = b.row_rank
                         WHERE (b.doc_type, b.link_type) = ("{source_doc_type}", "{target_doc_type}")
                           AND b.rollback_date = "{rollback_date}";
                        """

                # Execute SQL query
                if test_mode:
                    print(SQLQuery)
                else:
                    self.db.execute_query_in_shell(engine_name='test', query=SQLQuery)

    #------------------------------------------------------------#
    # Subclass definition: GraphIndex Management (ElasticSearch) #
    #------------------------------------------------------------#
    class IndexES():

        # Class constructor
        def __init__(self):
            self.db = GraphDB()
            self.params_test, self.engine_test = self.initiate_engine(global_config['mysql']['server_test'])
            self.params_prod, self.engine_prod = self.initiate_engine(global_config['mysql']['server_prod'])
            self.params = {'test': self.params_test, 'prod': self.params_prod}
            self.engine = {'test': self.engine_test, 'prod': self.engine_prod}

        def initiate_engine(self, server_name):
            if server_name not in global_config['elasticsearch']:
                raise ValueError(
                    f'could not find the configuration for elasticsearch server {server_name} in {global_config_file}.'
                )
            params = global_config['elasticsearch'][server_name]
            if "password" in params:
                es_hosts = f'https://{params["username"]}:{quote(params["password"])}@{params["host"]}:{params["port"]}'
            else:
                es_hosts = f'https://{params["username"]}@{params["host"]}:{params["port"]}'
            cert_file = params["cert_file"] if "cert_file" in params else None
            engine = ElasticSearchEngine(
                hosts=es_hosts, http_compress=True, ca_certs=cert_file, verify_certs=False, request_timeout=3600
            )
            return params, engine
        
        # Generate ES index --> keep
        def generate_split_files(self, index_date=False):

            # Initialise default column names
            default_column_names_doc  = ['doc_type', 'doc_id', 'degree_score', 'short_code', 'subtype_en', 'subtype_fr', 'name_en', 'name_fr', 'short_description_en', 'short_description_fr', 'long_description_en', 'long_description_fr']
            default_column_names_link = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id', 'link_rank', 'link_name_en', 'link_name_fr', 'link_short_description_en', 'link_short_description_fr']

            #-------------------------#
            # Loop over all doc types #
            #-------------------------#
            # Loop over all doc types
            for dummy, doc_type in index_doc_types_list:

                # Initialise index dict struct
                es_index_struct = {}

                # Display status
                print('Processing doc type:', doc_type)

                # Fetch doc fields
                obj_fields   = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['docs'][doc_type]]
                custom_column_names_doc = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in obj_fields]

                # TODO: fix this
                if doc_type == 'Lecture':
                    custom_column_names_doc = ["video_stream_url", "video_duration", "is_restricted"]

                # Combine default and custom column names
                column_names_doc = default_column_names_doc + custom_column_names_doc

                # Fetch list of docs for doc_type
                print('Loading docs ...')
                list_of_docs = self.db.execute_query(engine_name='test', query=f"""
                    SELECT {', '.join(column_names_doc)}
                    FROM elasticsearch_cache.Index_D_{doc_type}
                    ORDER BY doc_id ASC
                """)

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
                for dummy, link_type in index_doc_types_list:

                    # Fetch doc fields
                    obj_fields   = [tuple(v) if type(v) is list else ('n/a', v) for v in index_config['fields']['docs'][link_type]]
                    custom_column_names_link = [f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language] for field_language, field_name in obj_fields]

                    # TODO: fix this
                    if link_type == 'Lecture':
                        custom_column_names_link = ["video_stream_url", "video_duration", "is_restricted"]

                    # Combine default and custom column names
                    column_names_link = default_column_names_link + custom_column_names_link

                    # Check if link table exists
                    if not self.db.table_exists(engine_name='test', schema_name='elasticsearch_cache', table_name=f"Index_D_{doc_type}_L_{link_type}"):
                        print('Warning: Table does not exist:', f"Index_D_{doc_type}_L_{link_type}")
                        continue

                    print(f'Loading {link_type} links ...')
                    list_of_links = self.db.execute_query(engine_name='test', query=f"""
                        SELECT {', '.join(column_names_link)}
                        FROM elasticsearch_cache.Index_D_{doc_type}_L_{link_type}
                        ORDER BY doc_id ASC, link_rank ASC
                    """)

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
                            print('Warning: doc_id not found in index struct:', l[1])
                            continue

                        # Append link to doc JSON
                        es_index_struct[doc_type][l[1]]['links'] += [json_link]
    
                # Create target folder (if not exists)
                target_folder = f"{ELASTICSEARCH_DATA_EXPORT_PATH}"
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder)

                # Save index JSON to file
                with open(f'{ELASTICSEARCH_DATA_EXPORT_PATH}/es_splitindex_{index_date}_{doc_type}.json', 'w') as f:
                    json.dump(es_index_struct, f, indent=4)

        # Generate ES final index --> keep
        def generate_index(self, index_date=False):

            es_index = []
        
            for inst_id, doc_type in index_doc_types_list:

                print(f"Loading index struct for {doc_type} ...")
                with open(f'{ELASTICSEARCH_DATA_EXPORT_PATH}/es_splitindex_{index_date}_{doc_type}.json', 'r') as f:
                    es_index_struct = json.load(f)

                for doc_id in es_index_struct[doc_type]:
                    es_index += [es_index_struct[doc_type][doc_id]]

            # Save index JSON to file
            print(f"Saving final index to file ...")
            with open(f'{ELASTICSEARCH_DATA_EXPORT_PATH}/es_fullindex_{index_date}.json', 'w') as f:
                json.dump(es_index, f, indent=4)

        def info(self, engine_name):
            # print(self.engine[engine_name])
            # return self.engine[engine_name]
            rich.print_json(data=dict(self.engine[engine_name].info()))

        def cluster_health(self, engine_name):
            rich.print_json(data=dict(self.engine[engine_name].cluster.health()))

        # GET /_cluster/allocation/explain?pretty
        def cluster_allocation_explain(self, engine_name):
            rich.print_json(data=dict(self.engine[engine_name].cluster.allocation_explain()))

        def index_list(self, engine_name):
            print(f"List of indexes on {engine_name}:")
            for index in self.engine[engine_name].indices.get(index="*"):
                if not index.startswith('.'):
                    print(' - ', index)

        def index_list_with_size(self, engine_name='test'):
            index_sizes = []
            for index in self.engine[engine_name].indices.get(index="*"):
                if not index.startswith('.'):
                    index_sizes += [(index, self.engine[engine_name].indices.stats(index=index)['indices'][index]['total']['store']['size_in_bytes'])]
            print(f"List of indexes on {engine_name}:")
            for index, index_size in sorted(index_sizes, key=lambda x: x[0], reverse=False):
                print(f' - {index} ({index_size/1024/1024/1024:.2f} GB)')

        def drop_index(self, engine_name, index_name):
            try:
                self.engine[engine_name].indices.delete(index=index_name)
                print(f'Index {index_name} deleted')
            except:
                print(f'Index {index_name} does not exist')
                pass

        def alias_list(self, engine_name):
            print(f"List of aliases on {engine_name}:")
            aliases = self.engine[engine_name].indices.get_alias()
            alias_to_index = {}
            for index, alias_info in aliases.items():
                if not index.startswith('.'):
                    for alias in alias_info['aliases']:
                        if not alias.startswith('.'):
                            alias_to_index[alias] = index
            for alias, index in alias_to_index.items():
                print(f" - {alias} --> {index}")

        def set_alias(self, engine_name, alias_name, index_name):
            es = self.engine[engine_name]
            
            # Check if the alias exists
            existing_aliases = es.indices.get_alias(name=alias_name, ignore=404)
            
            actions = []
            
            if existing_aliases and existing_aliases.get('status', 200) == 200:
                # Remove alias from existing indices
                for existing_index in existing_aliases.keys():
                    actions.append({"remove": {"index": existing_index, "alias": alias_name}})
                
                if actions:
                    es.indices.update_aliases(body={"actions": actions})
                    print(f"Removed alias '{alias_name}' from indices: {', '.join(existing_aliases.keys())}")

            # Add alias to the new index
            actions.append({"add": {"index": index_name, "alias": alias_name}})
            es.indices.update_aliases(body={"actions": actions})
            print(f"Alias '{alias_name}' now points to index '{index_name}'")

        def drop_alias(self, engine_name, alias_name):
            es = self.engine[engine_name]
            existing_aliases = es.indices.get_alias(name=alias_name, ignore=404)
            actions = []
            if existing_aliases and existing_aliases.get('status', 200) == 200:
                for existing_index in existing_aliases.keys():
                    actions.append({"remove": {"index": existing_index, "alias": alias_name}})
            if actions:
                es.indices.update_aliases(body={"actions": actions})
                print(f"Removed alias '{alias_name}' from indices: {', '.join(existing_aliases.keys())}")

        def create_index_from_file(self, engine_name, index_name, index_file, chunk_size=1000, delete_if_exists=False):

            def sort_dict_alphabetically(d):
                """
                Recursively sorts a dictionary (and any nested dictionaries) by their keys.
                """
                if isinstance(d, dict):
                    # Recursively sort nested dictionaries
                    return {key: sort_dict_alphabetically(value) for key, value in sorted(d.items())}
                elif isinstance(d, list):
                    # If lists are encountered, apply the sort function to each element
                    return [sort_dict_alphabetically(item) for item in d]
                else:
                    # Return non-dict, non-list values as-is
                    return d

            # Delete index
            if delete_if_exists:
                try:
                    self.engine[engine_name].indices.delete(index=index_name)
                    print(f'Index {index_name} deleted')
                except:
                    print(f'Index {index_name} does not exist')
                    pass
                    
            # Create the new index with settings and mappings
            print(f'Creating index {index_name} with custom settings and mappings...')
            self.engine[engine_name].indices.create(index=index_name, body=es_settings_and_mappings)
            print(f'Index {index_name} created.')

            # Verify that the new index was created with the correct settings and mappings (fail and exit if not)
            print(f'Verifying index {index_name} settings and mappings...')
            index_info = self.engine[engine_name].indices.get(index=index_name)
            condition_1 = sort_dict_alphabetically(index_info[index_name]['settings']['index']['analysis']) == sort_dict_alphabetically(es_settings_and_mappings['settings']['index']['analysis'])
            condition_2 = sort_dict_alphabetically(index_info[index_name]['mappings']) == sort_dict_alphabetically(es_settings_and_mappings['mappings'])

            # Read the index settings and mappings from the file
            print(f'Reading index {index_file} ...')
            with open(index_file, 'r') as file:
                es_index = json.load(file)

            # Print progress
            print(f"Indexing ...")

            # Prepare the actions for bulk indexing
            actions = [
                {
                    "_index": index_name,
                    "_id": doc["doc_type"]+'-'+doc["doc_id"],
                    "_source": doc
                }
                for doc in es_index
            ]

            # Assuming 'actions' is a list of actions prepared for the Bulk API
            total_docs = len(actions)
            count = 0

            # Perform the bulk index operation
            try:
                # Iterate over the results from streaming_bulk with chunk_size of 100
                for success, info in helpers.streaming_bulk(self.engine[engine_name], actions, chunk_size=chunk_size, request_timeout=120):
                    if not success:
                        print('A document failed:', info)
                    count += 1
                    if count % 1000 == 0:
                        es_write_progress(count, total_docs, status='Continuing...')
                es_write_progress(total_docs, total_docs, status='Done')
                print("\nBulk index operation completed")
            except helpers.BulkIndexError as e:
                print(f"Bulk indexing error: {e}")
                for error in e.errors:
                    print(f"Error details: {error}")
                exit()
            except Exception as e:
                print(f"Error during bulk index operation: {e}")
                exit()
    
        def copy_index_from_test_to_prod(self, index_name, rename_to=None, chunk_size=1000):
            
            # Define the index names
            index_name_test = index_name
            index_name_prod = index_name
            if rename_to is not None:
                index_name_prod = rename_to

            # Define the parameters for the ElasticDump command
            params_server_test = f"https://{self.params_test['username']}:{quote(self.params_test['password'])}@{self.params_test['host']}:{self.params_test['port']}/{index_name_test}"
            params_server_prod = f"https://{self.params_prod['username']}:{quote(self.params_prod['password'])}@{self.params_prod['host']}:{self.params_prod['port']}/{index_name_prod}"
            base_command = [global_config['elasticsearch']['dump_bin'], f"--input={params_server_test}", f"--output={params_server_prod}", f"--input-ca={self.params_test['cert_file']}", f"--output-ca={self.params_prod['cert_file']}", f"--limit={chunk_size}"]
            
            # Copy the index from test to prod
            for type in ['settings', 'mapping', 'data']:
                subprocess.run(base_command + [f"--type={type}"], env={**os.environ, "NODE_TLS_REJECT_UNAUTHORIZED": "0"})

        def execute_query(self, engine_name, index_name, query):
            return self.engine[engine_name].search(index=index_name, body=query)

        def fetch_docs_by_id(self, engine_name, index_name, doc_ids_list):
            return self.engine[engine_name].mget(index=index_name, body={"ids": doc_ids_list})


        # es.create_index_from_file(engine_name='test', index_name='graphsearch_test_2025_03-27', index_file='/Users/francisco/Cloud/Academia/CEDE/EPFLGraph/GitHub/data/elasticsearch_data_exports/es_fullindex_2025-03-27.json', chunk_size=1000, delete_if_exists=True)
        # es.alias_list(engine_name='test')
        # es.set_alias(engine_name='test', alias_name='graphsearch_test', index_name='2025-03-27')
        # es.copy_index_from_test_to_prod(index_name='2025-03-27', rename_to='graphsearch_prod_2025_03_27', chunk_size=10000)

        # es.set_alias(engine_name='prod', alias_name='graphsearch_prod', index_name='graphsearch_prod_2025_03_27')

#=======================================================#
# Function definition: Tkinter Graphical User Interface #
#=======================================================#
def LaunchGUI(gr):

    # Initialize the main window
    root = tk.Tk()
    root.title("GraphRegistry GUI")
    root.geometry("1020x1020")

    # Configure grid weights for resizing behavior
    root.grid_rowconfigure(0, weight=1)     # Allow row 0 to expand vertically
    root.grid_columnconfigure(0, weight=0)  # Column 0 doesn't need to stretch horizontally
    root.grid_columnconfigure(1, weight=0)

    # Node types
    list_of_node_types = ['Category', 'Concept', 'Course', 'Lecture', 'MOOC', 'Person', 'Publication', 'Startup', 'Unit', 'Widget']

    # Initialise GUI elements dict
    gui = {
        'orchestration' : {
            'subframe' : None,
            'description' : None,
            'node_types' : {
                'subframe' : None,
                'description' : None,
                'list' : [],
                'button_add_new' : None,
            },
            'edge_types' : {
                'subframe' : None,
                'description' : None,
                'list' : [],
                'button_add_new' : None,
            },            
        },
        'processing' : {
            'subframe' : None,
            'description' : None,
            'cachemanage' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
            },
            'indexdb' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
                'engine' : False,
                'cache_buildup' : {'subframe' : None},
                'page_profile' : {'subframe' : None},
                'index_docs' : {'subframe' : None},
                'index_doc_links' : {'subframe' : None},
                'index_docs_es' : {'subframe' : None},
                'index_doc_links_es' : {'subframe' : None},
            },
            'elasticsearch' : {
                'subframe' : None,
                'description' : None,
                'actions' : {'print':False, 'eval':False, 'commit':False},
            },
        }
    }

    #================================================#
    # Functions handling button creation and actions #
    #================================================#
    if True:

        #--------------------------------------------#
        checkbox_rows_stack = []
        def create_action_checkboxes_row(frame_pointer, var_pointer, include_engine=False):

            # Create now checkbox row in stack
            checkbox_row = tk.Frame(frame_pointer)
            checkbox_row.pack(anchor='w', expand=False)

            # Create checkboxes for each action
            for action in ['print', 'eval', 'commit']:
                var_pointer[action] = tk.BooleanVar()
                tk.Checkbutton(checkbox_row, variable=var_pointer[action]).pack(side='left')
                tk.Label(checkbox_row, text=action).pack(side="left", padx=(0,4))

            # Add engine dropdown if required
            if include_engine:
                var_pointer['engine'] = tk.StringVar(value='test')
                tk.OptionMenu(checkbox_row, var_pointer['engine'], 'test', 'prod').pack(padx=(80,0))

            # Add checkbox row to stack
            checkbox_rows_stack.append(checkbox_row)

        #--------------------------------------------#
        button_rows_stack = []
        def create_buttons_row(frame_pointer, actions_matrix, function_subspace):
            
            # Create now button row in stack
            button_row = tk.Frame(root)

            # Container frame to hold buttons in a grid
            button_row = tk.Frame(frame_pointer)
            button_row.pack(pady=(0,0), anchor='w')

            # Buttons packed horizontally
            for row, col, wid, action in actions_matrix:
                tk.Button(button_row, width=wid, text=action, command=lambda a=action: on_button_click(f'{function_subspace} {a}')).grid(row=row, column=col, padx=(0,0), pady=(0,0), sticky='w')

            # Add button row to stack
            button_rows_stack.append(button_row)

        #--------------------------------------------#
        def on_button_click(button_input_action):

            # Print action
            print(f"\n\nButton clicked: {button_input_action}")

            #---------------------#
            # Orchestration panel #
            #---------------------#
            if 'orchestration' in button_input_action:

                # Assemble configuration JSON from GUI inputs
                config_json = {'nodes':[], 'edges':[]} 
                for d in gui['orchestration']['node_types']['list']:
                    config_json['nodes'] += [(d['dropdowns'][0].get(), d['process_fields'].get(), d['process_scores'].get())]
                for d in gui['orchestration']['edge_types']['list']:
                    config_json['edges'] += [(d['dropdowns'][0].get(), d['dropdowns'][1].get(), d['process_fields'].get(), d['process_scores'].get())]

                # Orchestration status
                if button_input_action == 'orchestration status':

                    # Print and execute method
                    print("""gr.orchestrator.status()""")
                    gr.orchestrator.status()

                # Orchestration config
                elif button_input_action == 'orchestration apply':

                    # Print configuration JSON extracted from GUI
                    print('config_json =')
                    rich.print_json(data=config_json)

                    # Print and execute method
                    print("""gr.orchestrator.typeflags.config(config_json, reset=True, verbose=True)""")
                    gr.orchestrator.typeflags.config(config_json, reset=True, verbose=True)

                # Orchestration reset
                elif button_input_action == 'orchestration reset type flags':

                    # Print and execute method
                    print("""gr.orchestrator.typeflags.reset()""")
                    gr.orchestrator.typeflags.reset()

                # Orchestration config
                elif button_input_action == 'orchestration set type flags':

                    # Print configuration JSON extracted from GUI
                    print('config_json =')
                    rich.print_json(data=config_json)

                    # Print and execute method
                    print("""gr.orchestrator.typeflags.config(config_json, reset=True, verbose=True)""")
                    gr.orchestrator.typeflags.config(config_json, reset=True, verbose=True)

                # Orchestration reset
                elif button_input_action == 'orchestration reset object flags':

                    # Print and execute method
                    print("""gr.orchestrator.fieldschanged.reset()""")
                    gr.orchestrator.fieldschanged.reset()
                    print("""gr.orchestrator.scoresexpired.reset()""")
                    gr.orchestrator.scoresexpired.reset()

                # Orchestration config
                elif button_input_action == 'orchestration set object flags':

                    # Print configuration JSON extracted from GUI
                    print('config_json =')
                    rich.print_json(data=config_json)

                    # Print and execute method
                    print("""gr.orchestrator.fieldschanged.config(config_json, reset=True, verbose=True)""")
                    gr.orchestrator.fieldschanged.config(config_json, reset=True, verbose=True)
                    print("""gr.orchestrator.scoresexpired.config(config_json, reset=True, verbose=True)""")
                    gr.orchestrator.scoresexpired.config(config_json, reset=True, verbose=True)

                # Orchestration propagate
                elif button_input_action == 'orchestration propagate flags':

                    # Print and execute method
                    print("""gr.orchestrator.propagate()""")
                    gr.orchestrator.propagate()

                # Orchestration reset
                elif button_input_action == 'orchestration sync new data':

                    # Print and execute method
                    print("""gr.orchestrator.sync()""")
                    gr.orchestrator.sync()



                # Orchestration: randomize
                elif button_input_action == 'orchestration randomize':

                    # Print and execute method
                    print("""gr.orchestrator.randomize()""")
                    gr.orchestrator.randomize()

                # Orchestration: apply expire
                elif button_input_action == 'orchestration apply expire':

                    # Print and execute method
                    print("""gr.orchestrator.expire()""")
                    gr.orchestrator.expire()

                # Orchestration: refresh tp=1 flags
                elif button_input_action == 'orchestration refresh tp=1 flags':

                    # Print and execute method
                    print("""gr.orchestrator.refresh()""")
                    gr.orchestrator.refresh()

            #------------------------#
            # Cache Management panel #
            #------------------------#
            elif 'cachemanage' in button_input_action:

                # Fetch action flags
                method_actions = ()
                for method_action_name in ['print', 'eval', 'commit']:
                    if gui['processing']['cachemanage']['actions'][method_action_name].get():
                        method_actions += (method_action_name,)
                print(f"method_actions =", method_actions)

                # Cache management apply views
                if button_input_action == 'cachemanage apply views':
                    print(f"""gr.cachemanager.apply_views(actions={method_actions})""")
                    gr.cachemanager.apply_views(actions=method_actions)

                # Cache management apply formulas
                elif button_input_action == 'cachemanage apply formulas':
                    verbose = False
                    if 'print' in method_actions:
                        verbose = True
                    print(f"""gr.cachemanager.apply_formulas(verbose={verbose})""")
                    gr.cachemanager.apply_formulas(verbose=verbose)

                # Cache management calculate scores matrix
                elif button_input_action == 'cachemanage calculate scores matrix':
                    types_to_process = gr.orchestrator.typeflags.status(types_only=True)
                    for from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type in types_to_process['edges']:
                        if flag_type == 'scores':
                            print(f"""gr.cachemanager.calculate_scores_matrix(from_object_type='{from_object_type}', to_object_type='{to_object_type}, actions={method_actions})""")
                            gr.cachemanager.calculate_scores_matrix(from_object_type=from_object_type, to_object_type=to_object_type, actions=method_actions)

                # Cache management consolidate scores matrix
                elif button_input_action == 'cachemanage consolidate scores matrix':
                    types_to_process = gr.orchestrator.typeflags.status(types_only=True)
                    for from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type in types_to_process['edges']:
                        if flag_type == 'scores':
                            print(f"""gr.cachemanager.consolidate_scores_matrix(from_object_type='{from_object_type}', to_object_type='{to_object_type}')""")
                            gr.cachemanager.consolidate_scores_matrix(from_object_type=from_object_type, to_object_type=to_object_type)



                # 'cachemanage apply views'
                # 'cachemanage apply formulas'
                # 'cachemanage calculate scores matrix'
                # 'cachemanage consolidate scores matrix'

                # 'indexdb cache_buildup info'
                # 'indexdb cache_buildup build docs fields'
                # 'indexdb cache_buildup build links parentchild'

                # 'indexdb page_profile info'
                # 'indexdb page_profile create table'
                # 'indexdb page_profile patch'

                # 'indexdb index_docs info'
                # 'indexdb index_docs create table'
                # 'indexdb index_docs patch'

                # 'indexdb index_doc_links info'
                # 'indexdb index_doc_links create table'
                # 'indexdb index_doc_links horiz. patch'
                # 'indexdb index_doc_links vert. patch'
                # 'indexdb index_doc_links vertical patch p2c'

                # 'indexdb index_docs_es info'
                # 'indexdb index_docs_es create table'
                # 'indexdb index_docs_es patch'

                # 'indexdb index_doc_links_es info'
                # 'indexdb index_doc_links_es create table'
                # 'indexdb index_doc_links_es horizontal patch'
                # 'indexdb index_doc_links_es vertical patch'

                # 'indexdb create mixed views'
                # 'indexdb copy patches to prod'

            #-----------------------------#
            # GraphIndex Management panel #
            #-----------------------------#
            elif 'indexdb' in button_input_action:

                # Fetch action flags
                method_actions = ()
                for method_action_name in ['print', 'eval', 'commit']:
                    if gui['processing']['indexdb']['actions'][method_action_name].get():
                        method_actions += (method_action_name,)
                print(f"method_actions =", method_actions)

                # Fetch doc types and dic-link types to process
                if 'indexdb index_docs' in button_input_action or 'indexdb index_doc_links' in button_input_action:
                    out = gr.orchestrator.typeflags.status(types_only=True)
                    doctypes_to_process = [o[1] for o in out['nodes']]
                    doclinktypes_to_process = [o[1]+'-'+o[3]+'-'+o[4] for o in out['edges']]

                # IndexDB cache buildup info
                if button_input_action == 'indexdb cache_buildup info':
                    print(f"""gr.indexdb.cachebuilder.info()""")
                    gr.indexdb.cachebuilder.info()

                # IndexDB cache buildup build all docs and link fields
                elif button_input_action == 'indexdb cache_buildup build all':
                    print(f"""gr.indexdb.cachebuilder.build_all(actions={method_actions})""")
                    gr.indexdb.cachebuilder.build_all(actions=method_actions)

                # IndexDB page profile info
                elif button_input_action == 'indexdb page_profile info':
                    print(f"""gr.indexdb.pageprofile.info()""")
                    gr.indexdb.pageprofile.info()

                # IndexDB page profile create table
                elif button_input_action == 'indexdb page_profile create table':
                    print(f"""gr.indexdb.pageprofile.create_table(actions={method_actions})""")
                    gr.indexdb.pageprofile.create_table(actions=method_actions)

                # IndexDB page profile patch
                elif button_input_action == 'indexdb page_profile patch':
                    print(f"""gr.indexdb.pageprofile.patch(actions={method_actions})""")
                    gr.indexdb.pageprofile.patch(actions=method_actions)

                # IndexDB index docs info
                elif button_input_action == 'indexdb index_docs info':
                    for doc_type in doctypes_to_process:
                        print(f"""gr.indexdb.idocs[doc_type].info()""")
                        gr.indexdb.idocs[doc_type].info()

                # IndexDB index docs create table
                elif button_input_action == 'indexdb index_docs create table':
                    for doc_type in doctypes_to_process:
                        print(f"""gr.indexdb.idocs['{doc_type}'].create_table(actions={method_actions})""")
                        gr.indexdb.idocs[doc_type].create_table(actions=method_actions)

                # IndexDB index docs patch
                elif button_input_action == 'indexdb index_docs patch':
                    for doc_type in doctypes_to_process:
                        print(f"""gr.indexdb.idocs['{doc_type}'].patch(actions={method_actions})""")
                        gr.indexdb.idocs[doc_type].patch(actions=method_actions)

                # IndexDB index doc-links info
                elif button_input_action == 'indexdb index_doc_links info':
                    for doc_link_type_subtype in doclinktypes_to_process:
                        doc_type, link_type, flag_type = doc_link_type_subtype.split('-')
                        for link_subtype in ['ORG', 'SEM']:
                            if link_subtype in gr.indexdb.idoclinks[doc_type][link_type]:
                                print(f"""gr.indexdb.idoclinks['{doc_type}']['{link_type}']['{link_subtype}'].info()""")
                                gr.indexdb.idoclinks[doc_type][link_type][link_subtype].info()

                # IndexDB index doc-links create table
                elif button_input_action == 'indexdb index_doc_links create table':
                    for doc_link_type_subtype in doclinktypes_to_process:
                        doc_type, link_type, flag_type = doc_link_type_subtype.split('-')
                        for link_subtype in ['ORG', 'SEM']:
                            if link_subtype in gr.indexdb.idoclinks[doc_type][link_type]:
                                print(f"""gr.indexdb.idoclinks['{doc_type}']['{link_type}']['{link_subtype}'].create_table(actions={method_actions})""")
                                gr.indexdb.idoclinks[doc_type][link_type][link_subtype].create_table(actions=method_actions)

                # IndexDB index doc-links horizontal patch
                elif button_input_action == 'indexdb index_doc_links horiz. patch':
                    for doc_link_type_subtype in doclinktypes_to_process:
                        doc_type, link_type, flag_type = doc_link_type_subtype.split('-')
                        for link_subtype in ['ORG', 'SEM']:
                            if link_subtype in gr.indexdb.idoclinks[doc_type][link_type]:
                                print(f"""gr.indexdb.idoclinks['{doc_type}']['{link_type}']['{link_subtype}'].horizontal_patch(actions={method_actions})""")
                                gr.indexdb.idoclinks[doc_type][link_type][link_subtype].horizontal_patch(actions=method_actions)

                # IndexDB index doc-links vertical patch
                elif button_input_action == 'indexdb index_doc_links vert. patch':
                    for doc_link_type_subtype in doclinktypes_to_process:
                        doc_type, link_type, flag_type = doc_link_type_subtype.split('-')
                        for link_subtype in ['ORG', 'SEM']:
                            if link_subtype in gr.indexdb.idoclinks[doc_type][link_type]:
                                print(f"""gr.indexdb.idoclinks['{doc_type}']['{link_type}']['{link_subtype}'].vertical_patch(actions={method_actions})""")
                                gr.indexdb.idoclinks[doc_type][link_type][link_subtype].vertical_patch(actions=method_actions)

                # IndexDB index doc-links vertical patch parent-child
                elif button_input_action == 'indexdb index_doc_links vertical patch p2c':
                    for doc_link_type_subtype in doclinktypes_to_process:
                        doc_type, link_type, flag_type = doc_link_type_subtype.split('-')
                        for link_subtype in ['ORG', 'SEM']:
                            if link_subtype in gr.indexdb.idoclinks[doc_type][link_type]:
                                print(f"""gr.indexdb.idoclinks['{doc_type}']['{link_type}']['{link_subtype}'].vertical_patch_parent_child(actions={method_actions})""")
                                gr.indexdb.idoclinks[doc_type][link_type][link_subtype].vertical_patch_parentchild(actions=method_actions)

                # IndexDB index docs ES info
                elif button_input_action == 'indexdb index_docs_es info':
                    pass

                # IndexDB index docs ES create table
                elif button_input_action == 'indexdb index_docs_es create table':
                    pass

                # IndexDB index docs ES patch
                elif button_input_action == 'indexdb index_docs_es patch':
                    pass

                # IndexDB index doc links ES info
                elif button_input_action == 'indexdb index_doc_links_es info':
                    pass

                # IndexDB index doc links ES create table
                elif button_input_action == 'indexdb index_doc_links_es create table':
                    pass

                # IndexDB index doc links ES horizontal patch
                elif button_input_action == 'indexdb index_doc_links_es horizontal patch':
                    pass

                # IndexDB index doc links ES vertical patch
                elif button_input_action == 'indexdb index_doc_links_es vertical patch':
                    pass

                # IndexDB create mixed views
                elif button_input_action == 'indexdb create mixed views':
                    pass

                # IndexDB copy patches to prod
                elif button_input_action == 'indexdb copy patches to prod':
                    pass

    #--------------------------------#
    # Subframe (left): Orchestration #
    #--------------------------------#
    if True:

        # Labeled subframe
        gui['orchestration']['subframe'] = tk.LabelFrame(root, text="Orchestration", width=500, padx=10, pady=10)
        gui['orchestration']['subframe'].grid_propagate(False)
        gui['orchestration']['subframe'].grid(row=0, column=0, sticky='ns', padx=(16,8), pady=(16,16))

        # Description inside subframe
        gui['orchestration']['description'] = tk.Label(gui['orchestration']['subframe'],
            text = "Select which type of objects to process, whether to process fields and/or scores, to sync new inserted or deleted data, and to reset or propagate \"to_process\" flags over all cache dependencies.",
            justify='left', anchor='w', fg='gray', wraplength=400
        ).pack(pady=(0,0), anchor='w', fill='x')

        #--------------------------------------------#
        def add_type_to_process(unit, pre_selection=None):

            # Extract pre-selection if provided
            if pre_selection is not None:
                source_node_type, target_node_type, process_fields, process_scores = pre_selection
            else:
                source_node_type, target_node_type, process_fields, process_scores = False, False, False, False

            # Create a new row frame
            row_frame = tk.Frame(gui['orchestration'][f'{unit}_types']['subframe'])
            row_frame.pack(fill='x', pady=0)

            # Dropdown: Source node types
            source_node_type_var = tk.StringVar()
            source_node_dropdown = ttk.Combobox(row_frame, values=list_of_node_types, textvariable=source_node_type_var, width=8)
            source_node_dropdown.grid(row=0, column=0, padx=(0, 0))
            if pre_selection:
                source_node_dropdown.after_idle(lambda: source_node_dropdown.set(source_node_type))
            
            # Dropdown: Target node types (if edge)
            target_node_dropdown = None
            if unit == 'edge':
                target_node_type_var = tk.StringVar(value=target_node_type if pre_selection else list_of_node_types[0])
                target_node_dropdown = ttk.Combobox(row_frame, values=list_of_node_types, textvariable=target_node_type_var, width=8)
                target_node_dropdown.grid(row=0, column=1, padx=(0, 0))
                if pre_selection:
                    target_node_dropdown.after_idle(lambda: target_node_dropdown.set(target_node_type))

            # Checkbox: Process fields 
            pf_var = tk.BooleanVar()
            pf_var.set(bool(process_fields))
            pf_label = tk.Label(row_frame, text="Fields")
            pf_label.grid(row=0, column=2, padx=(0, 0))
            pf_checkbox = tk.Checkbutton(row_frame, variable=pf_var)
            # pf_checkbox.select() if process_fields else pf_checkbox.deselect()
            pf_checkbox.grid(row=0, column=3, padx=(0, 0))

            # Checkbox: Process scores
            ps_var = tk.BooleanVar()
            ps_var.set(bool(process_scores))
            ps_label = tk.Label(row_frame, text="Scores")
            ps_label.grid(row=0, column=4, padx=(0, 0))
            ps_checkbox = tk.Checkbutton(row_frame, variable=ps_var)
            # ps_checkbox.select() if process_scores else ps_checkbox.deselect()
            ps_checkbox.grid(row=0, column=5, padx=(0, 0))

            # Remove button
            def remove_this_row():
                row_frame.destroy()
                gui['orchestration'][f'{unit}_types']['list'].remove(row_data)

            remove_button = tk.Button(row_frame, text="Remove", command=remove_this_row)
            remove_button.grid(row=0, column=6, padx=(0, 0))

            # Optionally store widget references if needed later
            # Store row data
            row_data = {
                'frame': row_frame,
                'dropdowns': [source_node_dropdown, target_node_dropdown],
                'process_fields': pf_var,
                'process_scores': ps_var,
            }
            gui['orchestration'][f'{unit}_types']['list'].append(row_data)

        # Sub-subframe: Node types to process
        if True:

            # Labeled subframe
            gui['orchestration']['node_types']['subframe'] = tk.LabelFrame(gui['orchestration']['subframe'], text="Node types to process", width=480, padx=10, pady=10)
            gui['orchestration']['node_types']['subframe'].pack_propagate(False)
            gui['orchestration']['node_types']['subframe'].pack(padx=10, pady=(10,6), fill='y', expand=True)
            
            # Description inside subframe
            gui['orchestration']['node_types']['description'] = tk.Label(gui['orchestration']['node_types']['subframe'],
                text = f"Add and remove node types to process.",
                justify='left', anchor='w', fg='gray', wraplength=380
            ).pack(pady=(0,0), anchor='w', fill='x')

            # Buttons to add/remove dropdowns
            gui['orchestration']['node_types']['button_add_new'] = tk.Button(gui['orchestration']['node_types']['subframe'],
                text = "Add node type",
                command = (lambda: add_type_to_process('node'))
            ).pack(pady=(10, 5), anchor='w')

        # Sub-subframe: Edge types to process
        if True:

            # Labeled subframe
            gui['orchestration']['edge_types']['subframe'] = tk.LabelFrame(gui['orchestration']['subframe'], text=f"Edge types to process", width=480, padx=10, pady=10)
            gui['orchestration']['edge_types']['subframe'].pack_propagate(False)
            gui['orchestration']['edge_types']['subframe'].pack(padx=10, pady=(6,12), fill='y', expand=True)

            # Description inside subframe
            gui['orchestration']['edge_types']['description'] = tk.Label(gui['orchestration']['edge_types']['subframe'],
                text = f"Add and remove edge types to process.",
                justify='left', anchor='w', fg='gray', wraplength=380
            ).pack(pady=(0,0), anchor='w', fill='x')

            # Buttons to add/remove dropdowns
            gui['orchestration']['edge_types']['button_add_new'] = tk.Button(gui['orchestration']['edge_types']['subframe'],
                text = "Add edge type",
                command = (lambda: add_type_to_process('edge'))
            ).pack(pady=(10, 5), anchor='w')

        # Create buttons row for cache management actions
        create_buttons_row(
            frame_pointer  = gui['orchestration']['subframe'],
            actions_matrix = [
                (0, 0, 7, 'status'    ), (0, 1, 11, 'reset type flags'), (0, 2, 11, 'reset object flags'), (0, 3, 11, 'propagate flags'),
                (1, 0, 7, 'apply'     ), (1, 1, 11, 'set type flags'  ), (1, 2, 11, 'set object flags'  ), (1, 3, 11, 'sync new data'  ),
                (2, 0, 7, 'randomize' ), (2, 1, 11, 'apply expire'    ), (2, 2, 11, 'refresh tp=1 flags')
            ],
            function_subspace = 'orchestration'
        )

        # Initialise typeflags from current saved settings
        typeflags_settings = gr.orchestrator.typeflags.status(types_only=True)
        for tfs in typeflags_settings['nodes']:
            add_type_to_process('node', pre_selection=(tfs[1], None  , tfs[2]=='fields', tfs[2]=='scores'))
        for tfs in typeflags_settings['edges']:
            add_type_to_process('edge', pre_selection=(tfs[1], tfs[3], tfs[4]=='fields', tfs[4]=='scores'))

    #------------------------------#
    # Subframe (right): Processing #
    #------------------------------#
    if True:

        # Labeled subframe
        gui['processing']['subframe'] = tk.LabelFrame(root, text="Processing", width=440, padx=10, pady=10)
        gui['processing']['subframe'].grid_propagate(False)
        gui['processing']['subframe'].grid(row=0, column=1, sticky='ns', padx=(8,16), pady=(16,16))

        #----------------------------#
        # Subframe: Cache Management #
        #----------------------------#
        if True:

            # Create labeled subframe
            gui['processing']['cachemanage']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="Cache management", width=400, height=122, padx=10, pady=10)
            gui['processing']['cachemanage']['subframe'].pack_propagate(False)
            gui['processing']['cachemanage']['subframe'].pack(padx=10, pady=10)

            # Create checkboxes row for cache management actions
            create_action_checkboxes_row(
                frame_pointer = gui['processing']['cachemanage']['subframe'],
                var_pointer   = gui['processing']['cachemanage']['actions']
            )

            # Create buttons row for cache management actions
            create_buttons_row(
                frame_pointer  = gui['processing']['cachemanage']['subframe'],
                actions_matrix = [
                    (0, 0, 9, 'apply views'   ), (0, 1, 16, 'calculate scores matrix'  ),
                    (1, 0, 9, 'apply formulas'), (1, 1, 16, 'consolidate scores matrix')
                ],
                function_subspace = 'cachemanage'
            )

        #------------------------------------------------#
        # Subframe: GraphIndex Management (SQL Database) #
        #------------------------------------------------#
        if True:

            # Labeled subframe
            gui['processing']['indexdb']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="GraphIndex management (MySQL)", width=400, height=550, padx=10, pady=10)
            gui['processing']['indexdb']['subframe'].pack_propagate(False)
            gui['processing']['indexdb']['subframe'].pack(padx=10, pady=(10,0))

            # Create checkboxes row for cache management actions
            create_action_checkboxes_row(
                frame_pointer  = gui['processing']['indexdb']['subframe'],
                var_pointer    = gui['processing']['indexdb']['actions'],
                include_engine = True
            )

            # Sub-subframes
            if True:
                            
                # Labeled subframe
                gui['processing']['indexdb']['cache_buildup']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Cache build up", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['cache_buildup']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['cache_buildup']['subframe'].pack(padx=10, pady=(10,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['cache_buildup']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 5, 'build all'),
                    ],
                    function_subspace = 'indexdb cache_buildup'
                )

                # Labeled subframe
                gui['processing']['indexdb']['page_profile']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Page profiles", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['page_profile']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['page_profile']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['page_profile']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 7, 'create table'), (0, 2, 3, 'patch'),
                    ],
                    function_subspace = 'indexdb page_profile'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_docs']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index docs", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['index_docs']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_docs']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_docs']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 7, 'create table'), (0, 2, 3, 'patch'),
                    ],
                    function_subspace = 'indexdb index_docs'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_doc_links']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index doc-links", width=380, height=88, padx=4, pady=4)
                gui['processing']['indexdb']['index_doc_links']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_doc_links']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_doc_links']['subframe'],
                    actions_matrix = [
                        (0, 0, 7, 'info'        ), (0, 1, 7, 'horiz. patch'), (0, 2, 11, 'vertical patch p2c'),
                        (1, 0, 7, 'create table'), (1, 1, 7, 'vert. patch'  ),
                    ],
                    function_subspace = 'indexdb index_doc_links'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_docs_es']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index docs (for ElacticSearch)", width=380, height=60, padx=4, pady=4)
                gui['processing']['indexdb']['index_docs_es']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_docs_es']['subframe'].pack(padx=10, pady=(2,2))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_docs_es']['subframe'],
                    actions_matrix = [
                        (0, 0, 2, 'info'), (0, 1, 7, 'create table'), (0, 2, 3, 'patch'),
                    ],
                    function_subspace = 'indexdb index_docs_es'
                )

                # Labeled subframe
                gui['processing']['indexdb']['index_doc_links_es']['subframe'] = tk.LabelFrame(gui['processing']['indexdb']['subframe'], text="Index doc-links (for ElacticSearch)", width=380, height=88, padx=4, pady=4)
                gui['processing']['indexdb']['index_doc_links_es']['subframe'].pack_propagate(False)
                gui['processing']['indexdb']['index_doc_links_es']['subframe'].pack(padx=10, pady=(2,10))

                # Create buttons row for cache management actions
                create_buttons_row(
                    frame_pointer  = gui['processing']['indexdb']['index_doc_links_es']['subframe'],
                    actions_matrix = [
                        (0, 0, 7, 'info'        ), (0, 1, 10, 'horizontal patch'),
                        (1, 0, 7, 'create table'), (1, 1, 10, 'vertical patch'  ),
                    ],
                    function_subspace = 'indexdb index_doc_links_es'
                )
            
            # Create buttons row for cache management actions
            create_buttons_row(
                frame_pointer  = gui['processing']['indexdb']['subframe'],
                actions_matrix = [
                    (0, 0, 12, 'create mixed views'), (0, 1, 13, 'copy patches to prod'),
                ],
                function_subspace = 'indexdb'
            )

        #--------------------------------------------------#
        # Subframe: Graph Index Management (ElasticSearch) #
        #--------------------------------------------------#
        if True:

            # Labeled subframe
            gui['processing']['elasticsearch']['subframe'] = tk.LabelFrame(gui['processing']['subframe'], text="GraphIndex management (ElasticSearch)", width=400, height=330, padx=10, pady=10)
            gui['processing']['elasticsearch']['subframe'].pack_propagate(False)
            gui['processing']['elasticsearch']['subframe'].pack(padx=10, pady=10)
    
    # Launch GUI
    root.mainloop()

#==================================#
# Main: >> python graphregistry.py #
#==================================#
if __name__ == '__main__':

    exit()

    from graphregistry import GraphRegistry
    gr = GraphRegistry()
    node = gr.Node()
    node.set(('EPFL','Publication','148964'))
    node.set_from_existing()
    node.detect_concepts()
    node.set_text_source('abstract')
    node.commit_concepts(actions=('eval'))
    node.commit_concepts(actions=('commit'))
    node.commit_concepts(actions=('eval'))


    list_of_tables = gr.db.get_tables_in_schema(
        engine_name = 'prod',
        schema_name = 'graphsearch_prod_2025_02_10',
        include_views = False
    )

    for table_name in list_of_tables:
        gr.db.compare_tables_by_random_sampling(
            source_engine_name = 'prod',
            source_schema_name = 'graphsearch_prod_2025_02_10',
            source_table_name  = table_name,
            target_engine_name = 'prod',
            target_schema_name = 'graphsearch_prod',
            target_table_name  = table_name,
            sample_size        = 8
        )


    list_of_tables = gr.db.get_tables_in_schema(
        engine_name = 'prod',
        schema_name = 'graphsearch_prod',
        include_views = False,
        use_regex = [r'.*Widget.*']
    )

    for table_name in list_of_tables:
        print('Copying table:', table_name)
        gr.db.copy_table(
            engine_name           = 'prod',
            source_schema_name    = 'graphsearch_prod',
            source_table_name     = table_name,
            target_schema_name    = 'graphsearch_prod_2025_02_10',
            target_table_name     = table_name,
            list_of_columns       = False,
            where_condition       = 'TRUE',
            row_id_name           = 'row_id',
            chunk_size            = 100000,
            create_table          = True,
            drop_keys             = False,
            use_replace_or_ignore = 'IGNORE'
        )


    exit()
    
    # gr.db.copy_table_across_engines(
    #     source_engine_name = 'test',
    #     source_schema_name = 'graph_cache',
    #     source_table_name  = 'IndexBuildup_Fields_Docs_Widget',
    #     target_engine_name = 'prod',
    #     target_schema_name = 'graph_cache',
    #     keys_json  = table_keys_json['doc_profile'],
    #     filter_by  = 'to_process > 0.5',
    #     chunk_size = 100000,
    #     drop_table = False
    # )

    # gr.db.copy_table_across_engines(
    #     source_engine_name = 'test',
    #     source_schema_name = 'graph_cache',
    #     source_table_name  = 'Edges_N_Object_N_Object_T_ScoresMatrix_AS',
    #     target_engine_name = 'prod',
    #     target_schema_name = 'graph_cache',
    #     keys_json  = table_keys_json['object_to_object'],
    #     filter_by  = 'to_process > 0.5',
    #     chunk_size = 100000,
    #     drop_table = False
    # )



    # # get_table_type_from_name    

    # pass


    # Config object types to process
    gr.orchestrator.config(
        node_types = [('EPFL', 'Lecture')],
        edge_types = [],
        sync  = False,
        reset = False,
        print = True
    )
