#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from api.core.config import GlobalConfig
from elasticsearch import Elasticsearch as ElasticSearchEngine, helpers, ElasticsearchWarning
from loguru import logger as sysmsg
from urllib.parse import quote
from flatten_dict import flatten
import numpy as np
import os, sys, time, rich, gzip, json, subprocess, warnings, logging, random

# Initialize global config
glbcfg = GlobalConfig()

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

# ElasticSearch settings and mappings (old version that doens't pass test)
es_settings_and_mappings_OLD = {
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

# ElasticSearch settings and mappings (corrected version)
es_settings_and_mappings = {
    "aliases": {},
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "raw": {
                        "tokenizer": "keyword",
                        "filter": ["lowercase"]
                    },
                    "base_en": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "stemmer_en"]
                    },
                    "base_fr": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "stemmer_fr"]
                    },
                    "synonym_en": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "stemmer_en", "synonym_en"]
                    },
                    "trigram": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "shingle"]
                    }
                },
                "filter": {
                    # "shingle": {
                    #     "type": "shingle",
                    #     "min_shingle_size": 2,
                    #     "max_shingle_size": 3
                    # },
                    "stemmer_en": {
                        "type": "stemmer",
                        "language": "light_english"
                    },
                    "stemmer_fr": {
                        "type": "stemmer",
                        "language": "light_french"
                    },
                    "synonym_en": {
                        "type": "synonym_graph",
                        "synonyms": ["computational complexity , algorithmic complexity"]
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "name": {
                "properties": {
                    "en": {
                        "type": "text",
                        "analyzer": "base_en",
                        "search_analyzer": "synonym_en",
                        "fields": {
                            "raw": {
                                "type": "text",
                                "analyzer": "raw"
                            },
                            "sayt": {
                                "type": "search_as_you_type",
                                "analyzer": "base_en",
                                "doc_values": False,
                                "max_shingle_size": 3
                            },
                            "trigram": {
                                "type": "text",
                                "analyzer": "trigram"
                            }
                        }
                    },
                    "fr": {
                        "type": "text",
                        "analyzer": "base_fr",
                        # "search_analyzer": "base_fr",
                        "fields": {
                            "raw": {
                                "type": "text",
                                "analyzer": "raw"
                            },
                            "sayt": {
                                "type": "search_as_you_type",
                                "analyzer": "base_fr",
                                "doc_values": False,
                                "max_shingle_size": 3
                            },
                            "trigram": {
                                "type": "text",
                                "analyzer": "trigram"
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
                        "search_analyzer": "synonym_en"
                    },
                    "fr": {
                        "type": "text",
                        "analyzer": "base_fr",
                        # "search_analyzer": "base_fr"
                    }
                }
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

#---------------------#
# Auxiliary functions #
#---------------------#

# Handle progress output
def es_write_progress(prog, total, status=''):
    sys.stdout.write(f"\r{prog} of {total} documents indexed... {status}")
    sys.stdout.flush()

#---------------------#
#---------------------#
#---------------------#

# Suppress only the ElasticsearchWarning
warnings.filterwarnings('ignore', category=ElasticsearchWarning)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')
logger.setLevel(logging.DEBUG)


#-------------------------------------------------#
# Class definition for Graph ElasticSearch engine #
#-------------------------------------------------#
class GraphIndex():

    # Class variable to hold the single instance
    _instance = None

    # Create new instance of class before __init__ is called
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    # Class constructor
    def __init__(self, name="GraphIndex"):

        # Check if the instance is already initialized
        if not self._initialized:
            self.name = name
            self._initialized = True
            print(f"GraphIndex initialized with name: {self.name}")

        # Initiate the ElasticSearch engines
        self.params_test, self.engine_test = self.initiate_engine(glbcfg.settings['elasticsearch']['server_test'])
        self.params_prod, self.engine_prod = self.initiate_engine(glbcfg.settings['elasticsearch']['server_prod'])
        self.params = {'test': self.params_test, 'prod': self.params_prod}
        self.engine = {'test': self.engine_test, 'prod': self.engine_prod}

    #---------------------------------------------#
    # Method: Initialize the ElasticSearch engine #
    #---------------------------------------------#
    def initiate_engine(self, server_name):
        """
        Initialize the ElasticSearch engine (no SSL) based on the server name provided.
        """

        # Check if the server name is in the global configuration
        if server_name not in glbcfg.settings['elasticsearch']:
            raise ValueError(
                f"Could not find configuration for Elasticsearch server '{server_name}' in global config."
            )

        # Load parameters
        params = glbcfg.settings['elasticsearch'][server_name]

        # Build connection URL (HTTP only, no SSL)
        if "password" in params and params["password"]:
            es_hosts = f"https://{params['username']}:{quote(params['password'])}@{params['host']}:{params['port']}"
        elif "username" in params and params["username"]:
            es_hosts = f"https://{params['username']}@{params['host']}:{params['port']}"
        else:
            es_hosts = f"https://{params['host']}:{params['port']}"

        # Initialize Elasticsearch engine (no SSL)
        engine = ElasticSearchEngine(
            hosts           = [es_hosts],
            http_compress   = True,
            verify_certs    = True,
            ca_certs        = glbcfg.settings['elasticsearch']['graph_engine_test']['cert_file'],
            request_timeout = 3600
        )

        # Return parameters and engine instance
        return params, engine

    #---------------------------------------#
    # Method: Test ElasticSearch connection #
    #---------------------------------------#
    def test(self, engine_name):
        """
        Test the connection to the ElasticSearch engine.

        Returns:
            True  -> Connection successful
            False -> Connection failed
            None  -> Unexpected error
        """
        # Check if the engine name is valid
        if engine_name not in self.engine:
            raise ValueError(f"Engine '{engine_name}' not found in the GraphIndex instance.")

        try:
            # Perform a simple operation to test the connection
            self.engine[engine_name].info()
            return True
        except ConnectionError as e:
            # Could not connect to the server
            sysmsg.error(f"Failed to connect to ElasticSearch '{engine_name}': {e}")
            return False
        except Exception as e:
            # Catch-all for unexpected issues
            sysmsg.critical(f"Unexpected error while connecting to ElasticSearch '{engine_name}': {e}")
            return None

    #-------------------------------#
    # Method: Get index information #
    #-------------------------------#
    def info(self, engine_name):
        """
        Get information about the ElasticSearch index.
        """

        # Print the index information
        rich.print_json(data=dict(self.engine[engine_name].info()))

    #---------------------------------#
    # Method: Get cluster health info #
    #---------------------------------#
    def cluster_health(self, engine_name):
        """
        Get the health status of the ElasticSearch cluster.
        """
        # Equivalent to: GET /_cluster/health?pretty
        rich.print_json(data=dict(self.engine[engine_name].cluster.health()))

    #-------------------------------------#
    # Method: Get cluster allocation info #
    #-------------------------------------#
    def cluster_allocation_explain(self, engine_name):
        """
        Get the cluster allocation explain information.
        """
        # Equivalent to: GET /_cluster/allocation/explain?pretty
        rich.print_json(data=dict(self.engine[engine_name].cluster.allocation_explain()))

    #-----------------------------#
    # Method: Get list of indexes #
    #-----------------------------#
    def index_list(self, engine_name, display_size=False):
        """
        Get a list of indexes in the ElasticSearch engine.
        """
        # Check for level of detail requested
        acc_str = ''
        if display_size:
            # Equivalent to: GET /_cat/indices?v&s=index
            index_sizes = []
            for index in self.engine[engine_name].indices.get(index="*"):
                if not index.startswith('.'):
                    index_sizes += [(index, self.engine[engine_name].indices.stats(index=index)['indices'][index]['total']['store']['size_in_bytes'])]
            for index, index_size in sorted(index_sizes, key=lambda x: x[0], reverse=False):
                acc_str += f' - {index} ({index_size/1024/1024/1024:.2f} GB)\n'
        else:
            # Equivalent to: GET /_cat/indices?v
            for index in self.engine[engine_name].indices.get(index="*"):
                if not index.startswith('.'):
                    acc_str += f' - {index}\n'
        if acc_str == '':
            print(f"\n\033[33mNo indexes found on {engine_name}.\033[0m\n")
        else:
            print(f"\n\033[32mList of indexes on {engine_name}:\033[0m")
            print(f"{acc_str}\n")

    #-------------------------------#
    # Method: Drop an index by name #
    #-------------------------------#
    def drop_index(self, engine_name, index_name):
        """
        Drop an index by name in the ElasticSearch engine.
        """
        try:
            # Ask for confirmation before deleting the index
            confirmation = input(f"Are you sure you want to delete the index '{index_name}' on '{engine_name}'? (yes/no): ")
            if confirmation.lower() != 'yes':
                print("Index deletion cancelled.")
                return
            # Equivalent to: DELETE /<index_name>
            self.engine[engine_name].indices.delete(index=index_name)
            print(f'Index {index_name} deleted')
        except:
            print(f'Index {index_name} does not exist')
            pass

    #-----------------------------#
    # Method: Get list of aliases #
    #-----------------------------#
    def alias_list(self, engine_name):
        """
        Get a list of aliases in the ElasticSearch engine.
        """
        # Equivalent to: GET /_cat/aliases?v&s=alias
        acc_str = ''
        aliases = self.engine[engine_name].indices.get_alias()
        alias_to_index = {}
        for index, alias_info in aliases.items():
            if not index.startswith('.'):
                for alias in alias_info['aliases']:
                    if not alias.startswith('.'):
                        alias_to_index[alias] = index
        for alias, index in alias_to_index.items():
            acc_str += f" - {alias} --> {index}\n"
        if acc_str == '':
            print(f"\n\033[33mNo aliases found on {engine_name}.\033[0m\n")
        else:
            print(f"\n\033[32mList of aliases on {engine_name}:\033[0m")
            print(f"{acc_str}\n")

    #-----------------------------------#
    # Method: Set an alias for an index #
    #-----------------------------------#
    def set_alias(self, engine_name, alias_name, index_name):
        """
        Set an alias for an index in the ElasticSearch engine.
        """
        # Get the ElasticSearch engine instance
        es = self.engine[engine_name]

        # Check if the alias exists
        existing_aliases = es.indices.get_alias(name=alias_name, ignore=404)

        # Prepare actions for updating aliases
        actions = []

        # If the alias already exists, remove it from existing indices
        # This is to ensure that the alias points to the new index only
        if existing_aliases and existing_aliases.get('status', 200) == 200:

            # Remove alias from existing indices
            for existing_index in existing_aliases.keys():
                actions.append({"remove": {"index": existing_index, "alias": alias_name}})

            # If there are actions to remove the alias, execute them
            if actions:
                es.indices.update_aliases(body={"actions": actions})
                print(f"Removed alias '{alias_name}' from indices: {', '.join(existing_aliases.keys())}")

        # Add alias to the new index
        actions.append({"add": {"index": index_name, "alias": alias_name}})
        es.indices.update_aliases(body={"actions": actions})
        print(f"Alias '{alias_name}' now points to index '{index_name}'")

    #-------------------------------------#
    # Method: Drop an alias from an index #
    #-------------------------------------#
    def drop_alias(self, engine_name, alias_name):
        """
        Drop an alias from all indices in the ElasticSearch engine.
        """
        # Get the ElasticSearch engine instance
        es = self.engine[engine_name]

        # Check if the alias exists
        existing_aliases = es.indices.get_alias(name=alias_name, ignore=404)

        # Prepare actions for removing the alias
        actions = []

        # If the alias exists, remove it from all indices
        # This is to ensure that the alias is removed from all indices it points to
        if existing_aliases and existing_aliases.get('status', 200) == 200:
            # Remove alias from existing indices
            for existing_index in existing_aliases.keys():
                actions.append({"remove": {"index": existing_index, "alias": alias_name}})

        # If there are actions to remove the alias, execute them
        if actions:
            # Execute the actions to remove the alias
            es.indices.update_aliases(body={"actions": actions})
            print(f"Removed alias '{alias_name}' from indices: {', '.join(existing_aliases.keys())}")

    #-------------------------------------#
    # Method: Create an index from a file #
    #-------------------------------------#
    def import_index_from_file(self, engine_name, index_name, index_file, chunk_size=10000, delete_if_exists=False, force_replace=False):
        """
        Create an index in the ElasticSearch engine from a file.
        """
        # Define function to sort dictionary keys alphabetically
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

        # Delete index if exists?
        if delete_if_exists:
            try:
                # Ask for confirmation before deleting the index
                if force_replace:
                    confirmation = 'yes'
                else:
                    confirmation = input(f"Are you sure you want to delete the index '{index_name}' on '{engine_name}'? (yes/no): ")
                if confirmation.lower() != 'yes':
                    print("Index deletion cancelled.")
                    return
                # Delete the index if it exists
                self.engine[engine_name].indices.delete(index=index_name)
                print(f'Index {index_name} deleted')
            except:
                print(f'Index {index_name} does not exist')
                pass

        # Else, check if index exists and return if it does
        else:
            if self.engine[engine_name].indices.exists(index=index_name):
                sysmsg.error(f'❌ Index {index_name} already exists.')
                return

        # Create the new index with settings and mappings
        print(f'Creating index {index_name} with custom settings and mappings...')
        self.engine[engine_name].indices.create(index=index_name, body=es_settings_and_mappings)
        print(f'Index {index_name} created.')

        # Verify that the new index was created with the correct settings and mappings (fail and exit if not)
        print(f'Verifying index {index_name} settings and mappings...')
        index_info = self.engine[engine_name].indices.get(index=index_name)
        condition_1 = sort_dict_alphabetically(index_info[index_name]['settings']['index']['analysis']) == sort_dict_alphabetically(es_settings_and_mappings['settings']['index']['analysis'])
        condition_2 = sort_dict_alphabetically(index_info[index_name]['mappings']) == sort_dict_alphabetically(es_settings_and_mappings['mappings'])

        # If the conditions are not met, print an error message and exit
        if not condition_1 or not condition_2:
            print(f"Error: Index {index_name} was not created with the expected settings and mappings.")
            print("Expected settings and mappings:")
            rich.print_json(data=sort_dict_alphabetically(es_settings_and_mappings))
            print("Actual settings and mappings:")
            rich.print_json(data=sort_dict_alphabetically(index_info[index_name]))
            return False

        # Read the index settings and mappings from the file
        print(f'Reading index {index_file} ...')
        if index_file.endswith('.gz'):
            with gzip.open(index_file, 'rt', encoding='utf-8') as file:
                es_index = json.load(file)
        else:
            # Open the file normally if it is not gzipped
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

    #-------------------------------------#
    # Method: Execute a query on an index #
    #-------------------------------------#
    def execute_query(self, engine_name, index_name, query):
        """
        Execute a query on an index in the ElasticSearch engine.
        """
        # Return the search results for the given index and query
        return self.engine[engine_name].search(index=index_name, body=query)

    #---------------------------------------------#
    # Method: Fetch documents by ID from an index #
    #---------------------------------------------#
    def fetch_docs_by_id(self, engine_name, index_name, doc_ids_list):
        """
        Fetch documents by ID from an index in the ElasticSearch engine.
        """
        # Return the documents for the given index and list of document IDs
        return self.engine[engine_name].mget(index=index_name, body={"ids": doc_ids_list})

    #-----------------------------------#
    # Method: Copy index across engines #
    #-----------------------------------#
    def copy_index_across_engines(self, source_engine_name, target_engine_name, index_name, rename_to=None, chunk_size=1000):

        # Define the index names
        index_name_source = index_name
        index_name_target = index_name
        if rename_to is not None:
            index_name_target = rename_to

        # Define the parameters for the ElasticDump command
        params_server_source = f"https://{self.params[source_engine_name]['username']}:{quote(self.params[source_engine_name]['password'])}@{self.params[source_engine_name]['host']}:{self.params[source_engine_name]['port']}/{index_name_source}"
        params_server_target = f"https://{self.params[target_engine_name]['username']}:{quote(self.params[target_engine_name]['password'])}@{self.params[target_engine_name]['host']}:{self.params[target_engine_name]['port']}/{index_name_target}"
        base_command = [glbcfg.settings['elasticsearch']['dump_bin'], f"--input={params_server_source}", f"--output={params_server_target}", f"--input-ca={self.params[source_engine_name]['cert_file']}", f"--output-ca={self.params[target_engine_name]['cert_file']}", f"--limit={chunk_size}"]

        # Copy the index from test to prod
        for type in ['settings', 'mapping', 'data']:
            subprocess.run(base_command + [f"--type={type}"], env={**os.environ, "NODE_TLS_REJECT_UNAUTHORIZED": "0"})

    #--------------------------------------------------#
    # Method: Generate a random sample of document IDs #
    #--------------------------------------------------#
    def get_random_doc_id_set(self, engine_name, index_name, sample_size=100, partition_by=None, filter_by=None):

        # Generate ElasticSearch query for random sampling
        es_query = {
            "size": 0,
            "aggs": {
                f"by_{partition_by}": {
                    "terms": {
                        "field": f"{partition_by}.keyword",
                        "size": 10
                    },
                    "aggs": {
                        "sample_docs": {
                            "top_hits": {
                                "size": sample_size,
                                "_source": False,
                                "sort": [
                                    {
                                        "_script": {
                                            "type": "number",
                                            "script": {
                                                "lang": "painless",
                                                "source": "Math.random()"
                                            },
                                            "order": "asc"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }

        # Add filter_by if provided
        if filter_by:
            es_query["query"] = {
                "bool": {
                    "filter": [
                        {
                            "terms": {
                                "doc_type.keyword": filter_by
                            }
                        }
                    ]
                }
            }

        # Execute the query on the specified index
        out = self.execute_query(engine_name=engine_name, index_name=index_name, query=es_query)

        # Extract the random document IDs from the query results
        random_doc_id_set = set()
        if out and 'aggregations' in out:
            for bucket in out['aggregations'][f"by_{partition_by}"]['buckets']:
                for doc in bucket['sample_docs']['hits']['hits']:
                    random_doc_id_set.add(doc['_id'])

        # Return the random sample
        return sorted(list(random_doc_id_set))

    #-----------------------------------------#
    # Method: Extract documents by doc id set #
    #-----------------------------------------#
    def get_docs_by_id_set(self, engine_name, index_name, doc_ids, drop_fields=None, flatten_output=False):
        """
        Fetch documents from an index by a set of document IDs.
        """

        # Try to fetch documents by ID
        try:
            # Fetch documents by ID using mget
            response = self.engine[engine_name].mget(index=index_name, body={"ids": doc_ids})

            # Extract the documents from the response
            docs = [doc['_source'] for doc in response['docs'] if doc.get('found', False)]

            # Rearrange documents such that keys are (doc_type, doc_id) tuples
            docs = {
                (doc.get('doc_type'), doc.get('doc_id')): {
                    k: v for k, v in doc.items() if k not in ('doc_type', 'doc_id')
                }
                for doc in docs
                if 'doc_type' in doc and 'doc_id' in doc
            }

            # If drop_fields is specified, remove those fields from the documents
            if drop_fields:
                for k in docs:
                    docs[k] = {k2: v2 for k2, v2 in docs[k].items() if k2 not in drop_fields}

            # Flatten the source and target documents
            if flatten_output:
                docs = {k: flatten(v, reducer='dot', enumerate_types=(list,)) for k, v in docs.items()}

            # Return the documents
            return docs

        # If an error occurs, log the error and return an empty list
        except Exception as e:
            sysmsg.error(f"Failed to fetch documents from index '{index_name}' on '{engine_name}': {e}")
            return []

        # If no documents found, return an empty list
        return []

    #-----------------------------------------#
    #-----------------------------------------#
    def compare_indexes_by_random_sampling(self, engine_name_old, index_name_old, engine_name_new, index_name_new, sample_size=1024, doc_types=None):
        """
        Compare two Elasticsearch indices by random sampling of documents.
        Prints stats on new, deleted, matching, mismatching docs, etc.
        """

        # Check indices exist, return if not
        if not self.engine[engine_name_old].indices.exists(index=index_name_old):
            sysmsg.error(f"🚨 Source index '{index_name_old}' not found on '{engine_name_old}'.")
            return
        if not self.engine[engine_name_new].indices.exists(index=index_name_new):
            sysmsg.error(f"🚨 Target index '{index_name_new}' not found on '{engine_name_new}'.")
            return

        # Set maximum batch size
        MAX_BATCH_SIZE = 64

        # Support function that returns list of batch sizes for the given sample size
        def get_batches():
            full_batches = sample_size // MAX_BATCH_SIZE
            remainder = sample_size % MAX_BATCH_SIZE
            batch_sizes = [MAX_BATCH_SIZE] * full_batches
            if remainder > 0:
                batch_sizes.append(remainder)
            return batch_sizes

        # Get batch sizes
        batch_sizes = get_batches()

        #---------------------------------------------------#
        # Generate random sample of ElasticSearch documents #
        #---------------------------------------------------#

        # Were doc types not provided?
        if doc_types is None:
            doc_types = [d for _,d in index_doc_types_list]

        # Get number of doc types
        n_doc_types = len(doc_types)

        # Initialize sets and dicts
        doc_set_old = {}
        doc_set_new = {}

        # Get random samples by batches
        for batch_size in batch_sizes:

            # Get random doc id set
            random_doc_id_set  = self.get_random_doc_id_set(engine_name=engine_name_old, index_name=index_name_old, sample_size=round(batch_size/n_doc_types/2), partition_by='doc_type', filter_by=doc_types)
            random_doc_id_set += self.get_random_doc_id_set(engine_name=engine_name_new, index_name=index_name_new, sample_size=round(batch_size/n_doc_types/2), partition_by='doc_type', filter_by=doc_types)

            # Make random set unique
            unique_ids = sorted(list(set(random_doc_id_set)))

            # Get the docs by id set (source and target)
            doc_set_old.update(self.get_docs_by_id_set(engine_name=engine_name_old, index_name=index_name_old, doc_ids=unique_ids, drop_fields=['links'], flatten_output=True))
            doc_set_new.update(self.get_docs_by_id_set(engine_name=engine_name_new, index_name=index_name_new, doc_ids=unique_ids, drop_fields=['links'], flatten_output=True))

        # Get unique set of tuples
        unique_tuples = sorted(list(set(doc_set_old.keys()).union(set(doc_set_new.keys()))))

        # for k, t in enumerate(unique_tuples):
        #     print(k+1, t)
        # return

        # Update the sample size
        sample_size = len(unique_tuples)

        # Initialise stats dictionary
        stats = {
            'new_docs': 0,
            'deleted_docs': 0,
            'existing_docs': 0,
            'mismatch': 0,
            'custom_field_mismatch': 0,
            'match': 0,
            'set_to_null': 0,
            'percent_new_docs': 0,
            'percent_deleted_docs': 0,
            'percent_existing_docs': 0,
            'percent_mismatch': 0,
            'percent_custom_field_mismatch': 0,
            'percent_match': 0,
            'percent_set_to_null': 0,
            'mismatch_by_field': {}
        }

        # Initialise stacks
        mismatch_changes_stack = []

        # Initialise score and rank differences
        score_rank_diffs = {
            'degree_score': [],
            'degree_score_factor': []
        }

        #----------------------------#
        # Analyse comparison results #
        #----------------------------#

        # Initialise field missing or renamed list
        field_missing_or_renamed_list = []

        # Loop over the unique tuples
        for t in unique_tuples:

            # Check if the tuple is new
            if t in doc_set_old and t not in doc_set_new:
                stats['new_docs'] += 1

            # Check if the tuple is deleted
            elif t not in doc_set_old and t in doc_set_new:
                stats['deleted_docs'] += 1

            # Check if the tuple is in both source and target (existing doc)
            if t in doc_set_old and t in doc_set_new:

                # Add to existing docs
                stats['existing_docs'] += 1

                # Check if the values fully match
                if doc_set_old[t] == doc_set_new[t]:
                    stats['match'] += 1

                # Else, analyse the differences
                else:

                    # Initialise flags
                    exact_doc_mismatch_detected = False
                    custom_field_mismatch_detected = False
                    set_to_null_detected = False

                    # Loop over non-primary key fields
                    for k in doc_set_old[t]:

                        # Check if the key is in both source and target
                        if k not in doc_set_old[t] or k not in doc_set_new[t]:

                            # Add field existance mismatch to list
                            field_missing_or_renamed_list += [k]
                            field_missing_or_renamed_list = sorted(list(set(field_missing_or_renamed_list)))

                        # Else, analyse values in matching fields
                        else:

                            # Check if the values are different in matching fields
                            if doc_set_old[t][k] != doc_set_new[t][k]:

                                # Check if field exists in stats dictionary
                                if k not in stats['mismatch_by_field']:
                                    stats['mismatch_by_field'][k] = 0

                                # Flag mismatch detected
                                exact_doc_mismatch_detected = True

                                # Increment the mismatch counter
                                stats['mismatch_by_field'][k] += 1

                                # Check if custom field mismatch detected
                                if k not in ['doc_rank', 'doc_score', 'semantic_score', 'degree_score', 'degree_score_factor', 'object_created', 'object_updated']:

                                    # Flag custom field mismatch detected
                                    custom_field_mismatch_detected = True

                                    # Append the mismatch changes stack
                                    mismatch_changes_stack += [(f'{k}: [new] {str(doc_set_new[t][k])[:32]} --> [old] {str(doc_set_old[t][k])[:32]}')]

                                # Check if the value is set to NULL from source to target
                                if doc_set_old[t][k] is None:
                                    set_to_null_detected = True

                            # Append score and rank differences to list
                            if k in score_rank_diffs:
                                score_rank_diffs[k] += [doc_set_old[t][k] - doc_set_new[t][k]]

                    # Increment the mismatch counters based on flags
                    if exact_doc_mismatch_detected:
                        stats['mismatch'] += 1
                    if custom_field_mismatch_detected:
                        stats['custom_field_mismatch'] += 1
                    if set_to_null_detected:
                        stats['set_to_null'] += 1

        # rich.print_json(data=stats)
        # rich.print_json(data=mismatch_changes_stack)

        # return

        # Initialise test results
        test_results = {
            'flawless_match_test' : False,
            'deleted_docs_test' : True,
            'field_missing_or_renamed_test' : True,
            'custom_field_mismatch_test' : True,
            'set_to_null_test' : True,
            'median_score_diff_test' : True,
            'warning_flag' : False
        }

        # Calculate the percentages
        try:
            stats['percent_existing_docs'] = stats['existing_docs'] / sample_size * 100
            stats['percent_new_docs']      = stats['new_docs'     ] / sample_size * 100
            stats['percent_deleted_docs']  = stats['deleted_docs' ] / sample_size * 100

            if stats['existing_docs'] > 0:
                stats['percent_mismatch']      = stats['mismatch'     ] / stats['existing_docs'] * 100
                stats['percent_match']         = stats['match'        ] / stats['existing_docs'] * 100
                # stats['percent_set_to_null']   = stats['set_to_null'  ] / stats['existing_docs'] * 100
            else:
                stats['percent_mismatch']    = 0
                stats['percent_match']       = 0
                # stats['percent_set_to_null'] = 0

            if stats['mismatch'] > 0:
                stats['percent_custom_field_mismatch'] = stats['custom_field_mismatch'] / stats['mismatch'] * 100
                stats['percent_set_to_null'] = stats['set_to_null'] / stats['mismatch'] * 100
            else:
                stats['percent_custom_field_mismatch'] = 0
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
            print(f"🚀 \033[32mFlawless match test passed for {index_name_new}.\033[0m")
            return

        # Generate print colours
        if stats['percent_deleted_docs'] >= 25:
            percent_deleted_docs_colour = '\033[31m'
            test_results['deleted_docs_test'] = False
        elif stats['percent_deleted_docs'] >= 10:
            percent_deleted_docs_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_deleted_docs_colour = '\033[37m'

        if stats['percent_mismatch'] >= 10:
            percent_mismatch_colour = '\033[33m'
        elif stats['percent_mismatch'] >= 5:
            percent_mismatch_colour = '\033[33m'
        else:
            percent_mismatch_colour = '\033[37m'

        if stats['percent_custom_field_mismatch'] >= 10:
            percent_custom_field_mismatch_colour = '\033[31m'
            test_results['custom_field_mismatch_test'] = False
        elif stats['percent_custom_field_mismatch'] >= 5:
            percent_custom_field_mismatch_colour = '\033[33m'
            test_results['warning_flag'] = True
        else:
            percent_custom_field_mismatch_colour = '\033[37m'

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
        print(f"Results for \033[36m{engine_name_new}:{index_name_new}:\033[0m (new) vs \033[36m{engine_name_old}:{index_name_old}:\033[0m (old). doc_types: {doc_types}")
        print('')
        print(f" - Sample size ....... {sample_size}")
        print(f" - Existing docs ..... {stats['existing_docs']} {' '*(8-len(str(stats['existing_docs'])))} {stats['percent_existing_docs']:.1f}%")
        print(f" - New docs .......... {stats['new_docs']     } {' '*(8-len(str(stats['new_docs'])))     } {stats['percent_new_docs'     ]:.1f}%")
        print(f"{percent_deleted_docs_colour} - Deleted docs ...... {stats['deleted_docs'] } {' '*(8-len(str(stats['deleted_docs']))) } {stats['percent_deleted_docs' ]:.1f}% \033[0m")
        print('')
        print(f" - Match ............. {stats['match']        } {' '*(8-len(str(stats['match'])))        } {stats['percent_match'        ]:.1f}%")
        print(f"{percent_mismatch_colour} - Mismatch .......... {stats['mismatch']     } {' '*(8-len(str(stats['mismatch'])))     } {stats['percent_mismatch'     ]:.1f}% \033[0m")
        print(f"{percent_custom_field_mismatch_colour} - (custom fields) ... {stats['custom_field_mismatch']  } {' '*(8-len(str(stats['custom_field_mismatch']))  )} {stats['percent_custom_field_mismatch'  ]:.1f}% \033[0m")
        print(f"{percent_set_to_null_colour} - Set to NULL ....... {stats['set_to_null']  } {' '*(8-len(str(stats['set_to_null'])))  } {stats['percent_set_to_null'  ]:.1f}% \033[0m")
        print('')
        if len(stats['mismatch_by_field']) > 0:
            print('Mismatch(s) by field:')
            for field in stats['mismatch_by_field']:
                if stats['mismatch_by_field'][field] == 0:
                    print(f"\t- {field} {'.'*(64-len(field))} {stats['mismatch_by_field'][field]}")
                else:
                    if field in ['doc_rank', 'doc_score', 'semantic_score', 'degree_score', 'degree_score_factor', 'object_created', 'object_updated']:
                        print(f"\033[33m\t- {field} {'.'*(64-len(field))} {stats['mismatch_by_field'][field]}\033[0m")
                    else:
                        print(f"\033[31m\t- {field} {'.'*(64-len(field))} {stats['mismatch_by_field'][field]}\033[0m")
            print('')

        # Print score and rank average differences
        if len(score_rank_diffs['degree_score'])>0 or len(score_rank_diffs['degree_score_factor'])>0 or len(score_rank_diffs['doc_rank'])>0:
            print('Median score and rank differences:')
            for k in score_rank_diffs:
                if score_rank_diffs[k]:
                    # avg_val = sum(score_rank_diffs[k])/len(score_rank_diffs[k])
                    med_val = np.median(score_rank_diffs[k])
                    if   k in ['degree_score', 'degree_score_factor'] and abs(med_val)>=0.2:
                        test_results['median_score_diff_test'] = False
                        print(f"\033[31m\t- {k}: {med_val:.2f}\033[0m")
                    elif k in ['degree_score', 'degree_score_factor'] and abs(med_val)>=0.1:
                        test_results['warning_flag'] = True
                        print(f"\033[33m\t- {k}: {med_val:.2f}\033[0m")
                    else:
                        print(f"\t- {k}: {med_val:.2f}")
            print('')

        if len(field_missing_or_renamed_list) > 0:
            test_results['field_missing_or_renamed_test'] = False
            print(f"\033[31mfield mismatch(s) detected:\033[0m {field_missing_or_renamed_list}")
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
        if test_results['deleted_docs_test'] and test_results['field_missing_or_renamed_test'] and test_results['custom_field_mismatch_test'] and test_results['set_to_null_test'] and test_results['median_score_diff_test']:
            if test_results['warning_flag']:
                print("Test result: \033[33mMinor changes detected.\033[0m")
            else:
                print("Test result: \033[32mNo significant changes detected.\033[0m")
        else:
            print("Test result: \033[31mMajor changes detected!\033[0m")
        print('')

        time.sleep(1)

        return
