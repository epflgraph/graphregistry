# graphregistry/adapters/clients/elasticsearch.py
import warnings
from elastic_transport import SecurityWarning
from urllib3.exceptions import InsecureRequestWarning
from typing import Any, Dict
from pyparsing import Word, alphas

# Elasticsearch client warning (verify_certs=False)
warnings.filterwarnings(
    "ignore",
    category=SecurityWarning,
    message=r".*verify_certs=False is insecure.*",
)

# urllib3 HTTPS warning
warnings.filterwarnings(
    "ignore",
    category=InsecureRequestWarning,
    message=r".*Adding certificate verification is strongly advised.*",
)

import logging
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

from graphregistry.common.config import GlobalConfig
from elasticsearch import Elasticsearch as ElasticSearchEngine, helpers
from elasticsearch import exceptions as es_exceptions
from loguru import logger as sysmsg
from urllib.parse import quote
from flatten_dict import flatten
from datetime import datetime
from tqdm import tqdm
import numpy as np
import os, shutil, sys, time, rich, gzip, json, subprocess, warnings, logging, random, tempfile

# Auxiliary function: Count lines in JSONL file
def count_jsonl_lines(path: str) -> int:
    opener = gzip.open if path.endswith(".gz") else open
    mode = "rt" if path.endswith(".gz") else "r"
    with opener(path, mode, encoding="utf-8") as f:
        return sum(1 for _ in f)

# Auxiliary function: Wait until count is visible
def wait_until_count_visible(es, index: str, expected: int, timeout_s: int = 30, poll_s: float = 0.5) -> int:
    """
    Poll _count until it reaches expected (or timeout). Returns the last observed count.
    Uses only count API (no refresh privilege needed).
    """
    deadline = time.time() + timeout_s
    last = -1
    while time.time() < deadline:
        try:
            last = es.count(index=index).get("count", last)
        except Exception:
            pass
        if last >= expected:
            return last
        time.sleep(poll_s)
    return last

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
    'Category'   : 512,
    'Concept'    : 512,
    'Course'     : 128,
    'Exercise'   : 64,
    'Lecture'    : 128,
    'MOOC'       : 64,
    'Notebook'   : 64,
    'Person'     : 128,
    'Publication': 1,
    'Startup'    : 64,
    'Unit'       : 64,
    'Widget'     : 64
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')
logger.setLevel(logging.DEBUG)

#-------------------------------------------------#
# Class definition for Graph ElasticSearch engine #
#-------------------------------------------------#
class GraphES:

    # Class constructor
    def __init__(self, name="GraphIndex", use_ssl=False, config: GlobalConfig | None = None):

        # Configuration is loaded on first use, not at module import time.
        self._config = config or GlobalConfig()
        self.name = name

        # Initiate the ElasticSearch engines
        self.params_test        , self.engine_test         = self.initiate_engine('test')
        self.params_prod        , self.engine_prod         = self.initiate_engine('prod')
        self.params_xaas_prod   , self.engine_xaas_prod    = self.initiate_engine('xaas_prod')
        self.params_xaas_coresrv, self.engine_xaas_coresrv = self.initiate_engine('xaas_coresrv')
        self.params = {
            'test'         : self.params_test,
            'prod'         : self.params_prod,
            'xaas_prod'    : self.params_xaas_prod,
            'xaas_coresrv' : self.params_xaas_coresrv
        }
        self.engine = {
            'test'         : self.engine_test,
            'prod'         : self.engine_prod,
            'xaas_prod'    : self.engine_xaas_prod,
            'xaas_coresrv' : self.engine_xaas_coresrv
        }

    #---------------------------------------------#
    # Method: Initialize the ElasticSearch engine #
    #---------------------------------------------#
    def initiate_engine(self, engine_name, use_ssl=False):
        """
        Initialize the ElasticSearch engine (no SSL) based on the server name provided.
        """

        # Check if the server name is in the global configuration
        if f'{engine_name}_env' not in self._config.settings['elasticsearch']:
            raise ValueError(
                f"Could not find configuration for Elasticsearch server '{engine_name}' in global config."
            )

        # Load parameters
        params = self._config.settings['elasticsearch'][f'{engine_name}_env']

        # Build connection URL (HTTP only, no SSL)
        if "password" in params and params["password"]:
            es_hosts = f"https://{params['username']}:{quote(params['password'])}@{params['hostname']}:{params['port']}"
        elif "username" in params and params["username"]:
            es_hosts = f"https://{params['username']}@{params['hostname']}:{params['port']}"
        else:
            es_hosts = f"https://{params['hostname']}:{params['port']}"

        # Initialize Elasticsearch engine
        engine = ElasticSearchEngine(
            hosts           = [es_hosts],
            http_compress   = True,
            verify_certs    = False, # use_ssl,
            ca_certs        = '', # self._config.settings['elasticsearch']['graph_engine_test']['cert_file'] if use_ssl else '',
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

    #--------------------------------------#
    # Method: Export an index to a folder  #
    #--------------------------------------#
    def export_index_to_folder(self,
        engine_name:str, index_name:str, output_folder:str, *,
        replace_existing:bool=False, force:bool=False, use_gzip:bool=True,
        chunk_size:int=2000, request_timeout:int=120):
        """
        Export an ElasticSearch index to a folder:
        - settings + mappings -> settings_mappings.json
        - documents (JSONL)   -> documents.jsonl(.gz)

        Flags:
        - replace_existing: if True, will replace existing output_folder
        - force: if replace_existing=True and force=False, prompts user confirmation
        """

        # Get ElasticSearch connector object for selected engine
        es = self.engine[engine_name]

        # ---------------------------
        # 0) Basic checks
        # ---------------------------
        if not es.indices.exists(index=index_name):
            raise ValueError(f"Index '{index_name}' does not exist on engine '{engine_name}'.")

        # ---------------------------
        # 1) Handle output directory
        # ---------------------------

        # Append index name to output folder
        output_folder = os.path.join(output_folder, index_name)

        if os.path.exists(output_folder):
            if not replace_existing:
                raise FileExistsError(
                    f"Output folder already exists: {output_folder}\n"
                    f"Use replace_existing=True to overwrite."
                )

            if not force:
                confirmation = input(
                    f"Folder '{output_folder}' already exists. Replace it? (yes/no): "
                ).strip().lower()
                if confirmation != "yes":
                    print("Export cancelled.")
                    return

            shutil.rmtree(output_folder)

        os.makedirs(output_folder, exist_ok=True)

        # ---------------------------
        # 2) Export settings + mappings
        # ---------------------------
        index_info = es.indices.get(index=index_name)

        # NOTE: ES returns a bunch of settings. Usually you want the ones you can reapply.
        settings = index_info[index_name].get("settings", {}).get("index", {})
        mappings = index_info[index_name].get("mappings", {})

        # Drop noisy / non-portable settings unless explicitly requested
        drop_keys = {
            "uuid",
            "version",
            "provided_name",
            "creation_date",
            "creation_date_string",
            "routing",
            "store",
            "frozen",
            "history_uuid",
            "lifecycle",          # ILM policies might not exist elsewhere
            "shard",
            "resize",
        }
        settings = {k: v for k, v in settings.items() if k not in drop_keys}

        settings_and_mappings = {
            "index": index_name,
            "exported_at": datetime.utcnow().isoformat() + "Z",
            "settings": {"index": settings},
            "mappings": mappings,
        }

        settings_path = os.path.join(output_folder, "settings_mappings.json")
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(settings_and_mappings, f, ensure_ascii=False, indent=2)

        # ---------------------------
        # 3) Export documents as JSONL
        # ---------------------------
        docs_filename = "documents.jsonl.gz" if use_gzip else "documents.jsonl"
        docs_path = os.path.join(output_folder, docs_filename)

        query = {"query": {"match_all": {}}}

        # Stream docs via scan (scroll)
        scan_iter = helpers.scan(
            client=es,
            index=index_name,
            query=query,
            size=chunk_size,
            preserve_order=False,
            request_timeout=request_timeout,
        )

        # Write JSON Lines:
        # each line is one document with _id and _source (and optionally anything else you want)
        opener = gzip.open if use_gzip else open
        mode = "wt"

        total = es.count(index=index_name)["count"]

        count = 0
        with opener(docs_path, mode, encoding="utf-8") as f, tqdm(
            total=total,
            unit="docs",
            desc=f"Exporting ← {index_name}",
            dynamic_ncols=True,
        ) as pbar:

            for hit in scan_iter:
                doc = {
                    "_id": hit.get("_id"),
                    "_source": hit.get("_source", {}),
                }
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")

                count += 1
                pbar.update(1)

        print(f"✅ Export complete: {count:,} docs")
        print(f"   - {settings_path}")
        print(f"   - {docs_path}")

    #---------------------------------------#
    # Method: Import an index from a folder #
    #---------------------------------------#
    def import_index_from_folder(self, engine_name:str, input_folder:str, *, rename_to:str|None=None, replace_existing:bool=False, force:bool=False, chunk_size:int=2000, request_timeout:int=3600, refresh:bool=False):

        # Get ElasticSearch connector object for selected engine
        es = self.engine[engine_name]

        # ---------------------------
        # 0) Resolve target index name
        # ---------------------------

        # Extract index name from input folder if not renaming
        if rename_to is None:
            target_index = os.path.basename(os.path.normpath(input_folder))
        else:
            target_index = rename_to

        # ---------------------------
        # 1) Validate input folder + files
        # ---------------------------
        if not os.path.isdir(input_folder):
            raise FileNotFoundError(f"Input folder does not exist: {input_folder}")

        settings_path = os.path.join(input_folder, "settings_mappings.json")
        if not os.path.isfile(settings_path):
            raise FileNotFoundError(f"Missing settings file: {settings_path}")

        docs_path_gz = os.path.join(input_folder, "documents.jsonl.gz")
        docs_path = os.path.join(input_folder, "documents.jsonl")

        if os.path.isfile(docs_path_gz):
            docs_path_final = docs_path_gz
            use_gzip = True
        elif os.path.isfile(docs_path):
            docs_path_final = docs_path
            use_gzip = False
        else:
            raise FileNotFoundError(
                "Missing documents file (documents.jsonl or documents.jsonl.gz)"
            )

        # ---------------------------
        # 2) Handle existing target index
        # ---------------------------
        if es.indices.exists(index=target_index):
            if not replace_existing:
                raise FileExistsError(
                    f"Index '{target_index}' already exists on engine '{engine_name}'. "
                    f"Use replace_existing=True to overwrite."
                )

            if not force:
                confirmation = input(
                    f"Index '{target_index}' already exists on '{engine_name}'. Replace it? (yes/no): "
                ).strip().lower()
                if confirmation != "yes":
                    print("Import cancelled.")
                    return

            es.indices.delete(index=target_index)
            print(f"🗑️  Deleted existing index: {target_index}")

        # ---------------------------
        # 3) Read settings + mappings
        # ---------------------------
        with open(settings_path, "r", encoding="utf-8") as f:
            settings_and_mappings = json.load(f)

        body = {
            "settings": settings_and_mappings.get("settings", {}),
            "mappings": settings_and_mappings.get("mappings", {}),
        }

        print(f"📦 Creating index '{target_index}'...")
        es.indices.create(index=target_index, body=body)
        es.indices.put_settings(
            index=target_index,
            body={"index": {"blocks": {"write": False, "read_only_allow_delete": False}}}
        )
        print(f"✅ Index created: {target_index}")

        # ---------------------------
        # 4) Bulk import documents
        # ---------------------------
        opener = gzip.open if use_gzip else open
        mode = "rt" if use_gzip else "r"

        def gen_actions():
            with opener(docs_path_final, mode, encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        raise ValueError(
                            f"Invalid JSON on line {line_no} in {docs_path_final}: {e}"
                        )

                    action = {
                        "_op_type": "index",
                        "_index": target_index,
                        "_source": obj.get("_source", {}),
                    }

                    if "_id" in obj:
                        action["_id"] = obj["_id"]

                    yield action

        # Optional: show percentage by counting docs first
        total = count_jsonl_lines(docs_path_final)

        success = 0
        errors = []

        with tqdm(
            total=total,
            unit="docs",
            desc=f"Importing → {target_index}",
            dynamic_ncols=True,
        ) as pbar:

            for ok, info in helpers.streaming_bulk(
                client=es,
                actions=gen_actions(),
                chunk_size=chunk_size,
                request_timeout=request_timeout,
                raise_on_error=False,
                raise_on_exception=False,
                refresh=refresh,
            ):
                if ok:
                    success += 1
                else:
                    errors.append(info)

                pbar.update(1)

        if errors:
            error_count = len(errors) if isinstance(errors, list) else (errors if isinstance(errors, int) else 0)
            print(
                f"⚠️  Import completed with errors. "
                f"Indexed: {success:,}, Errors: {error_count:,}"
            )
            if isinstance(errors, list):
                for e in errors[:5]:
                    print("  Error:", e)
        else:
            print(f"✅ Import complete. Indexed: {success:,} documents.")

        try:
            # cnt = es.count(index=target_index)["count"]
            visible = wait_until_count_visible(es, target_index, expected=success, timeout_s=30)
            print(f"📊 Index '{target_index}' doc count (visible): {visible:,}")
        except Exception:
            sysmsg.warning("Failed to retrieve document count.")
            pass

    #----------------------------------------------#
    # Method: Copy an index across two ES servers  #
    #----------------------------------------------#
    def copy_index_across_engines(self,
        source_engine:str, target_engine:str, index_name:str, *,
        rename_to:str|None=None, replace_existing:bool=False, force:bool=False, use_gzip:bool=True, chunk_size:int=2000, request_timeout:int=120, refresh:bool=False, temp_folder:str|None='/tmp', keep_temp:bool=False):
        """
        Copy an index from one ElasticSearch engine to another by:
        1) exporting index_name from source_engine into a temp folder
        2) importing that folder into target_engine

        Parameters
        ----------
        source_engine : str
            Engine key for the source ES connection (self.engine[source_engine])
        target_engine : str
            Engine key for the target ES connection (self.engine[target_engine])
        index_name : str
            Source index name to export.
        rename_to : str | None
            If provided, the target index name on the destination.
            If None, uses the folder basename logic in import_index_from_folder()
            (which will be index_name).

        Flags (mirrors export/import)
        ----------------------------
        replace_existing : bool
            If True, replaces existing export folder AND destination index (if exists).
        force : bool
            If replace_existing=True and force=False, prompt before replacing.
        use_gzip : bool
            Export documents.jsonl.gz instead of documents.jsonl.
        chunk_size : int
            Scan/bulk chunk size (used in export scan + import bulk).
        request_timeout : int
            Timeout passed to ES operations.
        refresh : bool
            Passed to streaming_bulk refresh (import).
        temp_folder : str | None
            If provided, use this directory as the temp base folder.
            Otherwise uses system temp.
        keep_temp : bool
            If True, do not delete the temp folder after completion.

        Returns
        -------
        dict with paths and target index name
        """

        # Create a unique temp base directory
        if temp_folder is None:
            base_tmp = tempfile.mkdtemp(prefix="graphregistry_es_copy_")
        else:
            # Create temp folder if needed
            base_tmp = os.path.abspath(temp_folder)
            os.makedirs(base_tmp, exist_ok=True)
            # Use a unique subfolder to avoid collisions
            stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            base_tmp = os.path.join(base_tmp, f"graphregistry_es_copy_{stamp}_{index_name}")
            os.makedirs(base_tmp, exist_ok=True)

        # IMPORTANT:
        # Your export_index_to_folder() appends index_name to output_folder itself.
        # So if we pass base_tmp, it will export into: base_tmp/index_name
        exported_folder = os.path.join(base_tmp, index_name)

        try:
            # 1) Export from source
            self.export_index_to_folder(
                engine_name=source_engine,
                index_name=index_name,
                output_folder=base_tmp,
                replace_existing=True,   # safe: base_tmp is unique; also avoids prompts
                force=True,
                use_gzip=use_gzip,
                chunk_size=chunk_size,
                request_timeout=request_timeout,
            )

            # 2) Import into target
            self.import_index_from_folder(
                engine_name=target_engine,
                input_folder=exported_folder,
                rename_to=rename_to,
                replace_existing=replace_existing,
                force=force,
                chunk_size=chunk_size,
                request_timeout=request_timeout,
                refresh=refresh,
            )

            target_index = rename_to or os.path.basename(os.path.normpath(exported_folder))

            return {
                "source_engine": source_engine,
                "target_engine": target_engine,
                "source_index": index_name,
                "target_index": target_index,
                "temp_base": base_tmp,
                "exported_folder": exported_folder,
                "kept_temp": keep_temp,
            }

        finally:
            if not keep_temp:
                # Clean up temp folder (best effort)
                try:
                    pass
                    # shutil.rmtree(base_tmp)
                except Exception:
                    pass

    #---------------------------------------------------------#
    # Method: Copy one or more aliases across two ES servers  #
    #---------------------------------------------------------#
    def copy_aliases_across_engines(
        self,
        source_engine: str,
        target_engine: str,
        *,
        index_pattern: str = "*",
        alias_pattern: str = "*",
        index_map: dict[str, str] | None = None,
        replace_existing: bool = False,
        force: bool = False,
        ignore_missing_target_index: bool = False,
        include_write_index: bool = False,
    ):
        """
        Copy index aliases from source_engine to target_engine.

        Typical use
        -----------
        - After copying indices across engines, run this to recreate aliases on the target.

        Parameters
        ----------
        source_engine, target_engine:
            Keys for self.engine[...] connections.

        index_pattern:
            Which source indices to consider (default "*").

        alias_pattern:
            Which aliases to copy (default "*").

        index_map:
            Optional mapping {source_index: target_index}. Useful when you renamed indices
            during migration/copy. If None, uses the same index name on target.

            Example:
                {"myindex_v1": "myindex_v2"}

        replace_existing:
            If True, remove aliases on target before re-adding them (for the affected alias/index pairs).

        force:
            If replace_existing=True and force=False, prompt before making changes.

        ignore_missing_target_index:
            If True, skip alias actions when the target index doesn't exist.
            If False, raise.

        include_write_index:
            If True, preserve is_write_index when present.
            If False, do not set it (ES may infer / or keep existing behavior).
        """

        src = self.engine[source_engine]
        dst = self.engine[target_engine]

        index_map = index_map or {}

        # Get aliases from source
        # Returns: { "indexA": {"aliases": {"alias1": {...}, "alias2": {...}}}, ... }
        src_aliases = src.indices.get_alias(index=index_pattern, name=alias_pattern)

        # Build target actions
        actions = []
        skipped = []  # (source_index, target_index, alias, reason)

        for src_index, payload in src_aliases.items():
            aliases = (payload or {}).get("aliases", {}) or {}
            if not aliases:
                continue

            tgt_index = index_map.get(src_index, src_index)

            # Validate target index exists
            if not dst.indices.exists(index=tgt_index):
                msg = "target index missing"
                if ignore_missing_target_index:
                    for a in aliases.keys():
                        skipped.append((src_index, tgt_index, a, msg))
                    continue
                raise ValueError(
                    f"Target index '{tgt_index}' does not exist on '{target_engine}' "
                    f"(source '{src_index}' on '{source_engine}')."
                )

            for alias_name, alias_body in aliases.items():
                # alias_body can contain: filter, routing, index_routing, search_routing, is_write_index, etc.
                add_action = {
                    "add": {
                        "index": tgt_index,
                        "alias": alias_name,
                    }
                }

                if isinstance(alias_body, dict):
                    # Preserve supported alias options
                    for k in ("filter", "routing", "index_routing", "search_routing"):
                        if k in alias_body:
                            add_action["add"][k] = alias_body[k]
                    if include_write_index and "is_write_index" in alias_body:
                        add_action["add"]["is_write_index"] = alias_body["is_write_index"]

                if replace_existing:
                    actions.append({"remove": {"index": tgt_index, "alias": alias_name}})
                actions.append(add_action)

        if not actions:
            return {
                "source_engine": source_engine,
                "target_engine": target_engine,
                "index_pattern": index_pattern,
                "alias_pattern": alias_pattern,
                "actions_applied": 0,
                "skipped": skipped,
            }

        if replace_existing and not force:
            confirmation = input(
                f"This will update aliases on '{target_engine}' (replace_existing=True). Continue? (yes/no): "
            ).strip().lower()
            if confirmation != "yes":
                print("Alias copy cancelled.")
                return {
                    "source_engine": source_engine,
                    "target_engine": target_engine,
                    "index_pattern": index_pattern,
                    "alias_pattern": alias_pattern,
                    "actions_applied": 0,
                    "skipped": skipped,
                    "cancelled": True,
                }

        # Apply in one call (atomic-ish)
        resp = dst.indices.update_aliases(body={"actions": actions})

        return {
            "source_engine": source_engine,
            "target_engine": target_engine,
            "index_pattern": index_pattern,
            "alias_pattern": alias_pattern,
            "actions_applied": len(actions),
            "skipped": skipped,
            "acknowledged": bool(resp.get("acknowledged", False)) if isinstance(resp, dict) else None,
        }


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
    def copy_index_across_engines_LEGACY(self, source_engine_name, target_engine_name, index_name, rename_to=None, chunk_size=1000):

        # Define the index names
        index_name_source = index_name
        index_name_target = index_name
        if rename_to is not None:
            index_name_target = rename_to
            sysmsg.info(f"Renaming index from '{index_name_source}' to '{index_name_target}' on target server.")

        # Define the parameters for the ElasticDump command
        params_server_source = f"https://{self.params[source_engine_name]['username']}:{quote(self.params[source_engine_name]['password'])}@{self.params[source_engine_name]['hostname']}:{self.params[source_engine_name]['port']}/{index_name_source}"
        params_server_target = f"https://{self.params[target_engine_name]['username']}:{quote(self.params[target_engine_name]['password'])}@{self.params[target_engine_name]['hostname']}:{self.params[target_engine_name]['port']}/{index_name_target}"
        # base_command = ["npx", "elasticdump", f"--input={params_server_source}", f"--output={params_server_target}", f"--input-ca={self.params[source_engine_name]['cert_file']}", f"--output-ca={self.params[target_engine_name]['cert_file']}", f"--limit={chunk_size}"]

        # Create the target index if it does not exist
        try:
            self.engine[target_engine_name].indices.create(index=index_name_target)
            sysmsg.success(f"✅ Created index '{index_name_target}' on '{target_engine_name}'.")
        except es_exceptions.BadRequestError as e:
            if "resource_already_exists_exception" in str(e):
                sysmsg.warning(f"Index '{index_name_target}' already exists on '{target_engine_name}'. Proceeding with data copy...")
                pass
            else:
                sysmsg.error(f"❌ Failed to create index '{index_name_target}' on '{target_engine_name}'. ")
                print(e)
                raise

        # Disable index blocks on target index (to prevent read-only errors)
        self.engine[target_engine_name].indices.put_settings(
            index = index_name_target,
            body  = {
                "index.blocks.read_only": False,
                "index.blocks.read_only_allow_delete": False,
                "index.blocks.write": False
            }
        )

        # Generate base command for data transfer
        base_command = ["npx", "elasticdump", f"--input={params_server_source}", f"--output={params_server_target}", f"--limit={chunk_size}"]

        # Copy the index from test to prod
        for type in ['settings', 'mapping', 'data']:
            print(f"\n⚙️  Copying {type} from '{source_engine_name}:{index_name_source}' to '{target_engine_name}:{index_name_target}' ...")
            subprocess.run(base_command + [f"--type={type}"], env={**os.environ, "NODE_TLS_REJECT_UNAUTHORIZED": "0"})

        # Print completion message
        sysmsg.success(f"\n✅ Index was successfully copied.")

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

#================#
# Main execution #
#================#
if __name__ == "__main__":
    es = GraphES()
    if es.test(engine_name='test') is True:
        sysmsg.success("✅ ElasticSearch client test passed.")
    else:
        sysmsg.error("❌ ElasticSearch client test failed.")