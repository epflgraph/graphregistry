#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, rich
from collections import defaultdict
from pathlib import Path
from yaml import safe_load

# Find the repository root directory
REPO_ROOT = Path(__file__).resolve().parents[2]

#================================#
# Class definition: GlobalConfig #
#================================#
class GlobalConfig:
    """Class to handle global configuration."""

    # Initialization method
    def __init__(self):

        # Load index configuration in to JSON
        with open(f"{REPO_ROOT}/config/config_global.yaml", "r", encoding="utf-8") as f:
            global_config = safe_load(f)

        # Initialise settings
        self.settings = json.loads(json.dumps(global_config, default=str))

        #-----------------------------------------#
        # Set MySQL schema names from config file #
        #-----------------------------------------#

        # Fetch schema names from config file
        self.mysql_schema_names = {
            'test' : {
                'ontology'    : self.settings['mysql']['db_schema_names']['ontology'],
                'registry'    : self.settings['mysql']['db_schema_names']['registry'],
                'lectures'    : self.settings['mysql']['db_schema_names']['lectures'],
                'airflow'     : self.settings['mysql']['db_schema_names']['airflow'],
                'es_cache'    : self.settings['mysql']['db_schema_names']['elasticsearch_cache'],
                'graph_cache' : self.settings['mysql']['db_schema_names']['graph_cache_test'],
                'graphsearch' : self.settings['mysql']['db_schema_names']['graphsearch_test']
            },
            'prod' : {
                'graph_cache' : self.settings['mysql']['db_schema_names']['graph_cache_prod'],
                'graphsearch' : self.settings['mysql']['db_schema_names']['graphsearch_prod']
            }
        }

        # Assign to local variables (to act as aliases)
        self.schema_ontology = self.mysql_schema_names['test']['ontology']
        self.schema_registry = self.mysql_schema_names['test']['registry']
        self.schema_lectures = self.mysql_schema_names['test']['lectures']
        self.schema_airflow  = self.mysql_schema_names['test']['airflow']
        self.schema_es_cache = self.mysql_schema_names['test']['es_cache']
        self.schema_graph_cache_test = self.mysql_schema_names['test']['graph_cache']
        self.schema_graph_cache_prod = self.mysql_schema_names['prod']['graph_cache']
        self.schema_graphsearch_test = self.mysql_schema_names['test']['graphsearch']
        self.schema_graphsearch_prod = self.mysql_schema_names['prod']['graphsearch']

        # Object type to schema mapping
        self.object_type_to_schema = {
            'Category'       : self.schema_ontology,
            'Concept'        : self.schema_ontology,
            'Course'         : self.schema_registry,
            'Lecture'        : self.schema_lectures,
            'MOOC'           : self.schema_registry,
            'Person'         : self.schema_registry,
            'Publication'    : self.schema_registry,
            'Slide'          : self.schema_lectures,
            'Specialisation' : self.schema_registry,
            'Startup'        : self.schema_registry,
            'Transcript'     : self.schema_lectures,
            'StudyPlan'      : self.schema_registry,
            'Unit'           : self.schema_registry,
            'Widget'         : self.schema_registry,
        }

        # Object type to institution id mapping
        self.object_type_to_institution_id = {
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

    # Print method
    def print(self):
        rich.print_json(data=self.settings)

#===============================#
# Class definition: IndexConfig #
#===============================#
class IndexConfig:
    """Class to handle index configuration."""

    # Initialization method
    def __init__(self):

        # Load index configuration in to JSON
        with open(f"{REPO_ROOT}/config/config_index.json", "r", encoding="utf-8") as f:
            index_config = json.load(f)

        # Initialise parsed options dictionary
        tree = lambda: defaultdict(tree); self.settings = tree()

        # Fetch list of supported doc types
        doc_types = index_config['doc-types']

        # Assign to parsed options dictionary
        self.settings['doc_types'] = doc_types

        #--------------------#
        # Fetch doc settings #
        #--------------------#

        # Loop over each doc type to fetch its specific options and field definitions
        for doc_type in doc_types:

            #---------------------------#
            # Fetch doc general options #
            #---------------------------#

            # Option: include_code_in_name
            include_code_in_name = 0
            if doc_type in index_config['options']['docs']:
                include_code_in_name = index_config['options']['docs'][doc_type]['include_code_in_name']

            # Assign to parsed options dictionary
            self.settings['options']['include_code_in_name'][doc_type] = include_code_in_name

            #----------------------------------------------#
            # Fetch doc definitions for GraphSearch tables #
            #----------------------------------------------#

            # Fetch list of fields to display on 'doc' specific tables
            # Start with lowest priority definitions, then override if others are available
            list_of_fields = []
            if doc_type       in index_config['fields']['docs']:
                list_of_fields = index_config['fields']['docs'][doc_type]

            # Assign list of fields to object's internal variable
            graphsearch_obj_fields = [
                f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language]
                for field_language, field_name in [tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields]
            ]

            # Assign to parsed options dictionary
            if len(graphsearch_obj_fields)>0:
                self.settings['graphsearch']['fields']['docs'][doc_type] = graphsearch_obj_fields

            #------------------------------------------------------#
            # Fetch doc definitions for ElasticSearch cache tables #
            #------------------------------------------------------#

            # Fetch list of fields to display on 'doc' specific tables
            # Start with lowest priority definitions, then override if others are available
            list_of_fields = []
            if doc_type       in index_config[   'fields']['docs']:
                list_of_fields = index_config[   'fields']['docs'][doc_type]
            if doc_type       in index_config['es-fields']['docs']:
                list_of_fields = index_config['es-fields']['docs'][doc_type]

            # Assign list of fields to object's internal variable
            elasticsearch_obj_fields = [
                f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language]
                for field_language, field_name in [tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields]
            ]

            # Assign to parsed options dictionary
            if len(elasticsearch_obj_fields)>0:
                self.settings['elasticsearch']['fields']['docs'][doc_type] = elasticsearch_obj_fields

            # Fetch ElasticSearch filters (if available)
            if doc_type              in index_config['es-filters']['docs']:
                elasticsearch_filters = index_config['es-filters']['docs'][doc_type]
            else:
                elasticsearch_filters = []

            # Assign to parsed options dictionary
            if len(elasticsearch_filters)>0:
                self.settings['elasticsearch']['filters']['docs'][doc_type] = elasticsearch_filters

        #-------------------------#
        # Fetch doc-link settings #
        #-------------------------#

        # Loop over each doc-link type to fetch its specific options and field definitions
        for doc_type in doc_types:
            for link_type in doc_types:

                #-----------------------------------------------------------------------------#
                # Fetch doclink definitions for GraphSearch tables (default / semantic links) #
                #-----------------------------------------------------------------------------#

                # Fetch list of fields to display on the 'link' side of doc-links
                # Start with lowest priority definitions, then override if others are available
                list_of_fields = []
                if link_type          in index_config['fields']['docs' ]:
                    list_of_fields     = index_config['fields']['docs' ][link_type]
                if link_type          in index_config['fields']['links']['default']:
                    if 'obj'          in index_config['fields']['links']['default'][link_type]:
                        list_of_fields = index_config['fields']['links']['default'][link_type]['obj']

                # Assign list of fields to object's internal variables
                graphsearch_obj_fields = [
                    f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language]
                    for field_language, field_name in [tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields]
                ]

                # Assign to parsed options dictionary
                if len(graphsearch_obj_fields)>0:
                    self.settings['graphsearch']['fields']['links']['default'][link_type] = graphsearch_obj_fields

                #--------------------------------------------------------------------------#
                # Fetch doclink ORDER BY rules for link ranking (default / semantic links) #
                #--------------------------------------------------------------------------#

                # Fetch ORDER BY rules to rank the links in doc-link tables
                graphsearch_obj_order_by = []
                if link_type                    in index_config['fields']['links']['default']:
                    if 'order'                  in index_config['fields']['links']['default'][link_type]:
                        graphsearch_obj_order_by = index_config['fields']['links']['default'][link_type]['order']

                # Assign to parsed options dictionary
                if len(graphsearch_obj_order_by)>0:
                    self.settings['graphsearch']['order_by']['links']['default'][link_type] = graphsearch_obj_order_by

                #-----------------------------------------------------------------------#
                # Fetch doclink definitions for GraphSearch tables (parent-child links) #
                #-----------------------------------------------------------------------#

                # Initialise parent-child specific internal variable
                graphsearch_obj2obj_fields = []

                # Fetch list of additional organisational-specific fields to display on the 'link' side of doc-links
                # There are additional fields, so there's no defaulting to other fields
                list_of_fields = []
                if doc_type               in index_config['fields']['links']['parent-child']:
                    if link_type          in index_config['fields']['links']['parent-child'][doc_type]:
                        if 'obj2obj'      in index_config['fields']['links']['parent-child'][doc_type][link_type]:
                            list_of_fields = index_config['fields']['links']['parent-child'][doc_type][link_type]['obj2obj']

                # Assign list of fields to object's internal variables
                graphsearch_obj2obj_fields = [
                    f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language]
                    for field_language, field_name in [tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields]
                ]

                # Assign to parsed options dictionary
                if len(graphsearch_obj2obj_fields)>0:
                    self.settings['graphsearch']['fields']['links']['parent_child'][doc_type][link_type] = graphsearch_obj2obj_fields

                #--------------------------------------------------------------------#
                # Fetch doclink ORDER BY rules for link ranking (parent-child links) #
                #--------------------------------------------------------------------#

                # Fetch ORDER BY rules to rank the links in doc-link tables
                graphsearch_obj2obj_order_by = []
                if doc_type                             in index_config['fields']['links']['parent-child']:
                    if link_type                        in index_config['fields']['links']['parent-child'][doc_type]:
                        if 'obj2obj'                    in index_config['fields']['links']['parent-child'][doc_type][link_type]:
                            graphsearch_obj2obj_order_by = index_config['fields']['links']['parent-child'][doc_type][link_type]['order']

                # Assign to parsed options dictionary
                if len(graphsearch_obj2obj_order_by)>0:
                    self.settings['graphsearch']['order_by']['links']['parent_child'][doc_type][link_type] = graphsearch_obj2obj_order_by

                #----------------------------------------------------------#
                # Fetch doclink definitions for ElasticSearch cache tables #
                #----------------------------------------------------------#

                # Fetch list of fields to display on the 'link' side of doc-links
                # Start with lowest priority definitions, then override if others are available
                list_of_fields = []
                if link_type          in index_config[   'fields']['docs' ]:
                    list_of_fields     = index_config[   'fields']['docs' ][link_type]
                if link_type          in index_config[   'fields']['links']['default']:
                    if 'obj'          in index_config[   'fields']['links']['default'][link_type]:
                        list_of_fields = index_config[   'fields']['links']['default'][link_type]['obj']
                if link_type          in index_config['es-fields']['docs' ]:
                    list_of_fields     = index_config['es-fields']['docs' ][link_type]
                if link_type          in index_config['es-fields']['links']:
                    list_of_fields     = index_config['es-fields']['links'][link_type]

                # Assign list of fields to object's internal variables
                elasticsearch_obj_fields = [
                    f"{field_name}"+{'n/a':'', 'en':'_en', 'fr':'_fr'}[field_language]
                    for field_language, field_name in [tuple(v) if type(v) is list else ('n/a', v) for v in list_of_fields]
                ]

                # Assign to parsed options dictionary
                if len(elasticsearch_obj_fields)>0:
                    self.settings['elasticsearch']['fields']['links'][link_type] = elasticsearch_obj_fields

                # Fetch ElasticSearch filters (if available)
                if link_type  in index_config['es-filters']['links']:
                    elasticsearch_filters = index_config['es-filters']['links'][link_type]
                else:
                    elasticsearch_filters = []

                # Assign to parsed options dictionary
                if len(elasticsearch_filters)>0:
                    self.settings['elasticsearch']['filters']['links'][link_type] = elasticsearch_filters

        # Convert defaultdict to normal dict for easier handling
        self.settings = json.loads(json.dumps(self.settings))

    # Print method
    def print(self, compact=False):

        # Emoji dictionary
        type_emojis = {
            "Category":    "🌲",
            "Concept":     "💡",
            "Course":      "🎓",
            "Lecture":     "🎥",
            "MOOC":        "🌐",
            "Person":      "🪪 ",
            "Publication": "📚",
            "Unit":        "🏛️ ",
            "Startup":     "🚀",
            "Widget":      "🧩",
        }

        #----------------------------------------------#
        # Build printable structure (for compact=True) #
        #----------------------------------------------#

        # Initialise printable struct
        printable_struct = {}

        # Loop over each doc type to build printable sets
        for doc_type in self.settings['doc_types']:

            # Rename variable for better comprehension
            doc_type_as_link = doc_type

            # Loop over each doc-link type to print its specific options and field definitions
            for other_doc_type in self.settings['doc_types']:

                # Build index m-tuple
                idx_tuple = [
                    self.settings['graphsearch'  ]['fields'  ]['links']['default'     ].get(doc_type_as_link, []),
                    self.settings['graphsearch'  ]['order_by']['links']['default'     ].get(doc_type_as_link, []),
                    self.settings['graphsearch'  ]['fields'  ]['links']['parent_child'].get(other_doc_type, {}).get(doc_type_as_link, []),
                    self.settings['graphsearch'  ]['order_by']['links']['parent_child'].get(other_doc_type, {}).get(doc_type_as_link, []),
                    self.settings['elasticsearch']['fields'  ]['links'].get(doc_type_as_link, []),
                    self.settings['elasticsearch']['filters' ]['links'].get(doc_type_as_link, [])
                ]

                # Append to printable struct
                if str(idx_tuple) not in printable_struct:
                    printable_struct[str(idx_tuple)] = {
                        'doc_type(s)' : [other_doc_type],
                        'link_type'   : doc_type_as_link,
                        'idx_tuple'   : idx_tuple
                    }
                else:
                    printable_struct[str(idx_tuple)]['doc_type(s)'] += [other_doc_type]

        #----------------------------------------------#

        # Loop over each doc type to print its specific options and field definitions
        for doc_type in self.settings['doc_types']:

            # Print header
            print('\n')
            print((6+len(doc_type))*'-')
            if doc_type in type_emojis:
                print(f"{type_emojis[doc_type]} {doc_type} {type_emojis[doc_type]}")
            else:
                print(f"📄 {doc_type} 📄")
            print((6+len(doc_type))*'-')

            # Print out initialisation info
            print(f"\n📄 Document type settings:\n")
            print(f" ☑️  Option: include_code_in_name ...... {self.settings['options' ]['include_code_in_name'].get(doc_type, 0)}")
            print(f" 🐬 GraphSearch > Fields .............. {self.settings['graphsearch'  ]['fields' ]['docs'].get(doc_type, [])}")
            print(f" ⚡️ ElasticSearch > Fields ............ {self.settings['elasticsearch']['fields' ]['docs'].get(doc_type, [])}")
            print(f" ⚡️ ElasticSearch > Filters ........... {self.settings['elasticsearch']['filters']['docs'].get(doc_type, [])}")

            # Rename variable for better comprehension
            doc_type_as_link = doc_type

            #-----------------------#
            # Print in compact form #
            #-----------------------#
            if compact:

                # Loop over all printable tuples
                for str_idx_tuple in printable_struct:

                    # Look for current link in structure
                    if printable_struct[str_idx_tuple]['link_type'] != doc_type_as_link:
                        continue

                    # Get doc types to display
                    dt = printable_struct[str_idx_tuple]['doc_type(s)']

                    # Print out initialisation info
                    print(f"\n🔗 Doc-Link type(s): {'{' if len(dt)>1 else ''}{','.join(dt)}{'}' if len(dt)>1 else ''} --> {printable_struct[str_idx_tuple]['link_type']}\n")
                    print(f" 🐬 GraphSearch > Default > Fields .......... {printable_struct[str_idx_tuple]['idx_tuple'][0]}")
                    print(f" 🐬 GraphSearch > Default > Order ........... {printable_struct[str_idx_tuple]['idx_tuple'][1]}")
                    print(f" 🐬 GraphSearch > Parent-Child > Fields ..... {printable_struct[str_idx_tuple]['idx_tuple'][2]}")
                    print(f" 🐬 GraphSearch > Parent-Child > Order ...... {printable_struct[str_idx_tuple]['idx_tuple'][3]}")
                    print(f" ⚡️ ElasticSearch > Fields .................. {printable_struct[str_idx_tuple]['idx_tuple'][4]}")
                    print(f" ⚡️ ElasticSearch > Filters ................. {printable_struct[str_idx_tuple]['idx_tuple'][5]}")

            #---------------------------------------#
            # Print all doc-link options explicitly #
            #---------------------------------------#
            else:

                # Loop over each doc-link type to print its specific options and field definitions
                for other_doc_type in self.settings['doc_types']:

                    # Print out initialisation info
                    print(f"\n🔗 Doc-Link type: {other_doc_type} --> {doc_type_as_link}\n")
                    print(f" 🐬 GraphSearch > Default > Fields .......... {self.settings['graphsearch'  ]['fields'  ]['links']['default'     ].get(doc_type_as_link, [])}")
                    print(f" 🐬 GraphSearch > Default > Order ........... {self.settings['graphsearch'  ]['order_by']['links']['default'     ].get(doc_type_as_link, [])}")
                    print(f" 🐬 GraphSearch > Parent-Child > Fields ..... {self.settings['graphsearch'  ]['fields'  ]['links']['parent_child'].get(other_doc_type, {}).get(doc_type_as_link, [])}")
                    print(f" 🐬 GraphSearch > Parent-Child > Order ...... {self.settings['graphsearch'  ]['order_by']['links']['parent_child'].get(other_doc_type, {}).get(doc_type_as_link, [])}")
                    print(f" ⚡️ ElasticSearch > Fields .................. {self.settings['elasticsearch']['fields'  ]['links'].get(doc_type_as_link, [])}")
                    print(f" ⚡️ ElasticSearch > Filters ................. {self.settings['elasticsearch']['filters' ]['links'].get(doc_type_as_link, [])}")

        # End of print method
        print('')

#===============================#
# Class definition: IndexConfig #
#===============================#
class ScoresConfig:
    """Class to handle index configuration."""

    # Initialization method
    def __init__(self):

        # Load index configuration in to JSON
        with open(f"{REPO_ROOT}/config/config_scores.json", "r", encoding="utf-8") as f:
            scores_config = json.load(f)

        # Initialise settings
        self.settings = {}

        # Fetch score edge tuples
        self.settings['scored_edge_tuples'] = scores_config['scored-edge-tuples']

    # Print method
    def print(self):

        # Print head
        print('')
        print("================================")
        print("🧮 List of edges to be scored 🧮")
        print("================================")

        # Print all education tuples
        print('\n🔗 Education related edges:')
        for t in self.settings['scored_edge_tuples']['education']:
            print(f" - {t[0]} --> {t[1]}")

        # Print all research tuples
        print('\n🔗 Research related edges:')
        for t in self.settings['scored_edge_tuples']['research']:
            print(f" - {t[0]} --> {t[1]}")

        # Print footer
        print('')

#================#
# Main execution #
#================#
if __name__ == "__main__":
    # self = GlobalConfig()
    # self.print()
    idxcfg = IndexConfig()
    idxcfg.print(compact=True)
    scrcfg = ScoresConfig()
    scrcfg.print()