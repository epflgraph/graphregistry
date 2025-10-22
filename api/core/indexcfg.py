#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, rich
from collections import defaultdict
from pathlib import Path

# Find the repository root directory
REPO_ROOT = Path(__file__).resolve().parents[2]

#===============================#
# Class definition: IndexConfig #
#===============================#
class IndexConfig:
    """Class to handle index configuration."""

    # Initialization method
    def __init__(self):

        # Load index configuration in to JSON
        with open(f"{REPO_ROOT}/config/index_cfg.json", "r", encoding="utf-8") as f:
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

                #--------------------------------------------------#
                # Fetch doclink definitions for GraphSearch tables #
                #--------------------------------------------------#

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
    def print(self):

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
            print(f" 🐬 GraphSearch doc fields ............ {self.settings['graphsearch'  ]['fields' ]['docs'].get(doc_type, '')}")
            print(f" ⚡️ ElasticSearch doc fields .......... {self.settings['elasticsearch']['fields' ]['docs'].get(doc_type, '')}")
            print(f" ⚡️ ElasticSearch filters ............. {self.settings['elasticsearch']['filters']['docs'].get(doc_type, '')}")


            doc_type_as_link = doc_type

            # Loop over each doc-link type to print its specific options and field definitions
            for other_doc_type in self.settings['doc_types']:

                # Print out initialisation info
                print(f"\n🔗 Doc-Link type: {other_doc_type} --> {doc_type_as_link}\n")
                print(f" 🐬 GraphSearch link fields (default) .......... {self.settings['graphsearch'  ]['fields' ]['links']['default'     ].get(doc_type_as_link, '')}")
                print(f" 🐬 GraphSearch link fields (parent-child) ..... {self.settings['graphsearch'  ]['fields' ]['links']['parent_child'].get(other_doc_type, {}).get(doc_type_as_link, '')}")
                print(f" ⚡️ ElasticSearch link fields .................. {self.settings['elasticsearch']['fields' ]['links'].get(doc_type_as_link, '')}")
                print(f" ⚡️ ElasticSearch filters ...................... {self.settings['elasticsearch']['filters']['links'].get(doc_type_as_link, '')}")

        # End of print method
        print('')

# Main execution
if __name__ == "__main__":
    idxcfg = IndexConfig()
    idxcfg.print()