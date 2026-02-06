#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
import rich, json
from pathlib import Path

# Initialize configuration objects
glbcfg = GlobalConfig()
idxcfg = IndexConfig()
scrcfg = ScoresConfig()

# Fetch index field datatypes from config file
with open(Path(__file__).resolve().parents[2] / 'database/init/config/config_datatypes.json', 'r', encoding="utf-8") as f:
    core_datatypes_config = json.load(f)

# SQL data type mapping dictionary
sql_data_type_mapping = {
    'char' : 'VARCHAR(255)',
    'text' : 'MEDIUMTEXT',
    'longtext' : 'LONGTEXT',
    'int' : 'MEDIUMINT UNSIGNED',
    'bool' : 'TINYINT(1)',
    'date' : 'DATE',
    'datetime' : 'TIMESTAMP'
}

# Define mapping from field datatypes onto "castable" types
cast_mapping = {
    "TINYINT(1)"        : "CAST(%s AS UNSIGNED)",
    "SMALLINT UNSIGNED" : "CAST(%s AS UNSIGNED)",
    "YEAR"              : "CAST(%s AS UNSIGNED)",
    "VARCHAR(16)"       : "CAST(%s AS CHAR)"
}

rich.print_json(data=core_datatypes_config)

#============================================#
# Class definition: Graph Database Structure #
#============================================#
class DBStruct():

    # Constructor
    def __init__(self):

        #-------------------------------#
        # Basic configuation parameters #
        #-------------------------------#

        # Get available doc types
        self.doc_types = idxcfg.settings['doc_types']

        # Get edge types to score
        self.edge_types_to_score = scrcfg.settings['scored_edge_tuples']['research'] + scrcfg.settings['scored_edge_tuples']['education']

        # Append ontology-related edges
        self.edge_types_to_score += [[d,'Concept' ] for d in self.doc_types]
        self.edge_types_to_score += [[d,'Category'] for d in self.doc_types]
        self.edge_types_to_score += [['Concept','Concept'], ['Category','Category'], ['Category','Concept']]

        # Clean, sort, and convert to tuples
        self.edge_types_to_score = [tuple(sorted(t)) for t in self.edge_types_to_score]
        self.edge_types_to_score = sorted(list(set(self.edge_types_to_score)))

        # Generate complete list of semantic doclink tuples
        self.doclink_types_sem  = [tuple(t) for t in self.edge_types_to_score]
        self.doclink_types_sem += [tuple(reversed(t)) for t in self.edge_types_to_score if t[0]!=t[1]]
        self.doclink_types_sem  = sorted(self.doclink_types_sem)

        # Get available parent-to-child tuples
        p2c_tuples = []
        for k1 in idxcfg.settings['graphsearch']['fields']['links']['parent_child'].keys():
            for k2 in idxcfg.settings['graphsearch']['fields']['links']['parent_child'][k1].keys():
                p2c_tuples += [tuple(sorted([k1,k2]))]

        # Append ontology tuples
        p2c_tuples += [('Category','Category'), ('Category','Concept')]

        # Clean and sort list
        p2c_tuples = sorted(list(set(p2c_tuples)))
        
        # Generate complete list of organisational doclink tuples
        self.doclink_types_org  = [tuple(t) for t in p2c_tuples]
        self.doclink_types_org += [tuple(reversed(t)) for t in p2c_tuples if t[0]!=t[1]]
        self.doclink_types_org  = sorted(self.doclink_types_org)

        #--------------------#
        # Initialise objects #
        #--------------------#

        # Initialise Doc objects
        self.docs = {}
        for doc_type in self.doc_types:
            self.docs[doc_type] = self.Doc(doc_type=doc_type)

        # Initialise DocLink objects (semantic)
        self.doclinks_sem = {}
        for (doc_type, link_type) in self.doclink_types_sem:
            self.doclinks_sem[(doc_type, link_type)] = self.DocLink(doc_type=doc_type, link_type=link_type, link_subtype='SEM')

        # Initialise DocLink objects (organisational)
        self.doclinks_org = {}
        for (doc_type, link_type) in self.doclink_types_org:
            self.doclinks_org[(doc_type, link_type)] = self.DocLink(doc_type=doc_type, link_type=link_type, link_subtype='ORG')

    #----------------------------------#
    # Method group: Export field lists #
    #----------------------------------#

    #===== All fields =====#

    # General simplified method to get all combined fields
    def get_all_doc_fields(self, doc_type, index_group, include_institution=True):
        
        # Get field list helpers
        id_fields_wi  = self.get_id_fields(unit_type='node', convention='doc-link', include_institution=True)
        id_fields_woi = self.get_id_fields(unit_type='node', convention='doc-link', include_institution=False)
        option_fields = idxcfg.settings['options'].keys()
        custom_fields = self.get_custom_fields(doc_type=doc_type, index_group=index_group)

        # Combine and return according to index group
        if index_group=='indexbuildup':
            return id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group=='indexrollback':
            return ['rollback_date'] + id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group=='graphsearch':
            return id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group='elasticsearch':
            return id_fields_woi + ['degree_score', 'short_code', 'subtype_en', 'subtype_fr', 'name_en', 'name_fr', 'short_description_en', 'short_description_fr', 'long_description_en', 'long_description_fr'] + custom_fields + ['row_id']

    # TODO:
    # - same as above, but for doclinks
    # - general function that converts al the fields above into SQL column datatype definitions
    #   (probably can drop a few of the existing methods)
    # - add create table functions and the rest

    #===== ID fields =====#

    # General simplified method to get id-defining fields
    def get_id_fields(self, unit_type, convention, include_institution=True):
        if   unit_type=='node':
            return self.get_doc_id_fields(convention, include_institution)
        elif unit_type=='edge':
            return self.get_doclink_id_fields(convention, include_institution)

    # Export graphsearch doc id-defining fields for docs
    def get_doc_id_fields(self, convention, include_institution=True):
        if convention=='node-edge':
            if include_institution:
                return ['institution_id', 'object_type', 'object_id']
            else:
                return ['object_type', 'object_id']
        elif convention=='doc-link':
            if include_institution:
                return ['doc_institution', 'doc_type', 'doc_id']
            else:
                return ['doc_type', 'doc_id']

    # Export graphsearch doc id-defining fields for doclinks
    def get_doclink_id_fields(self, convention, include_institution=True):
        if convention=='node-edge':
            if include_institution:
                return ['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id', 'to_object_type', 'to_object_id']
            else:
                return ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id']
        elif convention=='doc-link':
            if include_institution:
                return ['doc_institution', 'doc_type', 'doc_id', 'link_institution', 'link_type', 'link_subtype', 'link_id']
            else:
                return ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id']

    #===== Custom fields =====#

    # General simplified method to get custom fields
    def get_custom_fields(self, doc_type, link_type=None, link_subtype=None, index_group=None):
        if link_type is None:
            if   index_group=='graphsearch':
                return self.get_doc_graphsearch_fields(doc_type)
            elif index_group=='elasticsearch':
                return self.get_doc_elasticsearch_fields(doc_type)
        else:
            if   index_group=='graphsearch':
                return self.get_doclink_graphsearch_fields(doc_type, link_type, link_subtype)
            elif index_group=='elasticsearch':
                return self.get_doclink_elasticsearch_fields(doc_type, link_type)

    # Export graphsearch doc fields for a given doc type
    def get_doc_custom_fields_graphsearch(self, doc_type):
        return self.docs[doc_type].graphsearch_obj_fields

    # Export elasticsearch doc fields for a given doc type
    def get_doc_custom_fields_elasticsearch(self, doc_type):
        return self.docs[doc_type].elasticsearch_obj_fields

    # Export graphsearch doclink fields for a given doc type, link type, and link subtype (semantic or organisational)
    def get_doclink_custom_fields_graphsearch(self, doc_type, link_type, link_subtype):
        if   link_subtype.upper() == 'SEM':
            return self.doclinks_sem[(doc_type, link_type)].graphsearch_obj_fields + self.doclinks_sem[(doc_type, link_type)].graphsearch_obj2obj_fields
        elif link_subtype.upper() == 'ORG':
            return self.doclinks_org[(doc_type, link_type)].graphsearch_obj_fields + self.doclinks_org[(doc_type, link_type)].graphsearch_obj2obj_fields

    # Export elasticsearch doclink fields for a given doc type, link type, and link subtype (semantic or organisational)
    def get_doclink_custom_fields_elasticsearch(self, doc_type, link_type):
        if (doc_type, link_type) in self.doclinks_org:
            return self.doclinks_org[(doc_type, link_type)].elasticsearch_obj_fields
        else:
            return self.doclinks_sem[(doc_type, link_type)].elasticsearch_obj_fields
    
    #----------------------------------#
    # Method group: Export dynamic SQL #
    #----------------------------------#

    # General method to get SQL table name for a given doc type, link type, link subtype, and index group
    def get_sql_table_name(self, doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):
        if link_type is None:
            if index_group in ('graphsearch', 'elasticsearch'):
                return f"{glbcfg.mysql_schema_names['test']['graphsearch' if index_group=='graphsearch' else 'es_cache']+'.' if include_schema else ''}Index_D_{doc_type}"
            elif index_group=='indexbuildup':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexBuildup_Fields_Docs_{doc_type}"
            elif index_group=='indexrollback':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexRollback_Fields_Docs_{doc_type}"
        else:
            if index_group=='graphsearch':
                return f"{glbcfg.mysql_schema_names['test']['graphsearch']+'.' if include_schema else ''}Index_D_{doc_type}_L_{link_type}_T_{link_subtype.upper()}"
            elif index_group=='elasticsearch':
                return f"{glbcfg.mysql_schema_names['test']['es_cache']+'.' if include_schema else ''}Index_D_{doc_type}_L_{link_type}"
            elif index_group=='indexbuildup':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexBuildup_Fields_Links_ParentChild_{sorted([doc_type,link_type])[0]}_{sorted([doc_type,link_type])[1]}"
            elif index_group=='indexrollback':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexRollback_Fields_Links_ParentChild_{sorted([doc_type,link_type])[0]}_{sorted([doc_type,link_type])[1]}"

    # General method to get SQL id field definitions for a given doc type, link type, link subtype, and index group
    def get_sql_id_definitions(self, doc_type, link_type=None, link_subtype=None):
        if link_type is None:
            id_fields = self.get_id_fields(unit_type='doc', convention='doc-link', include_institution=True)
            id_definitions = ',\n'.join([f"{field_name} {core_datatypes_config['data-types']['doc_index'][field_name]} NOT NULL" for field_name in id_fields])
        else:
            id_fields = self.get_id_fields(unit_type='doclink', convention='doc-link', include_institution=True)
            id_definitions = ',\n'.join([f"{field_name} {core_datatypes_config['data-types']['link_index'][field_name]} NOT NULL" for field_name in id_fields])
        return id_definitions

    # General method to get SQL field definitions for a given doc type, link type, link subtype, and index group
    def get_sql_field_definitions(self, doc_type, link_type=None, link_subtype=None, index_group=None):
        fields_list = self.get_fields(doc_type, link_type, link_subtype, index_group)
        field_definitions = ',\n'.join([f"{field_name} {sql_data_type_mapping[idxcfg.settings['data_types'][field_name]]} DEFAULT NULL" for field_name in fields_list])
        return field_definitions

    # General method to get SQL field keys for a given doc type, link type, link subtype, and index group
    def get_sql_field_keys(self, doc_type, link_type=None, link_subtype=None, index_group=None):
        fields_list = self.get_fields(doc_type, link_type, link_subtype, index_group)
        field_keys = ', '.join([f"KEY {field_name}" for field_name in fields_list])
        return field_keys

    #----------------------------------#
    # Sub-class definition: Doc object #
    #----------------------------------#
    class Doc():
        
        # Constructor
        def __init__(self, doc_type):
            self.doc_type = doc_type
            self.options = {}
            for k in idxcfg.settings['options'].keys():
                self.options[k] = idxcfg.settings['options'][k][self.doc_type]
            self.graphsearch_obj_fields   = idxcfg.settings['graphsearch'  ]['fields' ]['docs'].get(self.doc_type, [])
            self.elasticsearch_obj_fields = idxcfg.settings['elasticsearch']['fields' ]['docs'].get(self.doc_type, [])

            # print('\n\n')
            # print(self.doc_type)
            # rich.print_json(data=self.options)
            # print(self.graphsearch_obj_fields)
            # print(self.elasticsearch_obj_fields)

    #--------------------------------------#
    # Sub-class definition: DocLink object #
    #--------------------------------------#
    class DocLink():
        
        # Constructor
        def __init__(self, doc_type, link_type, link_subtype):
            self.doc_type     = doc_type
            self.link_type    = link_type
            self.link_subtype = link_subtype
            self.graphsearch_obj_fields     = idxcfg.settings['graphsearch'  ]['fields' ]['links']['default'].get(self.link_type, [])
            self.graphsearch_obj2obj_fields = idxcfg.settings['graphsearch'  ]['fields' ]['links']['parent_child'].get(self.doc_type, {}).get(self.link_type, []) if link_subtype.upper() == 'ORG' else []
            self.elasticsearch_obj_fields   = idxcfg.settings['elasticsearch']['fields' ]['links'].get(self.link_type, [])
            
            # print('\n\n')
            # print(self.doc_type, self.link_type, self.link_subtype)
            # print(self.graphsearch_obj_fields)
            # print(self.graphsearch_obj2obj_fields)
            # print(self.elasticsearch_obj_fields)


#-------------------------------#
# Command line execution script #
#-------------------------------#
if __name__ == "__main__":

    dbstr = DBStruct()
    print(dbstr.get_fields(doc_type='Person', index_group='graphsearch'))
    print(dbstr.get_fields(doc_type='Person', index_group='elasticsearch'))
    print(dbstr.get_fields(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='graphsearch'))
    print(dbstr.get_fields(doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    print(dbstr.get_fields(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch'))
    print(dbstr.get_fields(doc_type='Unit'  , link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    print('\n')

    print(dbstr.get_sql_table_name       (doc_type='Person', index_group='graphsearch', include_schema=False))
    print(dbstr.get_sql_table_name       (doc_type='Person', index_group='graphsearch', include_schema=True))
    print(dbstr.get_sql_id_definitions   (doc_type='Person'))
    print(dbstr.get_sql_field_definitions(doc_type='Person', index_group='graphsearch'))
    print(dbstr.get_sql_field_keys(       doc_type='Person', index_group='graphsearch'))
    print('\n')

    print(dbstr.get_sql_table_name       (doc_type='Person', index_group='elasticsearch', include_schema=False))
    print(dbstr.get_sql_table_name       (doc_type='Person', index_group='elasticsearch', include_schema=True))
    print(dbstr.get_sql_id_definitions   (doc_type='Person'))
    print(dbstr.get_sql_field_definitions(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='graphsearch'))
    print(dbstr.get_sql_field_keys(       doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='graphsearch'))
    print('\n')

    print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch', include_schema=False))
    print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch', include_schema=True))
    print(dbstr.get_sql_id_definitions   (doc_type='Person', link_type='Unit'   , link_subtype='ORG'))
    print(dbstr.get_sql_field_definitions(doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    print(dbstr.get_sql_field_keys(       doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    print('\n')

    print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch', include_schema=False))
    print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch', include_schema=True))
    print(dbstr.get_sql_id_definitions   (doc_type='Person', link_type='Concept', link_subtype='SEM'))
    print(dbstr.get_sql_field_definitions(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch'))
    print(dbstr.get_sql_field_keys(       doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch'))
    print('\n')