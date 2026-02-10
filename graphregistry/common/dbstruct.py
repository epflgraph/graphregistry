#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
import rich, json, re
from pathlib import Path

# Initialize configuration objects
glbcfg = GlobalConfig()
idxcfg = IndexConfig()
scrcfg = ScoresConfig()

# SQL data type mapping dictionary
sql_data_type_mapping = {
    'char'     : 'VARCHAR(255)',
    'text'     : 'MEDIUMTEXT',
    'longtext' : 'LONGTEXT',
    'int'      : 'MEDIUMINT UNSIGNED',
    'bool'     : 'TINYINT(1)',
    'date'     : 'DATE',
    'datetime' : 'TIMESTAMP'
}

# Define mapping from field datatypes onto "castable" types
cast_mapping = {
    "TINYINT(1)"        : "CAST(%s AS UNSIGNED)",
    "SMALLINT UNSIGNED" : "CAST(%s AS UNSIGNED)",
    "YEAR"              : "CAST(%s AS UNSIGNED)",
    "VARCHAR(16)"       : "CAST(%s AS CHAR)"
}

# Function to flatten config schema and remove duplicates
def flatten_schema_remove_duplicates(schema: dict) -> dict:
    """
    Takes a schema dict with top-level sections like "data-types" and "data-keys",
    removes the 2nd-level table names ("from_to_edges", "object", ...), and returns:
      {section_name: {field_name: value, ...}, ...}
    If a field name appears multiple times within the same section, it is kept once
    (first occurrence wins).
    """
    out: dict[str, dict[str, str]] = {}

    # Iterate through top-level sections (e.g. "data-types", "data-keys")
    for section_name, tables in schema.items():
        if not isinstance(tables, dict):
            continue

        # Iterate through 2nd-level tables and flatten into single dict of field_name: value pairs
        flat: dict[str, str] = {}
        for _, fields in tables.items():          # drop table name
            if not isinstance(fields, dict):
                continue
            for k, v in fields.items():          # dedupe by field name
                if k not in flat:
                    flat[k] = v

        # Add flattened section to output
        out[section_name] = flat

    # Return flattened and deduped schema
    return out

# Fetch index field datatypes from config file
with open(Path(__file__).resolve().parents[2] / 'database/init/config/config_datatypes.json', 'r', encoding="utf-8") as f:
    core_datatypes_config = json.load(f)

# Flatten config schema and remove duplicates
core_datatypes_config_flat = flatten_schema_remove_duplicates(core_datatypes_config)

# Print flattened config for verification
# rich.print_json(data=core_datatypes_config_flat)

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
    def get_all_doc_fields(self, doc_type, index_group):

        # Get field list helpers
        id_fields_wi  = self.get_id_fields(unit_type='node', convention='doc-link', include_institution=True)
        id_fields_woi = self.get_id_fields(unit_type='node', convention='doc-link', include_institution=False)
        option_fields = list(idxcfg.settings['options'].keys())
        # print(type(option_fields))
        # print(option_fields)
        custom_fields = self.get_custom_fields(doc_type=doc_type, index_group=index_group)

        # Combine and return according to index group
        if index_group=='indexbuildup':
            return id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group=='indexrollback':
            return ['rollback_date'] + id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group=='graphsearch':
            return id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
        elif index_group=='elasticsearch':
            return id_fields_woi + ['degree_score', 'short_code', 'subtype_en', 'subtype_fr', 'name_en', 'name_fr', 'short_description_en', 'short_description_fr', 'long_description_en', 'long_description_fr'] + custom_fields + ['row_id']

    # General simplified method to get all combined fields
    def get_all_doclink_fields(self, doc_type, link_type, link_subtype, index_group):

        # Get field list helpers
        id_fields_wi  = self.get_id_fields(unit_type='edge', convention='doc-link', include_institution=True)
        id_fields_woi = self.get_id_fields(unit_type='edge', convention='doc-link', include_institution=False)
        custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Combine and return according to index group
        if index_group=='graphsearch':
            return id_fields_wi + custom_fields + ['degree_score', 'row_id']
        elif index_group=='elasticsearch':
            return id_fields_woi + ['link_rank', 'link_name_en', 'link_name_fr', 'link_short_description_en', 'link_short_description_fr'] + custom_fields + ['row_id']

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
                return self.get_doc_custom_fields_graphsearch(doc_type)
            elif index_group=='elasticsearch':
                return self.get_doc_custom_fields_elasticsearch(doc_type)
        else:
            if   index_group=='graphsearch':
                return self.get_doclink_custom_fields_graphsearch(doc_type, link_type, link_subtype)
            elif index_group=='elasticsearch':
                return self.get_doclink_custom_fields_elasticsearch(doc_type, link_type)

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

    # Convert list of fields into list of datatypes using config file
    def get_datatypes_from_fields(self, fields_list):
        datatypes_list = []
        for field_name in fields_list:
            if core_datatypes_config_flat['data-types'].get(field_name) is not None:
                datatypes_list += [core_datatypes_config_flat['data-types'][field_name]]
            elif idxcfg.settings['data_types'].get(field_name) is not None:
                datatypes_list += [sql_data_type_mapping[idxcfg.settings['data_types'][field_name]]]
            else:
                print(f"❌ No datatype found in config: {field_name}")
                exit()
        return datatypes_list

    # Get SQL table name for a given doc type, link type, link subtype, and index group
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

    # Generate SQL create table
    def get_sql_create_table(self, doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):

        # Get fields list based on node or edge type
        if link_type is None:
            fields_list = self.get_all_doc_fields(doc_type=doc_type, index_group=index_group)
        else:
            fields_list = self.get_all_doclink_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Convert fields list to datatypes list
        datatypes_list = self.get_datatypes_from_fields(fields_list)

        # Combine into list of "field datatype" strings
        field_definitions = [f"{field} {datatype}" for field, datatype in zip(fields_list, datatypes_list)]

        # Get table name
        sql_table_name = self.get_sql_table_name(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=include_schema)

        # Generate SQL create table statement with field datatype definitions
        sql_create_table = f"CREATE TABLE {sql_table_name} (\n  " + ",\n  ".join(field_definitions)

        # Get id fields for unique key definition
        doc_id_fields = self.get_doc_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)

        # Include key creation
        doc_custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Create composite unique key from id fields
        sql_create_table += f",\n  UNIQUE KEY uid ({', '.join(doc_id_fields)})"

        # Make all id fields keys
        for doc_id_field in doc_id_fields:
            sql_create_table += f",\n  KEY ({doc_id_field})"

        # Make all custom fields keys
        for custom_field in doc_custom_fields:
            sql_create_table += f",\n  KEY ({custom_field})"

        # Make row_id the primary key if it is included in the fields list
        if 'row_id' in fields_list:
            sql_create_table += ",\n  PRIMARY KEY (row_id)"

        # Finish SQL statement
        sql_create_table += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"

        # Return comple SQL statement
        return sql_create_table

    # Generate SQL create table
    def get_sql_alter_table(self, doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):

        # Get fields list based on node or edge type
        if link_type is None:
            fields_list = self.get_all_doc_fields(doc_type=doc_type, index_group=index_group)
        else:
            fields_list = self.get_all_doclink_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Convert fields list to datatypes list
        datatypes_list = self.get_datatypes_from_fields(fields_list)

        # Combine into list of "field datatype" strings
        field_definitions = [f"{field} {datatype}" for field, datatype in zip(fields_list, datatypes_list)]

        # Get table name
        sql_table_name = self.get_sql_table_name(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=include_schema)

        # Start by dropping primary key
        sql_drop_primary_key = f"ALTER TABLE {sql_table_name} DROP PRIMARY KEY;"

        # Generate SQL create table statement with field datatype definitions
        sql_alter_table = f"ALTER TABLE {sql_table_name}\n"

        # Append the modify column statements
        for field_definition in field_definitions:
            field_name = field_definition.split()[0]
            sql_alter_table += f"  MODIFY COLUMN {field_definition},\n"

        # Get id fields for unique key definition
        doc_id_fields = self.get_doc_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)

        # Include key creation
        doc_custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Create composite unique key from id fields
        sql_alter_table += f"  ADD UNIQUE KEY IF NOT EXISTS uid ({', '.join(doc_id_fields)})"

        # Make all id fields keys
        for doc_id_field in doc_id_fields:
            sql_alter_table += f",\n  ADD KEY IF NOT EXISTS ({doc_id_field})"

        # Make all custom fields keys
        for custom_field in doc_custom_fields:
            sql_alter_table += f",\n  ADD KEY IF NOT EXISTS ({custom_field})"

        # Make row_id the primary key if it is included in the fields list
        if 'row_id' in fields_list:
            sql_alter_table += ",\n  ADD PRIMARY KEY IF NOT EXISTS (row_id)"

        # Finish SQL statement
        sql_alter_table += ";"

        # Return comple SQL statement
        return sql_drop_primary_key, sql_alter_table

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

#===============================#
# Class definition: Graph Table #
#===============================#
class GraphTable():

    # Constructor
    def __init__(self, doc_type=None, link_type=None, link_subtype=None, index_group=None, schema_name=None, table_name=None):

        # Initialise input parameters
        self.doc_type     = doc_type
        self.link_type    = link_type
        self.link_subtype = link_subtype
        self.index_group  = index_group
        self.schema_name  = schema_name
        self.table_name   = table_name

        #------------------------------------#
        # Extract parameters from table name #
        #------------------------------------#
        if doc_type is None and table_name is not None:

            # Index buildup table (docs)
            if table_name.startswith('IndexBuildup_Fields_Docs_'):
                self.doc_type = table_name.replace('IndexBuildup_Fields_Docs_', '')
                self.index_group = 'indexbuildup'
                self.schema_name = glbcfg.mysql_schema_names['test']['graph_cache'] if self.schema_name is None else self.schema_name

            # Index buildup table (doclinks)
            elif table_name.startswith('IndexBuildup_Fields_Links_ParentChild_'):
                self.doc_type, self.link_type = table_name.replace('IndexBuildup_Fields_Links_ParentChild_', '').split('_')
                self.link_subtype = 'ORG'
                self.index_group = 'indexbuildup'
                self.schema_name = glbcfg.mysql_schema_names['test']['graph_cache'] if self.schema_name is None else self.schema_name

            # GraphSearch schema
            if self.schema_name == glbcfg.mysql_schema_names['test']['graphsearch']:

                # Index doc tables
                if self.table_name and re.match(r"Index_D_([^\_]*)$", self.table_name):
                    self.doc_type = re.findall(r"Index_D_([^\_]*)$", self.table_name)[0]
                    self.index_group = 'graphsearch'

                # Index doclink tables
                elif self.table_name and re.match(r"Index_D_([^\_]*)_L_([^\_]*)_T_(ORG|SEM)$", self.table_name):
                    self.doc_type, self.link_type, self.link_subtype = re.findall(r"Index_D_([^\_]*)_L_([^\_]*)_T_(ORG|SEM)$", self.table_name)[0]
                    self.index_group = 'graphsearch'

            # ElasticSearch schema
            elif self.schema_name == glbcfg.mysql_schema_names['test']['es_cache']:

                # Index doc tables
                if self.table_name and re.match(r"Index_D_([^\_]*)$", self.table_name):
                    self.doc_type = re.findall(r"Index_D_([^\_]*)$", self.table_name)[0]
                    self.index_group = 'elasticsearch'

                # Index doclink tables
                elif self.table_name and re.match(r"Index_D_([^\_]*)_L_([^\_]*)$", self.table_name):
                    self.doc_type, self.link_type = re.findall(r"Index_D_([^\_]*)_L_([^\_]*)$", self.table_name)[0]
                    self.index_group = 'elasticsearch'

        # Determine tables type (doc vs doclink)
        self.table_type = 'doc' if link_type is None else 'doclink'


        print(self.doc_type)
        print(self.link_type)
        print(self.link_subtype)
        print(self.index_group)
        print(self.schema_name)
        print(self.table_name)

#-------------------------------#
# Command line execution script #
#-------------------------------#
if __name__ == "__main__":


    tb = GraphTable(
        doc_type     = None,
        link_type    = None,
        link_subtype = None,
        index_group  = None,
        schema_name  = glbcfg.mysql_schema_names['test']['es_cache'],
        table_name   = 'Index_D_Category_L_Category'
    )

    exit()

    dbstr = DBStruct()

    sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(
        doc_type       = 'Category',
        index_group    = 'elasticsearch',
        include_schema = True
    )

    print('\n')
    print(sql_drop_primary_key)
    print(sql_alter_table)
    print('\n')

    exit()

    sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(
        doc_type       = 'Person',
        link_type      = 'Concept',
        link_subtype   = 'SEM',
        index_group    = 'elasticsearch',
        include_schema = True
    )
    print(sql_drop_primary_key)
    print(sql_alter_table)


    # # print(dbstr.get_fields(doc_type='Person', index_group='graphsearch'))
    # # print(dbstr.get_fields(doc_type='Person', index_group='elasticsearch'))
    # # print(dbstr.get_fields(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='graphsearch'))
    # # print(dbstr.get_fields(doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    # # print(dbstr.get_fields(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch'))
    # # print(dbstr.get_fields(doc_type='Unit'  , link_type='Unit'   , link_subtype='ORG', index_group='graphsearch'))
    # # print('\n')

    # # print(dbstr.get_sql_table_name       (doc_type='Person', index_group='graphsearch', include_schema=False))
    # # print(dbstr.get_sql_table_name       (doc_type='Person', index_group='graphsearch', include_schema=True))
    # # fields_list = dbstr.get_all_doc_fields       (doc_type='Person', index_group='graphsearch')
    # # print(f"Fields for Person (graphsearch): {fields_list}")
    # # datatypes_list = dbstr.get_datatypes_from_fields(fields_list)
    # # print(f"Datatypes for Person fields: {datatypes_list}")
    # sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(doc_type='Person', index_group='graphsearch', include_schema=True)
    # print(sql_drop_primary_key)
    # print(sql_alter_table)
    # print('\n')

    # # print(dbstr.get_sql_table_name       (doc_type='Person', index_group='elasticsearch', include_schema=False))
    # # print(dbstr.get_sql_table_name       (doc_type='Person', index_group='elasticsearch', include_schema=True))
    # # fields_list = dbstr.get_all_doc_fields       (doc_type='Person', index_group='elasticsearch')
    # # print(f"Fields for Person (elasticsearch): {fields_list}")
    # # datatypes_list = dbstr.get_datatypes_from_fields(fields_list)
    # # print(f"Datatypes for Person fields (elasticsearch): {datatypes_list}")
    # sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(doc_type='Person', index_group='elasticsearch', include_schema=True)
    # print(sql_drop_primary_key)
    # print(sql_alter_table)
    # print('\n')

    # # print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch', include_schema=False))
    # # print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch', include_schema=True))
    # # fields_list = dbstr.get_all_doclink_fields   (doc_type='Person', link_type='Unit'   , link_subtype='ORG', index_group='graphsearch')
    # # print(f"Fields for Person-Unit (ORG) fields: {fields_list}")
    # # datatypes_list = dbstr.get_datatypes_from_fields(fields_list)
    # # print(f"Datatypes for Person-Unit (ORG) fields: {datatypes_list}")
    # sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(doc_type='Person', link_type='Unit', link_subtype='ORG', index_group='graphsearch', include_schema=True)
    # print(sql_drop_primary_key)
    # print(sql_alter_table)
    # print('\n')

    # # print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch', include_schema=False))
    # # print(dbstr.get_sql_table_name       (doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch', include_schema=True))
    # # fields_list = dbstr.get_all_doclink_fields   (doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch')
    # # print(f"Fields for Person-Concept (SEM) fields: {fields_list}")
    # # datatypes_list = dbstr.get_datatypes_from_fields(fields_list)
    # # print(f"Datatypes for Person-Concept (SEM) fields: {datatypes_list}")
    # sql_drop_primary_key, sql_alter_table = dbstr.get_sql_alter_table(doc_type='Person', link_type='Concept', link_subtype='SEM', index_group='elasticsearch', include_schema=True)
    # print(sql_drop_primary_key)
    # print(sql_alter_table)
    # print('\n')