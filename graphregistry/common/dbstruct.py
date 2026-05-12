# graphregistry/common/dbstruct.py
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
import rich, json, re
from pathlib import Path

# TODO: Check presence of column name "context" in all edge definitions
# TODO: Some keys are not being created in elasticsearch cache schemas

# Initialize configuration objects
glbcfg = GlobalConfig()
idxcfg = IndexConfig()
scrcfg = ScoresConfig()


def _find_repo_root(start: Path | None = None) -> Path:
    start = (start or Path(__file__)).resolve()

    for parent in [start, *start.parents]:
        if (parent / "graphregistry").is_dir() and (parent / "config").is_dir():
            return parent

    raise RuntimeError(f"Could not find repository root from: {start}")


REPO_ROOT = _find_repo_root()
CONFIG_DB_PATH = REPO_ROOT / "config" / "config_db.yaml"

db_cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)



# Initialise MySQL client
# db_cfg = GraphDBConfig.from_file("config/config_db.yaml")
db = GraphDB(config=db_cfg)

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

#--------------------------------------------------------#
# Get list of SQL queries paths and store them as a dict #
#--------------------------------------------------------#

# Initialize empty dict to store SQL query paths
sql_queries_paths = {}

# Loop through all SQL query files in the "database/queries" folder and subfolders and store their paths in the dict
for file_path in (Path(__file__).resolve().parents[2] / 'database/queries').rglob('*.sql'):

    # Get subfolders
    subfolder_2, subfolder_1 = file_path.parent.name, file_path.parent.parent.name

    # Initialize nested dicts if they don't exist
    if subfolder_1 not in sql_queries_paths:
        sql_queries_paths[subfolder_1] = {}
    if subfolder_2 not in sql_queries_paths[subfolder_1]:
        sql_queries_paths[subfolder_1][subfolder_2] = {}

    # Store file path in dict using subfolder names and file stem as keys
    sql_queries_paths[subfolder_1][subfolder_2][file_path.stem] = file_path

#---------------------#
# Auxiliary functions #
#---------------------#

# Function that takes a query template with placeholders and replaces them with values from kwargs
def resolve_sql_query(file_path, **kwargs):

    # Open SQL query template file and read as string
    with open(file_path, 'r', encoding="utf-8") as f:
        query_template = f.read()

    # Replace placeholders in the query template with values from kwargs
    for key, value in kwargs.items():
        placeholder = f"[[{key}]]"
        query_template = query_template.replace(placeholder, str(value))

    # Return resolved query
    return query_template

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
class DynamicSQL():

    # Prevent from initialising twice
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DynamicSQL, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    # Constructor
    def __init__(self):

        # Prevent from initialising twice (e.g. when calling DynamicSQL() multiple times to access static methods)
        if getattr(self, "_initialized", False):
            return

        # Set initialization flag and print message
        self._initialized = True

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
        for     k1 in idxcfg.settings['graphsearch']['fields']['links']['parent_child'].keys():
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

    #------------------------------#
    # Method group: Static methods #
    #------------------------------#

    @staticmethod
    def get_fields(doc_type, link_type=None, link_subtype=None, index_group=None):
        if link_type is None:
            return DynamicSQL().get_all_doc_fields(doc_type=doc_type, index_group=index_group)
        else:
            return DynamicSQL().get_all_doclink_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

    @staticmethod
    def get_create_table(doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):
        return DynamicSQL().get_sql_create_table(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=include_schema)

    @staticmethod
    def get_alter_table(doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):
        return DynamicSQL().get_sql_alter_table(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=include_schema)

    @staticmethod
    def compare_fields(doc_type, link_type=None, link_subtype=None, index_group=None, engine_name='xaas_coresrv', schema_name=None):
        return DynamicSQL().compare_fields_with_table(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, engine_name=engine_name, schema_name=schema_name)

    #---------------------------------#
    # Method group: Table diagnostics #
    #---------------------------------#

    # Compare list of fields with the one currently in the table and return missing and extra fields
    def compare_fields_with_table(self, doc_type, link_type=None, link_subtype=None, index_group=None, engine_name='xaas_coresrv', schema_name=None):

        # Get list of fields based on doc type, link type, link subtype, and index group
        fields_in_config = self.get_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Typecheck
        if type(fields_in_config) is not list:
            print(f"❌ Error: fields_in_config must be a list. Got {type(fields_in_config)}")
            exit()

        # Get table name
        table_name = self.get_sql_table_name(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=False)

        # Get existing fields in the table
        fields_in_table = db.get_column_names(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

        # Get missing fields in existing table that are in the config (fields to add)
        missing_fields = [f for f in fields_in_config if f not in fields_in_table]

        # Get fields in existing table that are not in the config (fields to drop)
        fields_to_drop = [f for f in fields_in_table if f not in fields_in_config]

        # Return missing fields and fields to drop
        return missing_fields, fields_to_drop

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
        custom_fields = self.get_custom_fields(doc_type=doc_type, index_group=index_group)

        # Combine and return according to index group
        if type(id_fields_wi) is list and type(id_fields_woi) is list and type(option_fields) is list and type(custom_fields) is list:
            if index_group=='indexbuildup':
                return id_fields_wi + option_fields + custom_fields + ['degree_score', 'to_process', 'row_id']
            elif index_group=='indexrollback':
                return ['rollback_date'] + id_fields_wi + option_fields + custom_fields + ['degree_score', 'to_process', 'row_id']
            elif index_group=='graphsearch':
                return id_fields_wi + option_fields + custom_fields + ['degree_score', 'row_id']
            elif index_group=='elasticsearch':
                return id_fields_woi + ['degree_score', 'short_code', 'subtype_en', 'subtype_fr', 'name_en', 'name_fr', 'short_description_en', 'short_description_fr', 'long_description_en', 'long_description_fr'] + custom_fields + ['row_id']
            else:
                return []
        else:
            return []

    # General simplified method to get all combined fields
    def get_all_doclink_fields(self, doc_type, link_type, link_subtype, index_group):

        # Get field list helpers
        id_fields_wi  = self.get_id_fields(unit_type='edge', convention='doc-link', include_institution=True,  include_link_subtype=link_subtype is not None)
        id_fields_woi = self.get_id_fields(unit_type='edge', convention='doc-link', include_institution=False, include_link_subtype=link_subtype is not None or index_group=='elasticsearch')
        custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # print('==========>', "custom_fields ..... ", custom_fields)

        # Combine and return according to index group
        if type(id_fields_wi) is list and type(id_fields_woi) is list and type(custom_fields) is list:
            if index_group=='indexbuildup':
                return id_fields_wi + self.doclinks_org[(doc_type, link_type)].graphsearch_obj2obj_fields + ['to_process', 'row_id']
            elif index_group=='indexrollback':
                return ['rollback_date'] + id_fields_wi + self.doclinks_org[(doc_type, link_type)].graphsearch_obj2obj_fields + ['to_process', 'row_id']
            elif index_group=='graphsearch':
                return id_fields_wi + custom_fields + [{'ORG':'degree_score', 'SEM':'semantic_score'}[link_subtype.upper()], 'row_score', 'row_rank', 'row_id']
            elif index_group=='elasticsearch':
                return id_fields_woi + ['link_rank', 'link_name_en', 'link_name_fr', 'link_short_description_en', 'link_short_description_fr'] + custom_fields + ['row_id']
            else:
                return []
        else:
            return []

    #===== ID fields =====#

    # General simplified method to get id-defining fields
    def get_id_fields(self, unit_type, convention, include_institution=True, include_link_subtype=False):
        if   unit_type=='node':
            return self.get_doc_id_fields(convention=convention, include_institution=include_institution)
        elif unit_type=='edge':
            return self.get_doclink_id_fields(convention=convention, include_institution=include_institution, include_link_subtype=include_link_subtype)
        else:
            print("❌ Critical error [je42J1]: DynamicSQL.get_id_fields()")
            exit()

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
        else:
            print("❌ Critical error [F32gh3]: DynamicSQL.get_doc_id_fields()")
            exit()

    # Export graphsearch doc id-defining fields for doclinks
    def get_doclink_id_fields(self, convention, include_institution=True, include_link_subtype=False):
        if convention=='node-edge':
            if include_institution:
                return ['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id', 'to_object_type', 'to_object_id']
            else:
                return ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id']
        elif convention=='doc-link':
            if include_institution:
                if include_link_subtype:
                    return ['doc_institution', 'doc_type', 'doc_id', 'link_institution', 'link_type', 'link_subtype', 'link_id']
                else:
                    return ['doc_institution', 'doc_type', 'doc_id', 'link_institution', 'link_type', 'link_id']
            else:
                if include_link_subtype:
                    return ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id']
                else:
                    return ['doc_type', 'doc_id', 'link_type', 'link_id']
        else:
            print("❌ Critical error [KJ24rF]: DynamicSQL.get_doclink_id_fields()")
            exit()

    #===== Custom fields =====#

    # General simplified method to get custom fields
    def get_custom_fields(self, doc_type, link_type=None, link_subtype=None, index_group=None):
        if link_type is None:
            if index_group in ('indexbuildup', 'indexrollback', 'graphsearch'):
                return self.get_doc_custom_fields_graphsearch(doc_type)
            elif index_group=='elasticsearch':
                return self.get_doc_custom_fields_elasticsearch(doc_type)
            else:
                print("❌ Critical error [GrKw3-1]: DynamicSQL.get_custom_fields()")
                print("doc_type .........", doc_type)
                print("link_type ........", link_type)
                print("link_subtype .....", link_subtype)
                print("index_group ......", index_group)
                exit()
        else:
            if index_group in ('indexbuildup', 'indexrollback'):
                return self.get_doclink_custom_fields_indexbuildup(doc_type, link_type, link_subtype)
            elif index_group=='graphsearch':
                return self.get_doclink_custom_fields_graphsearch(doc_type, link_type, link_subtype)
            elif index_group=='elasticsearch':
                return self.get_doclink_custom_fields_elasticsearch(doc_type, link_type)
            else:
                print("❌ Critical error [GrKw3-2]: DynamicSQL.get_custom_fields()")
                print("doc_type .........", doc_type)
                print("link_type ........", link_type)
                print("link_subtype .....", link_subtype)
                print("index_group ......", index_group)
                exit()

    # Export graphsearch doc fields for a given doc type
    def get_doc_custom_fields_graphsearch(self, doc_type):
        return self.docs[doc_type].graphsearch_obj_fields

    # Export elasticsearch doc fields for a given doc type
    def get_doc_custom_fields_elasticsearch(self, doc_type):
        return self.docs[doc_type].elasticsearch_obj_fields

    # Export graphsearch doclink fields for a given doc type, link type, and link subtype (semantic or organisational)
    def get_doclink_custom_fields_indexbuildup(self, doc_type, link_type, link_subtype):
        fields_list = []
        if link_subtype.upper() == 'ORG':
            fields_list = self.doclinks_org[(doc_type, link_type)].graphsearch_obj2obj_fields
        return fields_list

    # Export graphsearch doclink fields for a given doc type, link type, and link subtype (semantic or organisational)
    def get_doclink_custom_fields_graphsearch(self, doc_type, link_type, link_subtype):
        fields_list = []
        if link_subtype.upper() == 'SEM':
            fields_list = self.doclinks_sem[(doc_type, link_type)].graphsearch_obj_fields
        elif link_subtype.upper() == 'ORG':
            fields_list = self.doclinks_org[(doc_type, link_type)].graphsearch_obj_fields
            if (doc_type, link_type) in self.doclinks_org:
                fields_list += [x for x in self.doclinks_org[(doc_type, link_type)].graphsearch_obj2obj_fields if x not in fields_list]
        return fields_list

    # Export elasticsearch doclink fields for a given doc type, link type, and link subtype (semantic or organisational)
    def get_doclink_custom_fields_elasticsearch(self, doc_type, link_type):
        if (doc_type, link_type) in self.doclinks_org:
            if (doc_type, link_type) in self.doclinks_org:
                return self.doclinks_org[(doc_type, link_type)].elasticsearch_obj_fields
            else:
                return []
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
                raise Exception(f"❌ No datatype found in config: {field_name}")
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
                print("❌ Critical error [91JdA]: DynamicSQL.get_sql_table_name()")
                exit()
        else:
            if index_group=='graphsearch':
                if link_subtype is None:
                    print(f"❌ Error: link_subtype must be specified for index_group='graphsearch'")
                    exit()
                return f"{glbcfg.mysql_schema_names['test']['graphsearch']+'.' if include_schema else ''}Index_D_{doc_type}_L_{link_type}_T_{link_subtype.upper()}"
            elif index_group=='elasticsearch':
                return f"{glbcfg.mysql_schema_names['test']['es_cache']+'.' if include_schema else ''}Index_D_{doc_type}_L_{link_type}"
            elif index_group=='indexbuildup':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexBuildup_Fields_Links_ParentChild_{sorted([doc_type,link_type])[0]}_{sorted([doc_type,link_type])[1]}"
            elif index_group=='indexrollback':
                return f"{glbcfg.mysql_schema_names['test']['graph_cache']+'.' if include_schema else ''}IndexRollback_Fields_Links_ParentChild_{sorted([doc_type,link_type])[0]}_{sorted([doc_type,link_type])[1]}"
            else:
                print("❌ Critical error [91JdA]: DynamicSQL.get_sql_table_name()")
                exit()

    # Generate SQL create table
    def get_sql_create_table(self, doc_type, link_type=None, link_subtype=None, index_group=None, include_schema=False):

        # Get fields list based on node or edge type
        if link_type is None:
            fields_list = self.get_all_doc_fields(doc_type=doc_type, index_group=index_group)
        else:
            fields_list = self.get_all_doclink_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # print("*************>>>>", "fields_list ..... ", fields_list)

        # Convert fields list to datatypes list
        datatypes_list = self.get_datatypes_from_fields(fields_list)

        # print("*************>>>>", "datatypes_list ..... ", datatypes_list)

        # Combine into list of "field datatype" strings
        if type(fields_list) is list and type(datatypes_list) is list and len(fields_list)==len(datatypes_list):
            field_definitions = [f"{field} {datatype}" for field, datatype in zip(fields_list, datatypes_list)]
        else:
            print(f"❌ Error combining fields and datatypes into field definitions: {fields_list} and {datatypes_list}")
            exit()

        # Get table name
        sql_table_name = self.get_sql_table_name(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group, include_schema=include_schema)

        # Generate SQL create table statement with field datatype definitions
        sql_create_table = f"CREATE TABLE {sql_table_name} (\n  " + ",\n  ".join(field_definitions)

        # Get id fields for unique key definition
        if link_type is None:
            id_fields = self.get_doc_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)
        else:
            id_fields = self.get_doclink_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)

        # Include key creation
        doc_custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # print("*************>>>>", "id_fields ..... ", id_fields)
        # print("*************>>>>", "doc_custom_fields ..... ", doc_custom_fields)

        # Typecheck
        if not (type(id_fields) is list and type(doc_custom_fields) is list):
            print(f"❌ Error: doc_id_fields and doc_custom_fields must be lists. Got {type(id_fields)} and {type(doc_custom_fields)}")
            exit()

        # Create composite unique key from id fields
        sql_create_table += f",\n  UNIQUE KEY uid ({', '.join(id_fields)})"

        # Add key for to_process field if exists
        if 'to_process' in fields_list:
            sql_create_table += f",\n  KEY to_process (to_process)"

        # Make all id fields keys
        for id_field_k in id_fields:
            sql_create_table += f",\n  KEY ({id_field_k})"

        # Make all custom fields keys
        # for custom_field in doc_custom_fields:
        #     sql_create_table += f",\n  KEY ({custom_field})"
        for custom_field in doc_custom_fields:
            # MySQL does not allow indexing of TEXT fields without a subset length
            subset = "(255)" if datatypes_list[fields_list.index(custom_field)] in ['MEDIUMTEXT', 'LONGTEXT'] else ""  # Add subset length for TEXT fields to allow indexing  
            sql_create_table += f",\n  KEY ({custom_field}{subset})"


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

        # Typecheck
        if not (type(fields_list) is list and type(datatypes_list) is list and len(fields_list)==len(datatypes_list)):
            print(f"❌ Error: fields_list and datatypes_list must be lists of the same length. Got {type(fields_list)} and {type(datatypes_list)} with lengths {len(fields_list) if type(fields_list) is list else 'N/A'} and {len(datatypes_list) if type(datatypes_list) is list else 'N/A'}")
            exit()

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
        if link_type is None:
            id_fields = self.get_doc_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)
        else:
            id_fields = self.get_doclink_id_fields(convention='doc-link', include_institution=False if index_group=='elasticsearch' else True)

        # Include key creation
        doc_custom_fields = self.get_custom_fields(doc_type=doc_type, link_type=link_type, link_subtype=link_subtype, index_group=index_group)

        # Typecheck
        if not (type(id_fields) is list and type(doc_custom_fields) is list):
            print(f"❌ Error: doc_id_fields and doc_custom_fields must be lists. Got {type(id_fields)} and {type(doc_custom_fields)}")
            exit()

        # Create composite unique key from id fields
        sql_alter_table += f"  ADD UNIQUE KEY IF NOT EXISTS uid ({', '.join(id_fields)})"

        # Make all id fields keys
        for id_field_k in id_fields:
            sql_alter_table += f",\n  ADD KEY IF NOT EXISTS ({id_field_k})"

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
            self.graphsearch_obj_fields   = list(idxcfg.settings['graphsearch'  ]['fields' ]['docs'].get(self.doc_type, []))
            self.elasticsearch_obj_fields = list(idxcfg.settings['elasticsearch']['fields' ]['docs'].get(self.doc_type, []))

    #--------------------------------------#
    # Sub-class definition: DocLink object #
    #--------------------------------------#
    class DocLink():

        # Constructor
        def __init__(self, doc_type, link_type, link_subtype):
            self.doc_type     = doc_type
            self.link_type    = link_type
            self.link_subtype = link_subtype
            self.graphsearch_obj_fields     =  list(idxcfg.settings['graphsearch'  ]['fields']['links']['default'].get(self.link_type, []))
            self.graphsearch_obj2obj_fields = (list(idxcfg.settings['graphsearch'  ]['fields']['links']['parent_child'].get(self.doc_type, {}).get(self.link_type, [])) if link_subtype.upper() == 'ORG' else [])
            self.elasticsearch_obj_fields   =  list(idxcfg.settings['elasticsearch']['fields']['links'].get(self.link_type, []))

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

        #----------------------------------#
        # Pre-generate dynamic SQL queries #
        #----------------------------------#

        # Get list of table fields
        self.table_fields = DynamicSQL().get_fields(doc_type=self.doc_type, index_group=self.index_group)

        # Get create table SQL statement
        self.create_table_sql = DynamicSQL().get_create_table(doc_type=self.doc_type, link_type=self.link_type, link_subtype=self.link_subtype, index_group=self.index_group, include_schema=True)

        # Get drop primary keys and alter table SQL statements
        self.drop_primary_key_sql, self.alter_table_sql = DynamicSQL().get_alter_table(doc_type=self.doc_type, link_type=self.link_type, link_subtype=self.link_subtype, index_group=self.index_group, include_schema=True)

    #----------------------#
    # Basic export methods #
    #----------------------#

    # Method: Get table name (with or without path)
    def get_table_name(self, include_path=False):
        if self.schema_name is not None:
            return f"{self.schema_name+'.' if include_path else ''}{self.table_name}"

    # Method: Get list of table fields
    def get_fields(self):
        return self.table_fields

    # Method: Get SQL create table statement
    def get_create_table(self):
        return self.create_table_sql

    # Method: Get SQL drop primary key statement
    def get_drop_primary_key(self):
        return self.drop_primary_key_sql

    # Method: Get SQL alter table statement
    def get_alter_table(self):
        return self.alter_table_sql

#-------------------------------#
# Command line execution script #
#-------------------------------#
if __name__ == "__main__":


    # list_of_tables = db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name='elasticsearch_cache')
    # for t in list_of_tables:
    #     if '_L_' in t:
    #         print(f"SELECT * FROM elasticsearch_cache.{t} WHERE (doc_id, link_id) NOT IN (SELECT doc_id, link_id FROM graphsearch_test.Index_D_Unit_L_Person_T_ORG);")

    # exit()



    # CHANGE THIS
    which_cache = 'elasticsearch'

    # Mapping
    mapping_for_which_cache = {
        'graphsearch' : ['graphsearch', 'graphsearch_test'],
        'elasticsearch' : ['es_cache', 'elasticsearch_cache']
    }

    # Set schema name for testing
    schema_name = glbcfg.mysql_schema_names['test'][mapping_for_which_cache[which_cache][0]]

    # Initialise table
    for t in sorted(['IndexBuildup_Fields_Links_ParentChild_Course_Lecture', 'IndexBuildup_Fields_Links_ParentChild_Course_Person', 'IndexBuildup_Fields_Links_ParentChild_Lecture_MOOC', 'IndexBuildup_Fields_Links_ParentChild_Lecture_Widget', 'IndexBuildup_Fields_Links_ParentChild_MOOC_Person', 'IndexBuildup_Fields_Links_ParentChild_Notebook_Person', 'IndexBuildup_Fields_Links_ParentChild_Person_Publication', 'IndexBuildup_Fields_Links_ParentChild_Person_Unit', 'IndexBuildup_Fields_Links_ParentChild_Unit_Unit']):
        tb = GraphTable(schema_name='graph_cache', table_name=t)
        print('\n\n')
        print(f"""
            {tb.create_table_sql.replace(';','')} AS

            SELECT {', '.join(tb.table_fields)}
            FROM graph_cache.{t};
        """.replace(' (\n', '_TEMP (\n').replace(', row_id\n', '\n'))
        print('\n\n')



    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Notebook_L_Category')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # # Initialise table
    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Exercise_L_Concept')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Notebook_L_Concept')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # # Initialise table
    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Category_L_Exercise')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Category_L_Notebook')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # # Initialise table
    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Concept_L_Exercise')
    # print('\n\n',tb.create_table_sql,'\n\n')

    # tb = GraphTable(schema_name=schema_name, table_name='Index_D_Concept_L_Notebook')
    # print('\n\n',tb.create_table_sql,'\n\n')




    exit()

    # Get list of tables in schema
    # list_of_tables = db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=schema_name, use_regex=[r"IndexBuildup_Fields_Docs_[^_]*"])
    list_of_tables = db.get_tables_in_schema(engine_name='xaas_coresrv', schema_name=schema_name)

    # Loop over list of tables
    for table_name in list_of_tables:

        # if table_name != 'Index_D_Lecture_L_Lecture_T_SEM':
        #     continue

        # Display status
        # print(f"Processing table: {table_name}")

        if 'PageProfile' in table_name or table_name=='Index_D_Lecture_L_Concept_T_ORG' or table_name=='Index_D_Lecture_L_Concept_T_ORG_Search' or table_name=='Index_D_Lecture_L_Person_T_ORG':
            continue

        # Initialise table
        tb = GraphTable(schema_name=schema_name, table_name=table_name)

        # print(tb.get_drop_primary_key(), '\n' )
        # print(tb.get_alter_table(), '\n\n' )

        missing_fields, fields_to_drop = DynamicSQL().compare_fields(
            doc_type     = tb.doc_type,
            link_type    = tb.link_type,
            link_subtype = tb.link_subtype,
            index_group  = tb.index_group,
            engine_name  = 'xaas_coresrv',
            schema_name  = tb.schema_name
        )

        if not missing_fields and not fields_to_drop:
            continue

        print("\n================================================================")
        print(f"Results for {tb.schema_name}.{table_name}")
        print("================================================================\n")

        if missing_fields:
            print(f"⚠️ Missing fields to add: {missing_fields}")
        else:
            print("✅ No missing fields to add.")

        if fields_to_drop:
            print(f"⚠️ Extra fields to drop: {fields_to_drop}")

            # Generate drop columns SQL query
            sql_query = f"ALTER TABLE {tb.schema_name}.{table_name} {', '.join([f'DROP COLUMN {field}' for field in fields_to_drop])};"
            print(sql_query)
        else:
            print("✅ No extra fields to drop.")