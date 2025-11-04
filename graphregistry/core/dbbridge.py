#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.common.auxfcn import print_colour
from graphregistry.clients.mysql import GraphDB
from graphregistry.common.config import GlobalConfig
from sqlalchemy import text
from sqlalchemy.dialects import mysql
from loguru import logger as sysmsg
from typing import List, Tuple

# Initialize global config
glbcfg = GlobalConfig()

# Initialise MySQL client
db = GraphDB()

# Auxiliary function to get schema name from a (from,to) tuple
def get_schema(from_object_type, to_object_type):
    schema_from = glbcfg.object_type_to_schema.get(from_object_type, glbcfg.schema_registry)
    schema_to = glbcfg.object_type_to_schema.get(to_object_type, glbcfg.schema_registry)
    if schema_from == glbcfg.schema_lectures or schema_to == glbcfg.schema_lectures:
        return glbcfg.schema_lectures
    elif schema_from == schema_to:
        return schema_from
    else:
        return glbcfg.schema_registry

#-----------------------------------------------#
# Class definition for Registry database bridge #
#-----------------------------------------------#
class RegistryDB():

    # Function to execute an INSERT operation in the registry
    def registry_insert(self,
            schema_name, table_name, key_column_names, key_column_values, upd_column_names, upd_column_values, actions=(),
            engine_name='test'
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
            out = db.execute_query(engine_name=engine_name, query=sql_query_eval, params=sql_params)

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
                INSERT INTO {t}
                    ({', '.join(key_column_names)})
                SELECT
                    {', '.join([f':{key_column_names[k]} AS {key_column_names[k]}' for k in range(num_key_columns)])};"""

        # Print the SQL query
        if 'print' in actions:
            stmt = text(sql_query_commit).bindparams(**sql_params)
            print(stmt.compile(
                dialect=mysql.dialect(),
                compile_kwargs={"literal_binds": True}
            ))

        # Execute commit
        if 'commit' in actions:
            out = db.execute_query(engine_name='test', query=sql_query_commit, params=sql_params, commit=True, return_exception=True)
            if not type(out) is list:
                error_type, error_msg, dbapi_code = out
                if dbapi_code==1062: # Duplicate entry
                    sysmsg.warning(f'Duplicate entry error when inserting into {t} with keys {sql_params}. Continuing ...')
                else:
                    sysmsg.critical(f'Error when inserting into {t} with keys {sql_params}. Exiting ...')
                    print('Error details:')
                    print(f'{error_type}: {error_msg} (DBAPI code: {dbapi_code})')
                    exit()

        # Return the test results
        return eval_results

    # Function to delete input list of concepts
    def delete_concepts_for_nodes(self, table, institution_id, object_type, nodes_id: List[str], engine_name='test', actions=()):
        schema_objects = glbcfg.object_type_to_schema.get(object_type, 'graph_registry')
        query_where = f'institution_id="{institution_id}" AND object_type="{object_type}" AND object_id IN :object_id'
        eval_results = None
        if 'eval' in actions:
            query_eval = f'SELECT COUNT(*) FROM {schema_objects}.{table} WHERE {query_where};'
            if 'print' in actions:
                print(query_eval)
            out = db.execute_query(query=query_eval, params={'object_id': nodes_id}, engine_name=engine_name)
            eval_results = {'delete concept': out[0][0]}
            if eval_results['delete concept'] > 0:
                print(eval_results)
        queries_remove = f'DELETE FROM {schema_objects}.{table} WHERE {query_where};'
        if 'print' in actions:
            print(queries_remove)
        if 'commit' in actions:
            db.execute_query(
                query=queries_remove, params={'object_id': nodes_id}, engine_name=engine_name, commit=True
            )
        return eval_results

    # Function to delete input list of nodes by id
    def delete_nodes_by_ids(self, institution_id, object_type, nodes_id: List[str], engine_name='test', actions=()):
        schema_objects = glbcfg.object_type_to_schema.get(object_type, glbcfg.schema_registry)
        query_where_per_table = {}
        for table in (
                'Nodes_N_Object', 'Data_N_Object_T_PageProfile', 'Data_N_Object_T_CustomFields',
        ):
            query_where_per_table[f'{schema_objects}.{table}'] = \
                f'institution_id="{institution_id}" AND object_type="{object_type}" AND object_id IN :object_id'

        for schema in (glbcfg.schema_registry, glbcfg.schema_lectures):
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
        eval_results = {}
        queries_remove = []
        for table, query_where in query_where_per_table.items():
            if 'eval' in actions:
                query_eval = f'SELECT COUNT(*) FROM {table} WHERE {query_where};'
                if 'print' in actions:
                    print(query_eval)
                out = db.execute_query(query=query_eval, params={'object_id': nodes_id}, engine_name=engine_name)
                eval_results[table] = out[0][0]
                if eval_results[table] > 0:
                    print(table, eval_results[table])
            queries_remove.append(f'DELETE FROM {table} WHERE {query_where};\n')
        if len(queries_remove) > 1:
            queries_remove.insert(0, 'BEGIN;')
            queries_remove.append('COMMIT;')
        if 'print' in actions:
            for q in queries_remove:
                print(q)
        if 'commit' in actions:
            for q in queries_remove:
                db.execute_query(query=q, params={'object_id': nodes_id}, engine_name=engine_name, commit=True)
        if db.table_exists(engine_name, schema_objects, 'Edges_N_Object_N_Concept_T_ConceptDetection'):
            eval_results['Edges_N_Object_N_Concept_T_ConceptDetection'] = self.delete_concepts_for_nodes(
                'Edges_N_Object_N_Concept_T_ConceptDetection', institution_id, object_type, nodes_id,
                engine_name=engine_name, actions=actions
            )
        if db.table_exists(engine_name, schema_objects, 'Edges_N_Object_N_Concept_T_ManualMapping'):
            eval_results['Edges_N_Object_N_Concept_T_ManualMapping'] = self.delete_concepts_for_nodes(
                'Edges_N_Object_N_Concept_T_ManualMapping', institution_id, object_type, nodes_id,
                engine_name=engine_name, actions=actions
            )
        return eval_results if 'eval' in actions else None

    # Function to delete input list of edges by id
    def delete_edges_by_ids(self, from_institution_id, from_object_type, to_institution_id, to_object_type,
            edges_id: List[Tuple[str, str]], engine_name='test', actions=()
    ):
        schema_edges = get_schema(from_object_type, to_object_type)
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
                out = db.execute_query(query=query_eval, params={'edges_id': edges_id}, engine_name=engine_name)
                eval_results[table] = out[0][0]
                if eval_results[table] > 0:
                    print(table, eval_results[table])
            query_remove += f'DELETE FROM {table} WHERE {query_where};\n'
        if 'print' in actions:
            print(query_remove)
        if 'commit' in actions:
            db.execute_query(
                query=query_remove, params={'edges_id': edges_id}, engine_name=engine_name, commit=True
            )
        return eval_results

    # Get the list of object_id from the existing nodes in the database
    def get_existing_nodes_id(self, institution_id: str, object_type: str, engine_name='test'):
        schema_name = glbcfg.object_type_to_schema.get(object_type, glbcfg.schema_registry)
        existing_nodes_id = db.execute_query(
            engine_name=engine_name,
            query=f"""
                SELECT object_id
                FROM {schema_name}.Nodes_N_Object
                WHERE institution_id='{institution_id}' AND object_type='{object_type}';"""
        )
        return [object_id for object_id, in existing_nodes_id]

    # Get the list of ids from the existing edges in the database
    def get_existing_edges_id(self, from_institution_id: str, from_object_type: str, to_institution_id: str, to_object_type: str, engine_name='test'):
        schema_name = get_schema(from_object_type, to_object_type)
        existing_edges_id = db.execute_query(
            engine_name=engine_name,
            query=f"""
                SELECT from_object_id, to_object_id
                FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent
                WHERE
                    from_institution_id='{from_institution_id}' AND from_object_type='{from_object_type}'
                    AND to_institution_id='{to_institution_id}' AND to_object_type='{to_object_type}';"""
        )
        return existing_edges_id
