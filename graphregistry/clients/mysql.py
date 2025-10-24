#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.common.auxfcn import print_dataframe
from graphregistry.common.config import GlobalConfig
from sqlalchemy import create_engine as SQLEngine, text, event
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from loguru import logger as sysmsg
import pandas as pd
import os, re, subprocess

# Initialize global config
glbcfg = GlobalConfig()

#-----------------------------------------#
# Class definition for Graph MySQL engine #
#-----------------------------------------#
class GraphDB():

    # Class variable to hold the single instance
    _instance = None

    # Create new instance of class before __init__ is called
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)  # Use `object.__new__()` explicitly
            cls._instance._initialized = False  # Flag for initialization check
        return cls._instance

    # Class constructor
    def __init__(self, name="GraphDB"):
        if not self._initialized:  # Prevent reinitialization
            self.name = name
            self._initialized = True  # Mark as initialized
            print(f"GraphDB initialized with name: {self.name}")

        # Initialize the MySQL engines
        self.params_test, self.engine_test = self.initiate_engine(glbcfg.settings['mysql']['server_test'])
        self.params_prod, self.engine_prod = self.initiate_engine(glbcfg.settings['mysql']['server_prod'])
        self.params = {'test': self.params_test, 'prod': self.params_prod}
        self.engine = {'test': self.engine_test, 'prod': self.engine_prod}

        # Set the MySQL password in an environment variable
        os.environ['MYSQL_TEST_PWD'] = self.params_test['password']
        os.environ['MYSQL_PROD_PWD'] = self.params_prod['password']

        # Build base shell command (MySQL)
        self.base_command_mysql = {
            'test': [glbcfg.settings['mysql']['client_bin'], '-u', self.params_test['username'], f'--password={os.getenv("MYSQL_TEST_PWD")}', '-h', self.params_test['host_address'], '-P', str(self.params_test['port'])],
            'prod': [glbcfg.settings['mysql']['client_bin'], '-u', self.params_prod['username'], f'--password={os.getenv("MYSQL_PROD_PWD")}', '-h', self.params_prod['host_address'], '-P', str(self.params_prod['port'])]
        }

        # Build base shell command (MySQLDump)
        self.base_command_mysqldump = {
            'test': [glbcfg.settings['mysql']['dump_bin'], '-u', self.params_test['username'], f'--password={os.getenv("MYSQL_TEST_PWD")}', '-h', self.params_test['host_address'], '-P', str(self.params_test['port']), '-v', '--no-create-db', '--no-create-info', '--skip-lock-tables', '--single-transaction'],
            'prod': [glbcfg.settings['mysql']['dump_bin'], '-u', self.params_prod['username'], f'--password={os.getenv("MYSQL_PROD_PWD")}', '-h', self.params_prod['host_address'], '-P', str(self.params_prod['port']), '-v', '--no-create-db', '--no-create-info', '--skip-lock-tables', '--single-transaction'],
        }

    #-------------------------------------#
    # Method: Initialize the MySQL engine #
    #-------------------------------------#
    def initiate_engine(self, server_name):
        if server_name not in glbcfg.settings['mysql']:
            raise ValueError(
                f'could not find the configuration for mysql server {server_name} in global config file.'
            )
        params = glbcfg.settings['mysql'][server_name]
        engine = SQLEngine(
            f'mysql+pymysql://{params["username"]}:{params["password"]}@{params["host_address"]}:{params["port"]}/',
            pool_pre_ping=True
        )
        @event.listens_for(engine, "connect")
        def set_sql_mode(dbapi_conn, _):
            with dbapi_conn.cursor() as cur:
                cur.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")

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
    def execute_query(self, engine_name, query, schema_name=None, params=None, commit=False, return_exception=False):
        connection = self.engine[engine_name].connect()
        try:
            if schema_name:
                connection.execute(text(f"USE {schema_name}"))
            result = connection.execute(text(query), parameters=params)
            if result.returns_rows:
                rows = result.fetchall()
            else:
                rows = []
            if commit:
                connection.commit()
        except (DataError, IntegrityError, SQLAlchemyError) as e:
            if return_exception:
                # You can return different levels of detail here
                error_type = type(e).__name__      # e.g. "DataError"
                error_message = str(e)             # human-readable
                # if you want the underlying DBAPI code, it's in e.orig (if available)
                dbapi_code = getattr(e.orig, "args", [None])[0] if hasattr(e, "orig") else None
                return error_type, error_message, dbapi_code
            else:
                print("\033[91mError executing query.\033[0m")
                print(e)
                raise
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
                SELECT t.{', t.'.join(eval_column_names)},
                       COUNT(*) AS n_to_process,
                       SUM({' OR '.join([f"COALESCE(t.{c}, '__null__') != COALESCE(j.{c}, '__null__')" for c in upd_column_names])}) AS n_to_patch
                  FROM (
                        {query}
                       ) t
            INNER JOIN {target_table_path} j
                    ON {' AND '.join([f"t.{c} = j.{c}" for c in key_column_names])}
              GROUP BY t.{', t.'.join(eval_column_names)}
            """

            # Print the evaluation query
            if 'print' in actions:
                print(query_eval)

            # Execute the evaluation query and print the results
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            if len(out) > 0:
                df = pd.DataFrame(out, columns=eval_column_names+['rows to process', 'rows to patch'])
                print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')

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

    #------------------------------------------------------------------------------#
    # Method: Executes/Evaluates a query using ON DUPLICATE KEY UPDATE (in chunks) #
    #------------------------------------------------------------------------------#
    def execute_query_as_safe_inserts_in_chunks(self, engine_name, schema_name, table_name, query, key_column_names, upd_column_names, eval_column_names=None, actions=(), table_to_chunk=None, chunk_size=None, row_id_name=None, show_progress=False):

        # Target table path
        t = target_table_path = f'{schema_name}.{table_name}'

        # Check if chunk_size and row_id_name are provided
        if 'commit' in actions and chunk_size is not None and row_id_name is not None:

            # Strip semicolon from inner query if needed
            base_query = query.strip().rstrip(';')

            # Build base commit query (template, to be filled with chunk conditions)
            def build_chunked_commit_query(chunk_condition):
                return f"""
                    INSERT INTO {target_table_path}
                               ({', '.join(key_column_names)}{', ' if upd_column_names else ''}{', '.join(upd_column_names)})
                         SELECT {', '.join(key_column_names)}{', ' if upd_column_names else ''}{', '.join(upd_column_names)}
                           FROM (
                                {base_query} {chunk_condition}
                                ) AS d
                ON DUPLICATE KEY UPDATE
                    {', '.join([
                        f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})"
                        for c in upd_column_names
                    ])}
                """

            # Get min/max for row_id
            row_id_field = row_id_name.split('.')[-1]  # handle aliases
            row_num_min = self.execute_query(engine_name, f"SELECT MIN({row_id_field}) FROM {table_to_chunk}")[0][0]
            row_num_max = self.execute_query(engine_name, f"SELECT MAX({row_id_field}) FROM {table_to_chunk}")[0][0]

            if row_num_min is None or row_num_max is None:
                print("⚠️ No rows found to process.")
                return

            # Execute each chunk with progress bar
            n_rows = row_num_max - row_num_min + 1
            for offset in tqdm(range(row_num_min, row_num_max + 1, chunk_size), desc='Executing in chunks', unit='chunk', total=(n_rows // chunk_size) + 1) if show_progress else range(row_num_min, row_num_max + 1, chunk_size):
                chunk_condition = f"{'WHERE' if 'WHERE' not in base_query.upper() else 'AND'} {row_id_name} BETWEEN {offset} AND {offset + chunk_size - 1}"
                chunked_query = build_chunked_commit_query(chunk_condition)

                if 'print' in actions:
                    print(chunked_query)

                self.execute_query_in_shell(engine_name=engine_name, query=chunked_query)

            return

        # Evaluate the patch operation
        if 'eval' in actions:
            query_eval = f"""
                       SELECT {', '.join(eval_column_names)}, COUNT(*) AS n_to_process
                         FROM ({query}) t
                     GROUP BY {', '.join(eval_column_names)}
            """
            if 'print' in actions:
                print(query_eval)
            out = self.execute_query(engine_name=engine_name, query=query_eval)
            if len(out) > 0:
                df = pd.DataFrame(out, columns=eval_column_names+['# to process'])
                print_dataframe(df, title=f'\n🔍 Evaluation results for {target_table_path}:')

        # Build the commit query (non-chunked)
        query_commit = f"""
             INSERT INTO {target_table_path}
                         ({', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)})
                  SELECT  {', '.join(key_column_names)}{', ' if len(upd_column_names)>0 else ''}{', '.join(upd_column_names)}
                    FROM (
                         {query}
                         ) AS d
        ON DUPLICATE KEY
                  UPDATE {', '.join([
                         f"{c} = IF(COALESCE({t}.{c}, '__null__') != COALESCE(d.{c}, '__null__'), d.{c}, {t}.{c})"
                         for c in upd_column_names
                         ])};
        """

        if 'print' in actions:
            print(query_commit)

        if 'commit' in actions:
            self.execute_query_in_shell(engine_name=engine_name, query=query_commit)

    #-------------------------------------------------#
    # Method: Executes a query sequentially by chunks #
    #-------------------------------------------------#
    def execute_query_in_chunks(self, engine_name, schema_name, table_name, query, has_filters=None, chunk_size=10000, row_id_name='row_id', show_progress=False, verbose=False):

        # Remove trailing semicolon from the query
        if query.strip()[-1] == ';':
            query = query.strip()[:-1]

        # Which filter command to use?
        if has_filters is None:
            if 'WHERE' in query:
                filter_command = 'AND'
            else:
                filter_command = 'WHERE'
        else:
            filter_command = 'AND' if has_filters else 'WHERE'

        # Row_id name contains alias?
        if '.' in row_id_name:
            row_id_name_no_alias = row_id_name.split('.')[1]
        else:
            row_id_name_no_alias = row_id_name

        # Get min and max row_id
        row_num_min = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MIN({row_id_name_no_alias}), 0) FROM {schema_name}.{table_name}")[0][0] - 1
        row_num_max = self.execute_query(engine_name=engine_name, query=f"SELECT COALESCE(MAX({row_id_name_no_alias}), 0) FROM {schema_name}.{table_name}")[0][0] + 1
        n_rows = row_num_max - row_num_min + 1

        # Process table in chunks
        for offset in tqdm(range(row_num_min, row_num_max, chunk_size), total=round(n_rows/chunk_size)) if show_progress else range(row_num_min, row_num_max, chunk_size):

            # Generate SQL query
            sql_query = f"{query} {filter_command} {row_id_name} BETWEEN {offset} AND {offset + chunk_size - 1};"

            # Execute the query
            self.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose=verbose)

    #---------------------------------------------#
    # Method: Executes a query in the MySQL shell #
    #---------------------------------------------#
    def execute_query_in_shell(self, engine_name, query, verbose=False):

        # Define the shell command
        shell_command = self.base_command_mysql[engine_name] + ['-e', query]

        # If verbose is enabled, print the command being executed
        if verbose:
            print(f"\n\033[96m⚙️ Executing query:\033[0m")
            print(f"\n\t{query.strip().replace('\n','\n\t')}\n")
            

        # Run the command and capture stdout and stderr
        result = subprocess.run(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Initialise return value
        return_value = None

        # Handle stderr
        if result.returncode == 0:
            return_value = True
            if verbose:
                print("\033[92m✅ Query executed successfully.\033[0m\n")

        else:
            return_value = False
            if result.stderr:
                # Ignore the common password warning
                if result.stderr.strip() == ("mysql: [Warning] Using a password on the command line interface can be insecure."):
                    pass
                else:
                    print(f"Error message from MySQL:\n{result.stderr.strip()}\n")
                    sysmsg.critical(f"Failed to execute query.")
                    exit()
            else:
                print("\033[91m‼️ Unknown error occurred.\033[0m\n")
                sysmsg.critical(f"Failed to execute query.")
                exit()

        # Return the result
        return return_value

    #--------------------------------------------------------------#
    # Method: Executes a query in the MySQL shell from an SQL file #
    #--------------------------------------------------------------#
    def execute_query_from_file(self, engine_name, file_path, database=None, verbose=False):
        
        # Start with the base command for the engine
        shell_command = list(self.base_command_mysql[engine_name])

        # Add database selection if provided
        if database:
            shell_command += [database]

        # Add verbosity and the SOURCE command
        if verbose:
            shell_command.append('-v')
        shell_command += ['-e', f"SOURCE {file_path}"]

        # Run the command and capture output
        result = subprocess.run(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check stderr for warnings/errors
        if result.stderr:
            warn = 'mysql: [Warning] Using a password on the command line interface can be insecure.'
            if result.stderr.strip() != warn:
                print(f"Error executing query from file: {file_path}")
                print(result.stderr)
                if 'ERROR' in result.stderr.upper():
                    return False

        return result

    #----------------------------------#
    # Method: Set cell values in table #
    #----------------------------------#
    def set_cells(self, engine_name, schema_name, table_name, set=(), where=(), verbose=False):

        # Check if there are any columns to set
        if len(set) == 0:
            sysmsg.error("No columns to set. Please provide at least one column and value pair.")
            return

        # Generate the SET clause
        set_clause = ', '.join([f"{col} = '{val}'" for col, val in set])

        # Generate the WHERE clause
        if len(where) > 0:
            where_clause = ' AND '.join([f"{col} = '{val}'" for col, val in where])
        else:
            where_clause = "TRUE"

        # Generate the SQL query
        sql_query = f"""
            UPDATE {schema_name}.{table_name}
               SET {set_clause}
             WHERE {where_clause}
        """

        # Execute the query in the MySQL shell
        self.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose=verbose)

    #------------------------------------#
    # Method: Get cell values from table #
    #------------------------------------#
    def get_cells(self, engine_name, schema_name, table_name, select=(), where=(), verbose=False):

        # Generate the WHERE clause
        if len(where) > 0:
            where_clause = ' AND '.join([f"{col} = '{val}'" if col is not None else f"({val})" for col, val in where])
        else:
            where_clause = "TRUE"

        # Generate the SQL query
        sql_query = f"""
            SELECT {', '.join(select) if len(select) > 0 else '*'}
              FROM {schema_name}.{table_name}
             WHERE {where_clause}
        """

        # Execute the query in the MySQL shell
        result = self.execute_query(engine_name=engine_name, query=sql_query) # TODO: add verbose
        if len(result) == 0:
            return []
        
        # Return the result as a list of tuples
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
        self.print_tables_in_schema(engine_name='test', schema_name=glbcfg.settings['mysql']['schema_cache'])
    
    #----------------------------------------------#
    # Method: Print list of tables in the test     #
    #----------------------------------------------#
    def print_tables_in_test(self):
        self.print_tables_in_schema(engine_name='test', schema_name=glbcfg.settings['mysql']['schema_test'])

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

        # Check if row_id column exists in the table
        check_column_query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'row_id'
        """
        has_row_id = self.execute_query(engine_name=engine_name, query=check_column_query)[0][0] > 0

        # If row_id exists, proceed with chunked dump
        if has_row_id:

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

                # Generate output file path
                output_file = f'{folder_path}/{table_name}_{str(offset).zfill(10)}.sql'

                # Check if the output file already exists
                if os.path.exists(output_file):
                    continue

                # Generate shell command to dump table chunck using mysqldump executable
                shell_command = self.base_command_mysqldump[engine_name] + [schema_name, table_name, f'--where="{filter_by} AND (row_id BETWEEN {offset} AND {offset + chunk_size - 1})"'] + ['--result-file=' + output_file]

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

        # Else, if row_id does not exist, dump the entire table at once
        else:

            # Generate output file path
            output_file = f'{folder_path}/{table_name}_FULL.sql'

            # Check if the output file already exists
            if os.path.exists(output_file):
                sysmsg.warning(f"Output file {output_file} already exists. Skipping dump for table {table_name}.")
                return

            # Fallback: dump entire table with optional filter
            shell_command = self.base_command_mysqldump[engine_name] + [
                schema_name,
                table_name,
                f'--where="{filter_by}"',
                f'--result-file={output_file}'
            ]

            # Generate shell text command
            shell_text_command = ' '.join(shell_command)

            # Run the command and capture stdout and stderr
            result = subprocess.run(shell_text_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

            # Check if there's a MySQL-specific warning
            if result.stderr and 'Using a password on the command line interface can be insecure' not in result.stderr:
                print('Error dumping table (full):', table_name)
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

    #---------------------------------#
    # Method: Dump database to folder #
    #---------------------------------#
    def dump_database_to_folder(self, engine_name, schema_name, folder_path, filter_by='TRUE', chunk_size=100000, include_create_tables=True, include_views=True):

        # Display status
        sysmsg.info(f"📝 Dump database '{schema_name}' from '{engine_name}' to: {folder_path}")

        # Check if the database exists
        if not self.database_exists(engine_name=engine_name, schema_name=schema_name):
            sysmsg.error(f"Database '{schema_name}' does not exist in '{engine_name}'.\n")
            return

        # Get list of tables in the schema
        list_of_tables = self.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name, include_views=include_views)
        print(list_of_tables)

        # Loop over the tables
        for table_name in list_of_tables:

            # Display status
            sysmsg.trace(f"⚙️ Exporting table: {table_name} ...")

            # Create export folder with database schema name and table name (if it doesn't exist)
            export_path = f"{folder_path}/{schema_name}/{table_name}"
            if not os.path.exists(export_path):
                os.makedirs(export_path)

            # If include_create_tables, dump the create table statement
            if include_create_tables:

                # Get the create table SQL
                create_table_sql = self.get_create_table(engine_name=engine_name, schema_name=schema_name, table_name=table_name)

                # Write the create table SQL to a file
                with open(f"{folder_path}/{schema_name}/{table_name}/create_table.sql", 'w') as fid:
                    fid.write(create_table_sql)

            # Dump the table to the folder
            self.dump_table_to_folder(
                engine_name = engine_name,
                schema_name = schema_name,
                table_name  = table_name,
                folder_path = export_path,
                chunk_size  = chunk_size,
                filter_by   = filter_by
            )
        
        # Display status
        sysmsg.success(f"✅ Done exporting databases from '{schema_name}'.\n")
    
    #-------------------------------------#
    # Method: Import database from folder #
    #-------------------------------------#
    def import_database_from_folder(self, engine_name, folder_path, schema_name=None, create_if_not_exists=False, replace_existing=False, include_views=True):

        # Is schema name provided? If not, extract from folder path
        if schema_name is None:
            # Get schema name from the folder path
            # -> the folder structure is assumed to be: schema_path = root_path/schema_name/
            #    and the full path is root_path/schema_name/table_name/*.sql
            schema_name = os.path.basename(os.path.normpath(folder_path))

        # Display status
        sysmsg.info(f"📝 Import database '{schema_name}' into '{engine_name}'.")

        # Check if the database exists (create if not)
        if not self.database_exists(engine_name=engine_name, schema_name=schema_name):

            # Display warning
            sysmsg.warning(f"Database '{schema_name}' does not exist in '{engine_name}'. The flag 'create_if_not_exists' is set to {str(create_if_not_exists).upper()}.")

            # Create database if it does not exist and create_if_not_exists is True
            if not create_if_not_exists:
                sysmsg.error(f"❌ Failed to import database.")
                return
            else:
                sysmsg.trace(f"Creating database '{schema_name}' ...")
                self.create_database(engine_name=engine_name, schema_name=schema_name)

        # If the database exists and replace_existing is True, drop and recreate it
        elif replace_existing:
            
            # Display warning
            sysmsg.warning(f"Database '{schema_name}' already exists. The flag 'replace_existing' is set to TRUE.")
            
            # Ask for confirmation (write yes/no)
            confirmation = input(f"Are you sure you want to replace the existing database? (yes/no): ")
            if confirmation.lower() != 'yes':
                sysmsg.error("❌ Operation cancelled by user.")
                return

            # Drop and recreate the database
            self.drop_database(  engine_name=engine_name, schema_name=schema_name)
            self.create_database(engine_name=engine_name, schema_name=schema_name)

        # Else, if the database exists and replace_existing is False, do nothing
        else:
            sysmsg.warning(f"Database '{schema_name}' already exists. The flag 'replace_existing' is set to FALSE.")
            sysmsg.error("❌ Failed to import database.")
            return
        
        # Get list of tables in the schema folder
        list_of_tables = [d for d in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, d))]

        # Separate the views from the tables
        list_of_views = [d for d in list_of_tables if os.path.exists(os.path.join(folder_path, d, 'create_view.sql'))]
        list_of_tables = [d for d in list_of_tables if d not in list_of_views]

        # Loop over the tables
        for table_name in list_of_tables:

            # Display status
            sysmsg.trace(f"⚙️ Importing table: {table_name} ...")

            #----------------------------#
            # Apply CREATE TABLE queries #
            #----------------------------#

            # Create the table in the database
            create_table_file = f"{folder_path}/{table_name}/create_table.sql"

            # Check if the create table file exists
            if os.path.exists(create_table_file):

                # Read the SQL statement from the file
                with open(create_table_file, 'r') as f:
                    create_table_sql = f.read()

                # Execute the SQL statement to create the table
                self.execute_query(engine_name=engine_name, query=create_table_sql, schema_name=schema_name) 

            else:
                sysmsg.warning(f"Create table file '{create_table_file}' does not exist. Skipping table import.")

            #----------------------------#
            # Import data from SQL files #
            #----------------------------#
            
            # Import the table from the folder
            self.import_table_from_folder(
                engine_name = engine_name,
                schema_name = schema_name,
                folder_path = f"{folder_path}/{table_name}"
            )

            #---------------------------#
            # Apply CREATE VIEW queries #
            #---------------------------#

        # Include views?
        if include_views:
                
            # Loop over the views
            for view_name in list_of_views:

                # Create the view in the database
                create_view_file = f"{folder_path}/{view_name}/create_view.sql"

                # Check if the create view file exists
                if os.path.exists(create_view_file):

                    # Read the SQL statement from the file
                    with open(create_view_file, 'r') as f:
                        create_view_sql = f.read()

                    # Execute the SQL statement to create the view
                    self.execute_query(engine_name=engine_name, query=create_view_sql, schema_name=schema_name)

        # If include_views, import the views
        if include_views:
            pass

        # Display status
        sysmsg.success(f"✅ Done importing database into '{schema_name}'.\n")

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
        config_mysql_export_path = glbcfg.settings['mysql']['data_path']['export']
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

        # Get list of tables in graphsearch test
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
        if not self.table_exists(engine_name=source_engine_name, schema_name=source_schema_name, table_name=target_table_name):
            sysmsg.error(f"🚨 Table {source_schema_name}.{target_table_name} does not exist in '{source_engine_name}'.")
            return
        
        # Check if the target table exists
        if not self.table_exists(engine_name=target_engine_name, schema_name=target_schema_name, table_name=source_table_name):
            sysmsg.error(f"🚨 Table {target_schema_name}.{source_table_name} does not exist in '{target_engine_name}'.")
            return

        # Detect table type
        table_type = get_table_type_from_name(source_table_name)
        if table_type == 'doc_profile':
            pass

        #------------------------------------------#
        # Generate the SQL query for sample tuples #
        #------------------------------------------#
      
        # Get random primary key set
        random_primary_key_set  = self.get_random_primary_key_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, sample_size=round(sample_size/2), partition_by='object_type', use_row_id=True)
        random_primary_key_set += self.get_random_primary_key_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, sample_size=round(sample_size/2), partition_by='object_type', use_row_id=True)

        # Get the rows by primary key set (source and target)
        source_row_set_dict = self.get_rows_by_primary_key_set(engine_name=source_engine_name, schema_name=source_schema_name, table_name=source_table_name, primary_key_set=random_primary_key_set, return_as_dict=True)
        target_row_set_dict = self.get_rows_by_primary_key_set(engine_name=target_engine_name, schema_name=target_schema_name, table_name=target_table_name, primary_key_set=random_primary_key_set, return_as_dict=True)

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

    #----------------------------#
    # Method: Get database stats #
    #----------------------------#
    def print_database_stats(self, engine_name, schema_name, re_include=[], re_exclude=[]):

        # Get list of tables in the schema
        list_of_tables = self.get_tables_in_schema(engine_name=engine_name, schema_name=schema_name)

        # Apply include/exclude filters
        if len(re_include) > 0:
            list_of_tables = [t for t in list_of_tables if     any(re.search(pattern, t) for pattern in re_include)]
        if len(re_exclude) > 0:
            list_of_tables = [t for t in list_of_tables if not any(re.search(pattern, t) for pattern in re_exclude)]

        # Print line break
        print('')

        # Loop over the tables
        for table_name in list_of_tables:

            # Get the row count
            row_count = self.execute_query(engine_name=engine_name, query=f"SELECT COUNT(*) FROM {schema_name}.{table_name};")[0][0]
            
            # Print table : row count (in red if =0 else in blue)
            if row_count > 0:
                print(f"\033[34m{table_name}: {row_count}\033[0m")
            else:
                print(f"\033[31m{table_name}: {row_count}\033[0m")

        # Print line break
        print('')

#================#
# Main execution #
#================#
if __name__ == "__main__":
    db = GraphDB()
    print(db.test())