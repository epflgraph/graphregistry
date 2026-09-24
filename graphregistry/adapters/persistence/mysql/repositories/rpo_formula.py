# graphregistry/adapters/persistence/mysql/repositories/rpo_formula.py
from __future__ import annotations
from pathlib import Path
import glob
import re
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_formula import FormulaRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import REPO_ROOT
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Root folder of the SQL formula templates and the short folder aliases.
SQL_FORMULAS_PATH = REPO_ROOT / 'database' / 'formulas'
SQL_FORMULAS_FOLDER_ALIASES = {
    'fields'    : 'calculated_fields',
    'traversals': 'graph_traversals',
    'scores'    : 'calculated_scores',
}

# The formula families applied by the named commands.
CALCULATED_FIELD_FOLDERS = ['calculated_fields/obj', 'calculated_fields/obj2obj']
TRAVERSAL_FOLDERS = ['graph_traversals']
SCORING_FOLDERS = [
    'calculated_scores/obj2ontology/concepts',
    'calculated_scores/obj2ontology/concepts_union',
    'calculated_scores/obj2ontology/categories',
    'calculated_scores/obj2ontology/categories_union',
    'calculated_scores/degree_scores',
]

# The materialised views of the cache, in the legacy execution order.
MATERIALIZED_VIEWS = [
    'obj2obj: all fields symmetric',
    'obj: page profile',
    'obj: all fields',
    'obj2obj: parent-child symmetric',
]

#==================#
# Class Definition #
#==================#
class MySQLFormulaRepository(FormulaRepository):
    """MySQL adapter for the FormulaRepository port.

    The template resolution, execution-type dispatch, and the four
    materialised views are extracted verbatim from the legacy
    GraphRegistry.CacheManagement class. The safe-insert targets of the
    calculated-field templates and the schema placeholder substitution
    follow the legacy exactly.
    """

    # Public Method: Initialize the repository with an injected database
    # client, schema resolver, and global configuration.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, global_config: GlobalConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.global_config = global_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Template resolution                              #
    #================================================================#

    # Internal Method: Read a formula template and substitute the [[schema]]
    # placeholders with the configured coresrv schema names.
    def _resolve_formula_sql(self, file_path: Path) -> str:
        with open(file_path, 'r') as file:
            sql_formula = file.read()
        for schema_key, schema_name in self.global_config.mysql_schema_names['coresrv'].items():
            sql_formula = sql_formula.replace(f'[[{schema_key}]]', schema_name)
        return sql_formula

    # Internal Method: Determine the soft-delete column of a table, which
    # differs between the registry schemas (record_deleted) and the cache
    # schemas (deleted).
    def _soft_delete_column(self, schema_name: str, table_name: str) -> str | None:
        for column_name in ('record_deleted', 'deleted'):
            if self.db.column_exists(engine_name='coresrv', schema_name=schema_name, table_name=table_name, column_name=column_name):
                return column_name
        return None

    #================================================================#
    # Method Group: Formula execution                                #
    #================================================================#

    # Public Method: Apply every formula of a folder relative to
    # database/formulas.
    def apply_formulas_from_folder(self, local_path: str, actions: ActionSet = ('commit',)) -> list[str]:
        sysmsg.info(f"🧪 📝 Apply formulas of type '{local_path}'.")
        applied = []
        for file_path in sorted(glob.glob(f'{SQL_FORMULAS_PATH}/{local_path}/formula.*.sql')):
            self._apply_formula_from_file(file_path=Path(file_path), actions=actions)
            applied.append(re.findall(r'formula\.(.*)\.sql$', file_path)[0])
        sysmsg.success("🧪 ✅ Done applying formulas.")
        return applied

    # Public Method: Apply one formula by its path relative to
    # database/formulas, resolving the folder aliases.
    def apply_formula_by_path(self, formula_path: str, actions: ActionSet = ('eval',)) -> None:

        # Allow omitting the .sql extension.
        if not formula_path.endswith('.sql'):
            formula_path = f"{formula_path}.sql"

        # Resolve folder aliases (e.g. traversals -> graph_traversals).
        parts = formula_path.split('/')
        if parts and parts[0] in SQL_FORMULAS_FOLDER_ALIASES:
            parts[0] = SQL_FORMULAS_FOLDER_ALIASES[parts[0]]
        formula_path = '/'.join(parts)

        # Resolve the full path and apply the formula when it exists.
        full_path = SQL_FORMULAS_PATH / formula_path
        if not full_path.is_file():
            sysmsg.error(f"Formula file not found: {full_path}")
            return
        self._apply_formula_from_file(file_path=full_path, actions=actions)

    # Internal Method: Apply one formula file, dispatching between
    # safe-insert upserts and direct execution by the template's verbs.
    def _apply_formula_from_file(self, file_path: Path, actions: ActionSet) -> None:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Extract the formula name and type from the file path.
        formula_name = re.findall(r'formula\.(.*)\.sql$', str(file_path))[0]
        formula_type = re.findall(r'(.*)/formula\..*\.sql$', str(file_path).replace(f'{SQL_FORMULAS_PATH}/', ''))[0]
        sysmsg.trace(f"⚙️  Applying formula: '{formula_name}' ...")

        # Resolve the template variables.
        sql_formula = self._resolve_formula_sql(file_path)

        # Determine the execution type from the template's verbs.
        if any(verb in sql_formula for verb in ('INSERT', 'REPLACE', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
            execution_type = 'direct execution'
        elif 'SELECT' in sql_formula:
            execution_type = 'safe inserts'
        else:
            sysmsg.warning("Could not determine type of formula (safe inserts vs direct execution).")
            return

        # SELECT templates upsert their rows into the calculated-field
        # tables, keyed per formula family.
        if execution_type == 'safe inserts':
            if formula_type == 'calculated_fields/obj':
                target_table = 'Data_N_Object_T_CalculatedFields'
                key_column_names = ['object_type', 'object_id']
                eval_column_names = ['object_type']
            elif formula_type == 'calculated_fields/obj2obj':
                target_table = 'Data_N_Object_N_Object_T_CalculatedFields'
                key_column_names = ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id', 'context']
                eval_column_names = ['from_object_type', 'to_object_type']
            else:
                sysmsg.warning(f"Unknown safe-insert target for formula type '{formula_type}'.")
                return
            self.db.execute_query_as_safe_inserts(
                engine_name       = engine_name,
                schema_name       = cache_schema,
                table_name        = target_table,
                query             = sql_formula,
                key_column_names  = key_column_names,
                upd_column_names  = ['field_language', 'field_name', 'field_value'],
                eval_column_names = eval_column_names,
                actions           = actions,
                verbose           = 'print' in actions,
                query_id          = 'D9NxAGY2',
            )

        # Write templates execute directly in the shell.
        elif execution_type == 'direct execution':
            if 'print' in actions:
                print_sql(sql_formula, title='Neg00cQJ')
            if 'commit' in actions:
                self.db.execute_query_in_shell(engine_name=engine_name, query=sql_formula, verbose=False, query_id='Neg00cQJ')

    # Internal Method: Apply one formula file in row_id chunks, with DDL and
    # non-UPDATE statements falling back to direct shell execution inside
    # the statement splitter.
    def _apply_formula_in_chunks(self, file_path: Path, chunk_size: int, actions: ActionSet, show_progress: bool) -> None:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        formula_name = re.findall(r'formula\.(.*)\.sql$', str(file_path))[0]
        sysmsg.trace(f"⚙️  Applying formula in chunks: '{formula_name}' ...")
        sql_formula = self._resolve_formula_sql(file_path)
        if 'print' in actions:
            print_sql(sql_formula, title=f"Formula in chunks [{formula_name}]")
        if 'commit' in actions:
            self.db.execute_sql_statements_in_chunks(
                engine_name   = engine_name,
                sql           = sql_formula,
                chunk_size    = chunk_size,
                show_progress = show_progress,
                verbose       = self.verbose,
                query_id      = formula_name,
            )

    #================================================================#
    # Method Group: Named formula families                           #
    #================================================================#

    # Public Method: Apply the data reset formulas in row_id chunks.
    def apply_data_reset_formulas(self, actions: ActionSet = ('commit',)) -> None:
        sysmsg.info("🧪 📝 Apply data reset formulas in chunks.")
        for file_path in sorted(glob.glob(f'{SQL_FORMULAS_PATH}/data_reset/formula.*.sql')):
            self._apply_formula_in_chunks(file_path=Path(file_path), chunk_size=100000, actions=actions, show_progress=True)
        sysmsg.success("🧪 ✅ Done applying data reset formulas in chunks.")

    # Public Method: Apply the calculated-field formulas.
    def apply_calculated_field_formulas(self, actions: ActionSet = ('commit',)) -> None:
        for local_path in CALCULATED_FIELD_FOLDERS:
            self.apply_formulas_from_folder(local_path=local_path, actions=actions)

    # Public Method: Apply the graph traversal formulas.
    def apply_traversals(self, actions: ActionSet = ('commit',)) -> None:
        for local_path in TRAVERSAL_FOLDERS:
            self.apply_formulas_from_folder(local_path=local_path, actions=actions)

    # Public Method: Apply the scoring formulas.
    def apply_scoring_formulas(self, actions: ActionSet = ('commit',)) -> None:
        for local_path in SCORING_FOLDERS:
            self.apply_formulas_from_folder(local_path=local_path, actions=actions)

    #================================================================#
    # Method Group: Materialised views                               #
    #================================================================#

    # Public Method: Materialize the cache views in the legacy order.
    def materialize_views(self, actions: ActionSet = ('commit',)) -> None:
        sysmsg.info(f"👀 📝 Materialize views and commit updated data to the graph cache [actions: {actions}].")
        if len(actions) == 0:
            sysmsg.warning("No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
            sysmsg.info("🚀 📝 Nothing to do.")
            return
        if 'eval' in actions and 'commit' not in actions:
            sysmsg.warning("Executing in evaluation mode only.")
        for view_name in MATERIALIZED_VIEWS:
            self._cache_update_from_view(view_name, actions=actions)
        sysmsg.success("👀 ✅ Done materializing views.")

    # Internal Method: Materialize one view: build its UNION ALL query over
    # the registry schemas and replace the target cache table rows.
    def _cache_update_from_view(self, view_name: str, actions: ActionSet) -> None:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # The registry schemas whose soft-delete columns gate every branch.
        registry_schemas = [self.global_config.schema_registry, self.global_config.schema_lectures, self.global_config.schema_ontology]

        #--------------------------------#
        # Build the view query per name #
        #--------------------------------#
        # The symmetric all-fields view unions both directions of every
        # flagged custom and calculated field pair.
        if view_name == 'obj2obj: all fields symmetric':
            target_table = 'Data_N_Object_N_Object_T_AllFieldsSymmetric'
            eval_columns = ['from_object_type', 'to_object_type', 'field_name']
            template = """
                    SELECT cf.from_object_type,
                           cf.from_object_id,
                           cf.to_object_type,
                           cf.to_object_id,
                           cf.context,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM {airflow}.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN {schema}.Data_N_Object_N_Object_T_{source} cf
                     USING (from_object_type, from_object_id, to_object_type, to_object_id)
                INNER JOIN {airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_object_type, to_object_type)
                      WHERE tp.to_process = 1
                        AND tp.deleted = 0
                        AND tf.to_process = 1
                        AND cf.{deleted_col} = 0
                        AND cf.from_object_type NOT IN ('Slide')
                        AND   cf.to_object_type NOT IN ('Slide')

                 UNION ALL

                    SELECT cf.to_object_type      AS from_object_type,
                           cf.to_object_id        AS from_object_id,
                           cf.from_object_type    AS to_object_type,
                           cf.from_object_id      AS to_object_id,
                           cf.context             AS context,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM {airflow}.Operations_N_Object_N_Object_T_FieldsChanged tp
                INNER JOIN {schema}.Data_N_Object_N_Object_T_{source} cf
                     USING (from_object_type, from_object_id, to_object_type, to_object_id)
                INNER JOIN {airflow}.Operations_N_Object_N_Object_T_TypeFlags tf
                     USING (from_object_type, to_object_type)
                      WHERE tp.to_process = 1
                        AND tp.deleted = 0
                        AND tf.to_process = 1
                        AND cf.{deleted_col} = 0
                        AND cf.from_object_type NOT IN ('Slide')
                        AND   cf.to_object_type NOT IN ('Slide')
                """
            sql_query_stack = []
            for schema_name in registry_schemas:
                deleted_col = self._soft_delete_column(schema_name, 'Data_N_Object_N_Object_T_CustomFields')
                if deleted_col is None:
                    continue
                sql_query_stack.append(template.format(airflow=airflow_schema, schema=schema_name, source='CustomFields', deleted_col=deleted_col))
            deleted_col = self._soft_delete_column(cache_schema, 'Data_N_Object_N_Object_T_CalculatedFields')
            if deleted_col is not None:
                sql_query_stack.append(template.format(airflow=airflow_schema, schema=cache_schema, source='CalculatedFields', deleted_col=deleted_col))
            sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

        # The page-profile view carries the full presentation column list.
        elif view_name == 'obj: page profile':
            target_table = 'Data_N_Object_T_PageProfile'
            eval_columns = ['object_type']
            profile_columns = (
                'object_type, object_id, numeric_id_en, numeric_id_fr, numeric_id_de, numeric_id_it, short_code, '
                'subtype_en, subtype_fr, subtype_de, subtype_it, '
                'name_en_is_auto_generated, name_en_is_auto_corrected, name_en_is_auto_translated, name_en_translated_from, name_en_value, '
                'name_fr_is_auto_generated, name_fr_is_auto_corrected, name_fr_is_auto_translated, name_fr_translated_from, name_fr_value, '
                'name_de_is_auto_generated, name_de_is_auto_corrected, name_de_is_auto_translated, name_de_translated_from, name_de_value, '
                'name_it_is_auto_generated, name_it_is_auto_corrected, name_it_is_auto_translated, name_it_translated_from, name_it_value, '
                'description_short_en_is_auto_generated, description_short_en_is_auto_corrected, description_short_en_is_auto_translated, description_short_en_translated_from, description_short_en_value, '
                'description_short_fr_is_auto_generated, description_short_fr_is_auto_corrected, description_short_fr_is_auto_translated, description_short_fr_translated_from, description_short_fr_value, '
                'description_short_de_is_auto_generated, description_short_de_is_auto_corrected, description_short_de_is_auto_translated, description_short_de_translated_from, description_short_de_value, '
                'description_short_it_is_auto_generated, description_short_it_is_auto_corrected, description_short_it_is_auto_translated, description_short_it_translated_from, description_short_it_value, '
                'description_medium_en_is_auto_generated, description_medium_en_is_auto_corrected, description_medium_en_is_auto_translated, description_medium_en_translated_from, description_medium_en_value, '
                'description_medium_fr_is_auto_generated, description_medium_fr_is_auto_corrected, description_medium_fr_is_auto_translated, description_medium_fr_translated_from, description_medium_fr_value, '
                'description_medium_de_is_auto_generated, description_medium_de_is_auto_corrected, description_medium_de_is_auto_translated, description_medium_de_translated_from, description_medium_de_value, '
                'description_medium_it_is_auto_generated, description_medium_it_is_auto_corrected, description_medium_it_is_auto_translated, description_medium_it_translated_from, description_medium_it_value, '
                'description_long_en_is_auto_generated, description_long_en_is_auto_corrected, description_long_en_is_auto_translated, description_long_en_translated_from, description_long_en_value, '
                'description_long_fr_is_auto_generated, description_long_fr_is_auto_corrected, description_long_fr_is_auto_translated, description_long_fr_translated_from, description_long_fr_value, '
                'description_long_de_is_auto_generated, description_long_de_is_auto_corrected, description_long_de_is_auto_translated, description_long_de_translated_from, description_long_de_value, '
                'description_long_it_is_auto_generated, description_long_it_is_auto_corrected, description_long_it_is_auto_translated, description_long_it_translated_from, description_long_it_value, '
                'external_key_en, external_key_fr, external_key_de, external_key_it, '
                'external_url_en, external_url_fr, external_url_de, external_url_it, is_visible'
            )
            sql_query_stack = []
            for schema_name in registry_schemas:
                deleted_col = self._soft_delete_column(schema_name, 'Data_N_Object_T_PageProfile')
                if deleted_col is None:
                    continue
                profile_select = ", ".join(f"pp.{column.strip()}" for column in profile_columns.split(","))
                sql_query_stack.append(f"""
                        SELECT {profile_select},
                               1 AS to_process, 0 AS deleted
                          FROM {airflow_schema}.Operations_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Data_N_Object_T_PageProfile pp
                         USING (object_type, object_id)
                    INNER JOIN {airflow_schema}.Operations_N_Object_T_TypeFlags tf
                         USING (object_type)
                          WHERE tp.to_process = 1
                            AND tp.deleted = 0
                            AND tf.flag_type = 'fields'
                            AND tf.to_process = 1
                            AND pp.{deleted_col} = 0
                      """)
            sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

        # The all-fields view carries the custom and calculated fields of
        # the flagged objects.
        elif view_name == 'obj: all fields':
            target_table = 'Data_N_Object_T_AllFields'
            eval_columns = ['object_type', 'field_name']
            template = """
                    SELECT cf.object_type, cf.object_id,
                           cf.field_language, cf.field_name, cf.field_value,
                           1 AS to_process, 0 AS deleted
                      FROM {airflow}.Operations_N_Object_T_FieldsChanged tp
                INNER JOIN {schema}.Data_N_Object_T_{source} cf
                     USING (object_type, object_id)
                INNER JOIN {airflow}.Operations_N_Object_T_TypeFlags tf
                     USING (object_type)
                      WHERE tp.to_process = 1
                        AND tp.deleted = 0
                        AND tf.flag_type = 'fields'
                        AND tf.to_process = 1
                        AND cf.{deleted_col} = 0
                        AND cf.object_type NOT IN ('Slide')
                """
            sql_query_stack = []
            for schema_name in registry_schemas:
                deleted_col = self._soft_delete_column(schema_name, 'Data_N_Object_T_CustomFields')
                if deleted_col is None:
                    continue
                sql_query_stack.append(template.format(airflow=airflow_schema, schema=schema_name, source='CustomFields', deleted_col=deleted_col))
            deleted_col = self._soft_delete_column(cache_schema, 'Data_N_Object_T_CalculatedFields')
            if deleted_col is not None:
                sql_query_stack.append(template.format(airflow=airflow_schema, schema=cache_schema, source='CalculatedFields', deleted_col=deleted_col))
            sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

        # The parent-child symmetric view unions both directions of every
        # flagged edge.
        elif view_name == 'obj2obj: parent-child symmetric':
            target_table = 'Edges_N_Object_N_Object_T_ParentChildSymmetric'
            eval_columns = ['from_object_type', 'to_object_type']
            sql_query_stack = []
            for schema_name in registry_schemas:
                deleted_col = self._soft_delete_column(schema_name, 'Edges_N_Object_N_Object_T_ChildToParent')
                if deleted_col is None:
                    continue
                sql_query_stack.append(f"""
                        SELECT 'Child-to-Parent' AS edge_type,
                               c2p.from_object_type,
                               c2p.from_object_id,
                               c2p.to_object_type,
                               c2p.to_object_id,
                               c2p.context,
                               1 AS to_process,
                               0 AS deleted
                          FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_object_type, from_object_id, to_object_type, to_object_id)
                    INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_object_type, to_object_type)
                          WHERE tp.to_process = 1
                            AND tp.deleted = 0
                            AND tf.to_process = 1
                            AND c2p.{deleted_col} = 0

                     UNION ALL

                        SELECT 'Parent-to-Child' AS edge_type,
                               c2p.to_object_type      AS from_object_type,
                               c2p.to_object_id        AS from_object_id,
                               c2p.from_object_type    AS to_object_type,
                               c2p.from_object_id      AS to_object_id,
                               c2p.context             AS context,
                               1 AS to_process,
                               0 AS deleted
                          FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged tp
                    INNER JOIN {schema_name}.Edges_N_Object_N_Object_T_ChildToParent c2p
                         USING (from_object_type, from_object_id, to_object_type, to_object_id)
                    INNER JOIN {airflow_schema}.Operations_N_Object_N_Object_T_TypeFlags tf
                         USING (from_object_type, to_object_type)
                          WHERE tp.to_process = 1
                            AND tp.deleted = 0
                            AND tf.to_process = 1
                            AND c2p.{deleted_col} = 0
                    """)
            sql_query = '\n\t\tUNION ALL\n'.join(sql_query_stack)

        # Unknown view names are skipped with a warning.
        else:
            sysmsg.warning(f"Unknown view: '{view_name}'.")
            return

        #------------------#
        # Evaluate/commit #
        #------------------#
        if 'eval' in actions:
            sql_query_eval = f"SELECT {', '.join(eval_columns)}, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY {', '.join(eval_columns)}"
            if 'print' in actions:
                print_sql(sql_query_eval, title='8nZVFGbc')
            self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='8nZVFGbc')
        if 'commit' in actions:
            sysmsg.trace(f"⚙️  Processing view: '{view_name}' ...")

            # The target columns are discovered from the live table.
            target_table_columns = self.db.get_column_names(engine_name=engine_name, schema_name=cache_schema, table_name=target_table)
            if 'row_id' in target_table_columns:
                target_table_columns.remove('row_id')
            sql_query_commit = f"\tREPLACE INTO {cache_schema}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"
            if 'print' in actions:
                print_sql(sql_query_commit, title='Mn0to7TQ')

            # Chunked execution does not work with UNIONs, so the commit
            # runs in the shell directly.
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, verbose=False, query_id='Mn0to7TQ')
