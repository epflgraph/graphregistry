# graphregistry/adapters/persistence/mysql/repositories/arp_lecturerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
from graphregistry.domain.models.entities.mdl_base import LectureKeyList
from graphregistry.domain.models.entities.mdl_lecture import LectureKey, Lecture, LectureList
from graphregistry.domain.types import ActionSet
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.application.services.srv_schema import SchemaResolver
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureMapper
from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import PAGE_PROFILE_COLUMNS
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.types import ObjectType

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLLectureRepository(LectureRepository):

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, db: GraphDB, schema_resolver: SchemaResolver) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.msg = GraphLogger()

    #----------------------------------------#
    # Basic Lecture CRUD/persistence operations #
    #----------------------------------------#

    # Method: Get list of existing lectures given an object type and id string pattern
    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str, str]]:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_list'],
            registry    = schema_name,
            object_type = object_type,
            id_pattern  = id_pattern.replace('*', '%') if id_pattern is not None else "%"
        )

        # Execute SQL query
        lecture_list = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Return lecture list
        return cast(list[tuple[str, str, str]], lecture_list)

    # Method: Check if a lecture exists in persistence from the lecture key
    def exists(self, key: LectureKey) -> bool:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_lecture(key)

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_exists'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute commit query
        lecture_exists = bool(self.db.execute_query(engine_name=engine_name, query=sql_query)[0][0])

        # Return True if count is greater than 0, indicating that the lecture exists, otherwise return False
        return lecture_exists

    # Method: Check if multiple lectures exist in persistence from a list of lecture keys
    def exists_many(self, key_list: LectureKeyList | list[LectureKey]) -> list[bool]:
        if isinstance(key_list, LectureKeyList):
            return [self.exists(key) for key in key_list.item_list]
        else:
            return [self.exists(key) for key in key_list]

    # Method: Fetch lecture data and construct Lecture object
    def get(self, key: LectureKey) -> Lecture | None:

        # Check if lecture exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_lecture(key)

        #-------------------------#
        # Get lecture's basic fields #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_get_basic'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        basic_data = cast(list[tuple[Any, ...]], self.db.execute_query(engine_name=engine_name, query=sql_query))
        basic_row = basic_data[0] if len(basic_data) > 0 else None

        # Any rows returned?
        if basic_row is None:
            self.msg.not_found(key)
            return None

        #--------------------------#
        # Get lecture's custom fields #
        #--------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_get_custom'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        custom_fields = cast(list[tuple[str, str, Any]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        #-------------------------#
        # Get lecture's page profile #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_get_profile'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        page_profile = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Any rows returned?
        if len(page_profile) > 0:
            page_profile_dict = dict(zip(PAGE_PROFILE_COLUMNS, page_profile[0]))
        else:
            page_profile_dict = {}

        #------------------------------#
        # Get lecture's detected concepts #
        #------------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_get_concepts'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        detected_concepts = cast(list[tuple[str, float]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Construct Lecture object from fetched data
        lecture = MySQLLectureMapper.from_parts(
            key               = key,
            basic_row         = basic_row,
            custom_field_rows = custom_fields,
            page_profile_row  = page_profile_dict,
            concept_rows      = detected_concepts
        )

        # Return the constructed Lecture object
        return lecture

    # Method: Fetch multiple lectures data and construct LectureList object from a list of lecture keys
    def get_many(self, key_list: LectureKeyList | list[LectureKey]) -> LectureList:
        if isinstance(key_list, LectureKeyList):
            key_list = key_list.item_list
        out = [lecture for lecture in (self.get(key) for key in key_list) if lecture is not None]
        return LectureList(item_list=out)

    # Method: Save (insert or update) lecture data to persistence
    def save(self, lecture: Lecture, actions: ActionSet = ('commit',)) -> Lecture:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_lecture(lecture.key)

        #---------------------#
        # Upsert basic fields #
        #---------------------#

        # Convert Lecture object to a dict representing the basic fields row
        basic_row = MySQLLectureMapper.to_basic_row(lecture)

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Lectures_N_Object",
            key_column_names  = ["institution_id", "object_type", "object_id"],
            key_column_values = [lecture.key.institution_id, lecture.key.object_type, lecture.key.object_id],
            upd_column_names  = list(basic_row.keys()),
            upd_column_values = list(basic_row.values()),
            actions           = actions,
        )

        #----------------------#
        # Upsert custom fields #
        #----------------------#

        # Convert Lecture object to a list of dicts representing the custom fields rows, then upsert each row
        for row in MySQLLectureMapper.to_custom_field_rows(lecture):
            self.db.execute_upsert_row(
                engine_name       = engine_name,
                schema_name       = schema_name,
                table_name        = "Data_N_Object_T_CustomFields",
                key_column_names  = ["institution_id", "object_type", "object_id", "field_language", "field_name"],
                key_column_values = [
                    row["institution_id"],
                    row["object_type"],
                    row["object_id"],
                    row["field_language"],
                    row["field_name"],
                ],
                upd_column_names  = ["field_value"],
                upd_column_values = [row["field_value"]],
                actions           = actions,
            )

        #---------------------#
        # Upsert page profile #
        #---------------------#

        # Convert Lecture object to a dict representing the page profile row
        page_profile_row = MySQLLectureMapper.to_page_profile_row(lecture)

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Data_N_Object_T_PageProfile",
            key_column_names  = ["institution_id", "object_type", "object_id"],
            key_column_values = [lecture.key.institution_id, lecture.key.object_type, lecture.key.object_id],
            upd_column_names  = list(page_profile_row.keys()),
            upd_column_values = list(page_profile_row.values()),
            actions           = actions,
        )

        #--------------------------#
        # Upsert detected concepts #
        #--------------------------#

        # Convert Lecture object to a list of dicts representing the custom fields rows, then upsert each row
        for row in MySQLLectureMapper.to_detected_concepts_rows(lecture):
            self.db.execute_upsert_row(
                engine_name       = engine_name,
                schema_name       = schema_name,
                table_name        = "Edges_N_Object_N_Concept_T_ConceptDetection",
                key_column_names  = ["institution_id", "object_type", "object_id", "concept_id", "text_source"],
                key_column_values = [
                    row["institution_id"],
                    row["object_type"],
                    row["object_id"],
                    row["concept_id"],
                    row["text_source"]
                ],
                upd_column_names  = ["score"],
                upd_column_values = [row["score"]],
                actions           = actions,
            )

        #---------------------#

        # Print status message
        self.msg.saved(lecture.key)

        # Return lecture for chaining
        return lecture

    # Method: Save (insert or update) multiple lectures data to persistence from a LectureList object
    def save_many(self, lecture_list: LectureList | list[Lecture], actions: ActionSet = ('commit',)) -> LectureList:
        if isinstance(lecture_list, LectureList):
            lecture_list = lecture_list.item_list
        return LectureList(item_list=[self.save(lecture, actions=actions) for lecture in lecture_list])

    # Method: Delete lecture data from persistence based on the lecture key
    def delete(self, key: LectureKey, actions: ActionSet = ('commit',)) -> bool | None:

        # Check if lecture exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_lecture(key)

        # Execute in commit mode
        if 'commit' in actions:

            # Resolve placeholdes in template query
            sql_query = resolve_sql_query(
                file_path      = sql_queries_paths['registry']['commit']['lecture_delete'],
                registry       = schema_name,
                institution_id = key.institution_id,
                object_type    = key.object_type,
                object_id      = key.object_id
            )

            # Execute commit query
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose='print' in actions)

            # Print status message
            self.msg.deleted(key)

            # Return True if lecture existed and was deleted
            return True

        # Return False if lecture exists but was not deleted
        return False

    # Method: Delete multiple lectures data from persistence based on a list of lecture keys
    def delete_many(self, key_list: LectureKeyList | list[LectureKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        if isinstance(key_list, LectureKeyList):
            return [self.delete(key, actions=actions) for key in key_list.item_list]
        else:
            return [self.delete(key, actions=actions) for key in key_list]

    #--------------------------------------------------#
    # Lecture diagnostics and special get/save operations #
    #--------------------------------------------------#

    # Method: Get lectures with no detected concepts based on optional object type and id pattern filters, returning a LectureList of the matching lectures
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> LectureList:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type if object_type is not None else "Course")

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_with_no_concepts'],
            registry    = schema_name,
            object_type = object_type if object_type is not None else "%",
            id_pattern  = id_pattern.replace('*', '%') if id_pattern is not None else "%"
        )

        # Execute SQL query and fetch result
        lecture_keys_data = cast(list[tuple[str, str, str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Construct LectureKey objects from fetched data
        lecture_keys = [
            LectureKey(
                institution_id = row[0],
                object_type    = cast(ObjectType, row[1]),
                object_id      = row[2]
            ) for row in lecture_keys_data
        ]

        # Fetch full Lecture objects for the LectureKeys and return as a LectureList
        return self.get_many(lecture_keys)
