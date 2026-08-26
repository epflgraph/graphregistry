# graphregistry/adapters/persistence/mysql/repositories/rpo_lecturerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
from graphregistry.adapters.persistence.mysql.mappers.map_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.persistence.mysql.repositories._helpers import qualified_table, upsert_rows
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_lecture import LectureRepository
from graphregistry.application.ports.repositories.prt_lecture_processing import LectureProcessingStatePort
from graphregistry.application.ports.repositories.prt_node import NodeRepository
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.types import ActionSet

# If TYPE_CHECKING is True, import GraphDB, MySQLUnitOfWork and SchemaResolver for type
# hints only.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork
    from graphregistry.application.ports.repositories.resolvers import SchemaResolver

#==================#
# Class Definition #
#==================#
class MySQLLectureRepository(LectureRepository, LectureProcessingStatePort):
    """MySQL adapter for lecture processing and enrichment persistence.

    The repository is bound to a UnitOfWork when used inside application
    services. In that mode it also creates a node repository that shares the
    same UnitOfWork, so lecture enrichment updates are atomic with the
    underlying node updates.

    For backward compatibility it can also be constructed directly with a
    GraphDB client, schema resolver, and node repository.
    """

    # Class initialization and dependency injection
    def __init__(self, db: "GraphDB | None" = None, schema_resolver: "SchemaResolver | None" = None, node_repo: NodeRepository | None = None, *, uow: "MySQLUnitOfWork | None" = None) -> None:

        # Validate that either a UnitOfWork is provided, or a GraphDB,
        # SchemaResolver and NodeRepository are provided, but not both.
        if uow is not None and (db is not None or schema_resolver is not None or node_repo is not None):
            raise ValueError("Provide either uow= or (db=, schema_resolver=, node_repo=), not both.")

        # If a UnitOfWork is provided, use its db, schema_resolver and create a
        # node repository that shares the same UnitOfWork.
        if uow is not None:
            self._uow = uow
            self.db = uow.db
            self.schema_resolver = uow.schema_resolver
            self.node_repo: NodeRepository = MySQLNodeRepository(uow=uow)

        # If a UnitOfWork is not provided, ensure that db, schema_resolver and
        # node_repo are provided; otherwise, raise an error.
        elif db is not None and schema_resolver is not None and node_repo is not None:
            self._uow = None
            self.db = db
            self.schema_resolver = schema_resolver
            self.node_repo = node_repo
        else:
            raise ValueError("MySQLLectureRepository requires either uow= or (db=, schema_resolver=, node_repo=).")

        # Initialize a GraphLogger instance for logging messages.
        self.msg = GraphLogger()

    #================================================================#
    # Function Group: Internal helpers                               #
    #================================================================#

    # Internal Function: Return a session for engine_name, creating a standalone one if
    def _session(self, engine_name: str) -> MySQLSession:
        """Return a session for engine_name, creating a standalone one if needed."""
        if self._uow is not None:
            return self._uow.get_session(engine_name)

        # No UnitOfWork is active, so open a standalone session for this engine.
        session = MySQLSession(self.db, engine_name)
        session.begin()
        return session

    # Internal Function: Close a session created outside a UnitOfWork.
    def _close_standalone_session(self, session: MySQLSession) -> None:
        """Close a session created outside a UnitOfWork."""
        if self._uow is None:
            session.close()

    # Internal Function: Execute a read query and return the results as a list of tuples.
    def _execute_read(self, engine_name: str, query: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a read query."""
        session = self._session(engine_name)
        try:
            return session.execute(query, params)
        finally:
            self._close_standalone_session(session)

    # Internal Function: Return a safely quoted schema-qualified table name.
    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return qualified_table(schema_name, table_name)

    # Internal Function: upsert single row
    @staticmethod
    def _upsert_single_row(
        session: MySQLSession,
        table_path: str,
        key_column_names: list[str],
        key_column_values: list[Any],
        upd_column_names: list[str],
        upd_column_values: list[Any],
    ) -> None:
    #----------------------------------------------------------------#
        """Upsert a single row using the shared batch upsert helper."""
        row = dict(zip(key_column_names, key_column_values))
        row.update(dict(zip(upd_column_names, upd_column_values)))
        upsert_rows(session, table_path, key_column_names, upd_column_names, [row])

    # Internal Function: commit single row
    def _commit_single_row(
        self,
        engine_name: str,
        table_path: str,
        key_column_names: list[str],
        key_column_values: list[Any],
        upd_column_names: list[str],
        upd_column_values: list[Any],
    ) -> None:
    #----------------------------------------------------------------#
        """Open a session, upsert one row, and commit if running standalone."""
        session = self._session(engine_name)
        try:
            self._upsert_single_row(
                session           = session,
                table_path        = table_path,
                key_column_names  = key_column_names,
                key_column_values = key_column_values,
                upd_column_names  = upd_column_names,
                upd_column_values = upd_column_values,
            )
            if self._uow is None:
                session.commit()
        except Exception:
            if self._uow is None:
                session.rollback()
            raise
        finally:
            self._close_standalone_session(session)

    #================================================================#
    # Method Group: Content processing operations                    #
    #================================================================#

    # Public Method: get undownloaded
    def get_undownloaded(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_with_video_undownloaded'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        undownloaded_lectures = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        undownloaded_lecture_keys = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in undownloaded_lectures
            ]
        )

        # Return the list of undownloaded lecture keys
        return undownloaded_lecture_keys

    # Public Method: get file url
    def get_file_url(self, lecture_key: NodeKey) -> str:

        # Check if lecture exists first (return None if not found)
        if not self.node_repo.exists(lecture_key):
            self.msg.not_found(lecture_key)
            raise ValueError(f"Lecture with key {lecture_key} not found, cannot get file URL")

        # Get schema name for Lecture object type using the schema resolver
        engine_name, lecture_schema_name = self.schema_resolver.for_node(lecture_key)

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_file_url'],
            lectures    = lecture_schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        file_url_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract file URL from query result
        if not file_url_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"File URL for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the file URL string
        (file_url,) = file_url_result[0]

        # Return the file URL
        return file_url

    # Public Method: Save the video download task ID for a lecture in persistence.
    def save_video_download_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Persist token row
        self._commit_single_row(
            engine_name       = engine_name,
            table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ["video_download_task_id"],
            upd_column_values = [task_id],
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get video download task id
    def get_video_download_task_id(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_task_id_video_download'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        task_id_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract task ID from query result
        if not task_id_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Video download task ID for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the task ID string
        (task_id,) = task_id_result[0]

        # Return the video download task ID
        return task_id

    # Public Method: get unfinished video download tasks
    def get_unfinished_video_download_tasks(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_unfinished_tasks_video_download'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        unfinished_video_tasks = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        unfinished_video_task_keys = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in unfinished_video_tasks
            ]
        )

        # Return the list of lecture keys with unfinished video tasks
        return unfinished_video_task_keys

    # Public Method: Save the video token for a lecture in persistence.
    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Persist token row
        self._commit_single_row(
            engine_name       = engine_name,
            table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ["video_token"],
            upd_column_values = [video_token],
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get video token
    def get_video_token(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_token_id_video'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        video_token_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract video token from query result
        if not video_token_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Video token for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the video token string
        (video_token,) = video_token_result[0]

        # Return the video token
        return video_token

    #================================================================#
    # Method Group: Audio extraction operations                      #
    #================================================================#

    # Public Method: get with unextracted audio
    def get_with_unextracted_audio(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_with_audio_unextracted'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        lectures_with_unextracted_audio = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        lecture_keys_with_unextracted_audio = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in lectures_with_unextracted_audio
            ]
        )

        # Return the list of lecture keys with unextracted audio
        return lecture_keys_with_unextracted_audio

    # Public Method: save audio extraction task id
    def save_audio_extraction_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Persist token row
        self._commit_single_row(
            engine_name       = engine_name,
            table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ["audio_extraction_task_id"],
            upd_column_values = [task_id],
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get audio extraction task id
    def get_audio_extraction_task_id(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_task_id_audio_extraction'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        task_id_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract task ID from query result
        if not task_id_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Audio extraction task ID for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the task ID string
        (task_id,) = task_id_result[0]

        # Return the audio extraction task ID
        return task_id

    # Public Method: get unfinished audio extraction tasks
    def get_unfinished_audio_extraction_tasks(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_unfinished_tasks_audio_extraction'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        unfinished_audio_tasks = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        unfinished_audio_task_keys = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in unfinished_audio_tasks
            ]
        )

        # Return the list of lecture keys with unfinished audio extraction tasks
        return unfinished_audio_task_keys

    # Public Method: save audio token
    def save_audio_token(self, lecture_key: NodeKey, audio_token: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Persist token row
        self._commit_single_row(
            engine_name       = engine_name,
            table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ["audio_token"],
            upd_column_values = [audio_token],
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get audio token
    def get_audio_token(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_token_id_audio'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        audio_token_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract audio token from query result
        if not audio_token_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Audio token for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the audio token string
        (audio_token,) = audio_token_result[0]

        # Return the audio token
        return audio_token

    #================================================================#
    # Method Group: Slide detection operations                       #
    #================================================================#

    # Public Method: get with undetected slides
    def get_with_undetected_slides(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_with_slides_undetected'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        lectures_with_undetected_slides = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        lecture_keys_with_undetected_slides = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in lectures_with_undetected_slides
            ]
        )

        # Return the list of lecture keys with undetected slides
        return lecture_keys_with_undetected_slides

    # Public Method: save slide detection task id
    def save_slide_detection_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Persist token row
        self._commit_single_row(
            engine_name       = engine_name,
            table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ["slide_detection_task_id"],
            upd_column_values = [task_id],
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get slide detection task id
    def get_slide_detection_task_id(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_task_id_slide_detection'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        task_id_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract task ID from query result
        if not task_id_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Slide detection task ID for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the task ID string
        (task_id,) = task_id_result[0]

        # Return the slide detection task ID
        return task_id

    # Public Method: get unfinished slide detection tasks
    def get_unfinished_slide_detection_tasks(self, limit: int | None = 16) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_unfinished_tasks_slide_detection'],
            airflow   = airflow_schema_name,
            limit     = limit if limit is not None else 16
        )

        # Execute query and fetch result
        unfinished_slide_tasks = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        unfinished_slide_task_keys = NodeKeyList(
            item_list=[
                NodeKey(object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in unfinished_slide_tasks
            ]
        )

        # Return the list of lecture keys with unfinished slide detection tasks
        return unfinished_slide_task_keys

    # Public Method: save slide tokens
    def save_slide_tokens(self, lecture_key: NodeKey, slide_num_and_tokens: list[tuple[int, str]]) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Get video token
        video_token = self.get_video_token(lecture_key)

        # Build slide token rows for a single batched upsert
        slide_rows: list[dict[str, Any]] = []
        slide_keys: list[NodeKey] = []
        for slide_num, slide_token in slide_num_and_tokens:
            slide_id = f"{lecture_key.object_id}-{slide_num:04d}"
            slide_key = NodeKey(object_type="Slide", object_id=slide_id)
            slide_keys.append(slide_key)
            slide_rows.append({
                "object_type" : slide_key.object_type,
                "object_id"   : slide_key.object_id,
                "video_token" : video_token,
                "image_token" : slide_token,
            })

        # Open a session to write the slide and lecture processing tokens.
        session = self._session(engine_name)
        try:
            if slide_rows:
                upsert_rows(
                    session          = session,
                    table_path       = self._qt(schema_name, "Operations_N_Slide_T_ProcessingTokens"),
                    key_column_names = ["object_type", "object_id"],
                    upd_column_names = ["video_token", "image_token"],
                    rows             = slide_rows,
                )

            # Mark the lecture as having slides detected.
            self._upsert_single_row(
                session           = session,
                table_path        = self._qt(schema_name, "Operations_N_Lecture_T_ProcessingTokens"),
                key_column_names  = ["object_type", "object_id"],
                key_column_values = [lecture_key.object_type, lecture_key.object_id],
                upd_column_names  = ["slides_detected"],
                upd_column_values = [True],
            )

            # Commit the standalone session if we created it outside a UnitOfWork.
            if self._uow is None:
                session.commit()
        except Exception:
            if self._uow is None:
                session.rollback()
            raise
        finally:
            self._close_standalone_session(session)

        # Emit saved notifications for every generated slide key.
        for slide_key in slide_keys:
            self.msg.airflow_saved(slide_key)

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Public Method: get slide tokens
    def get_slide_tokens(self, lecture_key: NodeKey) -> list[str]:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_token_id_list_slides'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Note: the result is a list of slide keys
        slide_tokens_result = cast(list[tuple[str]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Extract slide tokens from query result
        if not slide_tokens_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Slide tokens for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the slide tokens string,
        # then split it back into a list.
        (slide_tokens_str,) = slide_tokens_result[0]
        slide_tokens = slide_tokens_str.split(",") if slide_tokens_str else []
        return slide_tokens

    #================================================================#
    # Method Group: Lecture field enrichment operations              #
    #================================================================#

    # Public Method: get enrichment task
    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:

        # Check if lecture exists first (return None if not found)
        if not self.node_repo.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema names from object type using the schema resolver
        engine_name, lecture_schema_name = self.schema_resolver.for_node(key)
        _, ontology_schema_name = self.schema_resolver.for_node(NodeKey(object_type='Concept', object_id='dummy'))

        #----------------------------#
        # Get lecture's basic fields
        #----------------------------#

        # Resolve placeholders in template query
        sql_query = resolve_sql_query(
            file_path  = sql_queries_paths['registry']['commit']['lecture_get_enrich_task'],
            lectures   = lecture_schema_name,
            ontology   = ontology_schema_name,
            lecture_id = key.object_id
        )

        # Execute query and fetch result
        enrich_data = cast(list[tuple[Any, ...]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Any rows returned?
        if not enrich_data:
            print("❌ No concepts detected for any slides in this lecture, cannot build enrichment task.")
            return None

        # Build enrichment task object from fetched data
        enrich_task = MySQLLectureEnrichmentTaskMapper.from_rows(enrich_data, lecture_id=key.object_id)

        # Return the constructed enrichment task object
        return enrich_task

    # Public Method: save enrichment result
    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:

        #======================#
        # Process Lecture node #
        #======================#

        # Create node key from lecture id
        node_key = NodeKey(object_type='Lecture', object_id=result.lecture_id)

        # Check if lecture exists first (return None if not found)
        if not self.node_repo.exists(node_key):
            self.msg.not_found(node_key)
            raise ValueError(f"Lecture with key {node_key} not found, cannot save enrichment result")

        # Get the corresponding Node object for the lecture using its key
        node = self.node_repo.get(node_key)

        # Run all necessary assertions to ensure the enrichment result can be applied to
        # the Node object without issues.
        assert node                          is not None, f"Node with key {node_key} should exist but was not found"
        assert node.page_profile             is not None, f"Node with key {node_key} should have a page profile but it was None"
        assert node.page_profile.name        is not None, f"Node with key {node_key} should have a page profile name but it was None"
        assert node.page_profile.description is not None, f"Node with key {node_key} should have a page profile description but it was None"
        assert node.concepts                 is not None, f"Node with key {node_key} should have concepts but it was None"

        # Convert concepts list into scored concepts object
        result.top_concepts.post_validated_list

        # Assign enhanced fields from enrichment result to the Node object
        node.page_profile.name.en               = result.title              or node.page_profile.name.en
        node.page_profile.description.long.en   = result.long_description   or node.page_profile.description.long.en
        node.page_profile.description.medium.en = result.medium_description or node.page_profile.description.medium.en
        node.page_profile.description.short.en  = result.short_description  or node.page_profile.description.short.en
        node.concepts.ai_validated              = result.top_concepts.post_validated_list or node.concepts.ai_validated

        # Save enriched node object
        self.node_repo.save(node=node, actions=actions)

        #========================#
        # Process Lecture slides #
        #========================#

        # Loop over keyframe/slide ids
        for keyframe in result.keyframes:

            # Create node key for the slide using the lecture id and the keyframe id
            slide_node_key = NodeKey(object_type='Slide', object_id=keyframe.keyframe_id)

            # Check if slide node exists first (return None if not found)
            if not self.node_repo.exists(slide_node_key):
                self.msg.not_found(slide_node_key)
                print(f"⚠️ Slide with key {slide_node_key} not found, skipping enrichment result for this slide.")
                continue

            # Get the corresponding Node object for the slide using its key
            slide_node = self.node_repo.get(slide_node_key)

            # Run all necessary assertions to ensure the enrichment result can be applied
            # to the slide Node object without issues.
            assert slide_node is not None, f"Node with key {slide_node_key} should exist but was not found"

            # Assign enhanced concepts from enrichment result to the slide Node object
            slide_node.concepts.ai_validated = keyframe.refined_concepts.post_validated_list or slide_node.concepts.ai_validated

            # Save enriched slide node object
            self.node_repo.save(node=slide_node, actions=actions)

        # Return the node key
        return node_key
