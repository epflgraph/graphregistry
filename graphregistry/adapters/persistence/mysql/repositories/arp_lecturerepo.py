# graphregistry/adapters/persistence/mysql/repositories/arp_lecturerepo.py
from __future__ import annotations
from typing import Any, cast
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.domain.types import ActionSet

# Class definition
class MySQLLectureRepository(MySQLNodeRepository, LectureRepository):

    #-------------------------------#
    # Content processing operations #
    #-------------------------------#

    # Method: Get list of undownloaded lectures, returning a list of NodeKey objects for the undownloaded lectures
    def get_undownloaded(self) -> NodeKeyList:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path = sql_queries_paths['registry']['commit']['lecture_get_undownloaded'],
            airflow   = airflow_schema_name
        )

        # Execute query and fetch result
        undownloaded_lectures = cast(list[tuple[str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Extract lecture ids from query result and convert them into NodeKey objects
        undownloaded_lecture_keys = NodeKeyList(
            item_list=[
                NodeKey(institution_id='EPFL', object_type='Lecture', object_id=lecture_id)
                for (lecture_id,) in undownloaded_lectures
            ]
        )

        # Return the list of undownloaded lecture keys
        return undownloaded_lecture_keys

    # Method: Get file URL for a lecture based on the lecture key, returning the file URL as a string
    def get_file_url(self, lecture_key: NodeKey) -> str:

        # Check if lecture exists first (return None if not found)
        if not self.exists(lecture_key):
            self.msg.not_found(lecture_key)
            raise ValueError(f"Lecture with key {lecture_key} not found, cannot get file URL")

        # Get schema name for Lecture object type using the schema resolver
        engine_name, lecture_schema_name = self.schema_resolver.for_node(lecture_key)

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_file_url'],
            lectures    = lecture_schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        file_url_result = cast(list[tuple[str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Extract file URL from query result
        if not file_url_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"File URL for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the file URL string
        (file_url,) = file_url_result[0]

        # Return the file URL
        return file_url

    # Method: Save the video download task ID for a lecture in persistence
    def save_video_download_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Operations_N_Lecture_T_ProcessingTokens",
            key_column_names  = ["institution_id", "object_type", "object_id"],
            key_column_values = [lecture_key.institution_id, lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ['video_download_task_id'],
            upd_column_values = [task_id],
            actions           = ('commit',)
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Method: Get the video download task ID for a lecture (this can be used to check the status of the download or retrieve the downloaded video)
    def get_video_download_task_id(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_video_download_task_id'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        task_id_result = cast(list[tuple[str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Extract task ID from query result
        if not task_id_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Video download task ID for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the task ID string
        (task_id,) = task_id_result[0]

        # Return the video download task ID
        return task_id

    # Method: Save the video token for a lecture in persistence
    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> NodeKey:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Operations_N_Lecture_T_ProcessingTokens",
            key_column_names  = ["institution_id", "object_type", "object_id"],
            key_column_values = [lecture_key.institution_id, lecture_key.object_type, lecture_key.object_id],
            upd_column_names  = ['video_token'],
            upd_column_values = [video_token],
            actions           = ('commit',)
        )

        # Print status message
        self.msg.airflow_saved(lecture_key)

        # Return node for chaining
        return lecture_key

    # Method: Get the video token for a lecture (this can be used to retrieve the downloaded video or check if the video has been processed)
    def get_video_token(self, lecture_key: NodeKey) -> str:

        # Get schema name for Lecture object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['lecture_get_video_token'],
            airflow     = schema_name,
            lecture_id  = lecture_key.object_id
        )

        # Execute query and fetch result
        video_token_result = cast(list[tuple[str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Extract video token from query result
        if not video_token_result:
            self.msg.not_found(lecture_key)
            raise ValueError(f"Video token for lecture with key {lecture_key} not found in query result")

        # Unpack the single row and single column result to get the video token string
        (video_token,) = video_token_result[0]

        # Return the video token
        return video_token

    #-------------------------------------#
    # Lecture field enrichment operations #
    #-------------------------------------#

    # Method: Get enrichment task for a lecture based on the lecture key, returning a LectureEnrichmentTask object
    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:

        # Check if lecture exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema names from object type using the schema resolver
        engine_name, lecture_schema_name = self.schema_resolver.for_node(key)
        _, ontology_schema_name = self.schema_resolver.for_node(NodeKey(institution_id='dummy', object_type='Concept', object_id='dummy'))

        #----------------------------#
        # Get lecture's basic fields #
        #----------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path  = sql_queries_paths['registry']['commit']['lecture_get_enrich_task'],
            lectures   = lecture_schema_name,
            ontology   = ontology_schema_name,
            lecture_id = key.object_id
        )

        # Execute query and fetch result
        enrich_data = cast(list[tuple[Any, ...]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Any rows returned?
        if not enrich_data:
            print("❌ No concepts detected for any slides in this lecture, cannot build enrichment task.")
            return None

        # Build enrichment task object from fetched data
        enrich_task = MySQLLectureEnrichmentTaskMapper.from_rows(enrich_data, lecture_id=key.object_id)

        # Return the constructed enrichment task object
        return enrich_task

    # Method: Save enrichment result for a lecture to persistence and return the saved lecture key
    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:

        #======================#
        # Process Lecture node #
        #======================#

        # Create node key from lecture id
        node_key = NodeKey(institution_id='EPFL', object_type='Lecture', object_id=result.lecture_id)

        # Check if lecture exists first (return None if not found)
        if not self.exists(node_key):
            self.msg.not_found(node_key)
            raise ValueError(f"Lecture with key {node_key} not found, cannot save enrichment result")

        # Get the corresponding Node object for the lecture using its key
        node = self.get(node_key)

        # Run all necessary assertions to ensure the enrichment result can be applied to the Node object without issues
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
        self.save(node=node, actions=actions)

        #========================#
        # Process Lecture slides #
        #========================#

        # Loop over keyframe/slide ids
        for keyframe in result.keyframes:

            # Create node key for the slide using the lecture id and the keyframe id
            slide_node_key = NodeKey(institution_id='EPFL', object_type='Slide', object_id=keyframe.keyframe_id)

            # Check if slide node exists first (return None if not found)
            if not self.exists(slide_node_key):
                self.msg.not_found(slide_node_key)
                print(f"⚠️ Slide with key {slide_node_key} not found, skipping enrichment result for this slide.")
                continue

            # Get the corresponding Node object for the slide using its key
            slide_node = self.get(slide_node_key)

            # Run all necessary assertions to ensure the enrichment result can be applied to the slide Node object without issues
            assert slide_node is not None, f"Node with key {slide_node_key} should exist but was not found"

            # Assign enhanced concepts from enrichment result to the slide Node object
            slide_node.concepts.ai_validated = keyframe.refined_concepts.post_validated_list or slide_node.concepts.ai_validated

            # Save enriched slide node object
            self.save(node=slide_node, actions=actions)

        # Return the node key
        return node_key
