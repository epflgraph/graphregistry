# graphregistry/adapters/persistence/mysql/repositories/arp_lecturerepo.py
from __future__ import annotations
from typing import Any, cast
import rich
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.domain.types import ActionSet
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList

# Class definition
class MySQLLectureRepository(MySQLNodeRepository, LectureRepository):

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

        # Return the node key
        return node_key