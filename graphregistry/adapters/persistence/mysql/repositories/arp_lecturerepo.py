# graphregistry/adapters/persistence/mysql/repositories/arp_lecturerepo.py
from __future__ import annotations
from typing import Any, cast

import rich
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.types import ActionSet
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query

# Class definition
class MySQLLectureRepository(MySQLNodeRepository, LectureRepository):

    # Method: Get enrichment task for a lecture based on the lecture key, returning a LectureEnrichmentTask object
    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:

        # Check if lecture exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        #----------------------------#
        # Get lecture's basic fields #
        #----------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['lecture_get_enrich_task'],
            registry       = schema_name,
            lecture_id     = key.object_id
        )

        # Execute query and fetch result
        enrich_data = cast(list[tuple[Any, ...]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Any rows returned?
        if not enrich_data:
            self.msg.not_found(key)
            return None

        # Build enrichment task object from fetched data
        enrich_task = MySQLLectureEnrichmentTaskMapper.from_rows(enrich_data, lecture_id=key.object_id)

        # Return the constructed enrichment task object
        return enrich_task

    # Method: Save enrichment result for a lecture to persistence and return the saved lecture key
    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:

        rich.print(result)

        return NodeKey(
            institution_id = 'EPFL',
            object_type    = 'Lecture',
            object_id      = result.lecture_id
        )