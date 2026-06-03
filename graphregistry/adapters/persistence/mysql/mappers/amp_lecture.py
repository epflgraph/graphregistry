# graphregistry/adapters/persistence/mysql/mappers/amp_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_conceptmap import MySQLConceptMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import NodeField, NodeFieldList, Node
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureKeyframeOCTandConcepts, LectureConceptTitleList
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeFieldMapper, MySQLNodeMapper

# Class definition
class MySQLLectureMapper(MySQLNodeMapper):
    """
    Maps between MySQL row shapes and the domain Lecture model.
    """
    pass

# Class definition
class MySQLLectureEnrichmentTaskMapper:
    """ Maps between MySQL row shapes and the domain LectureEnrichmentTask model.
    """

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]], lecture_id: str) -> LectureEnrichmentTask:

        # Initialize the keyframes list
        keyframes: list[LectureKeyframeOCTandConcepts] = []

        # Build keyframes list from row data (if any)
        for row in (rows or []):

            # Build the keyframe object for this row
            keyframe = LectureKeyframeOCTandConcepts(
                keyframe_id = row[0],
                ocr_content = row[1],
                concepts    = LectureConceptTitleList(
                    raw_unrefined_list = row[2].split('|') if row[2] else []
                )
            )
            # Append the keyframe to the list
            keyframes.append(keyframe)

        # Build the enrichment task object
        enrich_task = LectureEnrichmentTask(
            lecture_id = lecture_id,
            keyframes  = keyframes
        )

        # Return the enrichment task
        return enrich_task
