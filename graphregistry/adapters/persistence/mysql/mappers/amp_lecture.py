# graphregistry/adapters/persistence/mysql/mappers/amp_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_conceptdet import MySQLConceptDetectionResultMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import NodeField, NodeFieldList, Node
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureKeyframeOCTandConcepts, LectureConceptTitleList
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeFieldMapper

# Class definition
class MySQLLectureMapper:
    """
    Maps between MySQL row shapes and the domain Lecture model.
    """

    @staticmethod
    def from_parts(
        key               : NodeKey,
        basic_row         : tuple[Any, ...] | None,
        custom_field_rows : list[tuple[Any, ...]] | None = None,
        page_profile_row  : dict[str, Any] | None = None,
        concept_rows      : list[tuple[Any, ...]] | None = None,
    ) -> Lecture:
        """
        Build a Lecture from the separate SQL result parts.
        """
        # Get basic fields
        title, text_source, raw_text = basic_row if basic_row is not None else 3*("",)

        # Return lecture object
        return Lecture(
            node = Node(
                key               = key,
                title             = title,
                text_source       = text_source,
                raw_text          = raw_text,
                field_list        = MySQLNodeFieldMapper.from_rows(custom_field_rows, node_key=key),
                page_profile      = MySQLPageProfileMapper.from_row(page_profile_row, node_key=key),
                detected_concepts = MySQLConceptDetectionResultMapper.from_rows(concept_rows),
            )
        )

    @staticmethod
    def to_basic_row(lecture: Lecture) -> dict[str, Any]:
        return {
            'object_title' : lecture.node.title,
            'text_source'  : lecture.node.text_source,
            'raw_text'     : lecture.node.raw_text,
        }

    @staticmethod
    def to_custom_field_rows(lecture: Lecture) -> list[dict[str, Any]]:
        return MySQLNodeFieldMapper.to_upsert_rows(lecture.node.field_list)

    @staticmethod
    def to_page_profile_row(lecture: Lecture) -> dict[str, Any]:
        assert lecture.node.page_profile is not None
        return MySQLPageProfileMapper.to_row(lecture.node.page_profile)

    @staticmethod
    def to_detected_concepts_rows(lecture: Lecture) -> list[dict[str, Any]]:
        return MySQLConceptDetectionResultMapper.to_upsert_rows(
            node_key          = lecture.node.key,
            text_source       = lecture.node.text_source,
            detected_concepts = lecture.node.detected_concepts,
        )

    @staticmethod
    def to_simplified_dict(lecture: Lecture) -> dict[str, Any]:
        return {
            'institution_id'    : lecture.node.key.institution_id,
            'object_type'       : lecture.node.key.object_type,
            'object_id'         : lecture.node.key.object_id,
            'object_title'      : lecture.node.title,
            'text_source'       : lecture.node.text_source,
            'raw_text'          : lecture.node.raw_text,
            'custom_fields'     : MySQLNodeFieldMapper.to_simplified_rows(lecture.node.field_list),
            'page_profile'      : (MySQLPageProfileMapper.to_row(lecture.node.page_profile) if lecture.node.page_profile is not None else {}),
            'detected_concepts' : [concept.to_json() for concept in lecture.node.detected_concepts.item_list]
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Lecture:
        key = NodeKey(
            institution_id = data['institution_id'],
            object_type    = data['object_type'],
            object_id      = data['object_id'],
        )
        return Lecture(
            node = Node(
                key               = key,
                title             = str(data.get('object_title') or ""),
                text_source       = str(data.get('text_source') or ""),
                raw_text          = str(data.get('raw_text') or ""),
                field_list        = MySQLNodeFieldMapper.from_dict_list(data['custom_fields'], node_key=key),
                page_profile      = MySQLPageProfileMapper.from_row(data['page_profile'], node_key=key),
                detected_concepts = MySQLConceptDetectionResultMapper.from_rows([
                    (item['concept_id'], item['score'])
                    for item in data.get('detected_concepts', [])
                ])
            )
        )

    @staticmethod
    def to_simplified_dict_list(lecture_list: LectureList | list[Lecture]) -> list[dict[str, Any]]:
        if isinstance(lecture_list, LectureList):
            lecture_list = lecture_list.item_list
        return [MySQLLectureMapper.to_simplified_dict(lecture) for lecture in lecture_list]

    @staticmethod
    def from_simplified_dict_list(data: list[dict[str, Any]]) -> LectureList:
        return LectureList(item_list=[MySQLLectureMapper.from_simplified_dict(item) for item in (data or [])])

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
