# graphregistry/adapters/persistence/mysql/mappers/amp_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_conceptdet import MySQLConceptDetectionResultMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import NodeField, NodeFieldList
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureSlideOCTandConcepts, LectureConceptTitleList

# Class definition
class MySQLNodeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain NodeField / NodeFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], lecture_key: NodeKey) -> NodeField:
        field_language, field_name, field_value = row
        return NodeField(
            key=NodeFieldKey(
                key            = lecture_key,
                field_language = field_language,
                field_name     = field_name,
            ),
            field_value = field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], lecture_key: NodeKey) -> NodeField:
        return NodeField(
            key=NodeFieldKey(
                key            = lecture_key,
                field_language = row['field_language'],
                field_name     = row['field_name'],
            ),
            field_value = row['field_value'],
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, lecture_key: NodeKey) -> NodeFieldList:
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_row(row, lecture_key=lecture_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dict_list(rows: list[dict[str, Any]] | None, lecture_key: NodeKey) -> NodeFieldList:
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_dict(row, lecture_key=lecture_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(field: NodeField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_dict(field: NodeField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: NodeField) -> dict[str, Any]:
        return {
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_upsert_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_upsert_row(field) for field in field_list.item_list]

    @staticmethod
    def to_dict_list(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_dict(field) for field in field_list.item_list]

    @staticmethod
    def to_simplified_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_simplified_row(field) for field in field_list.item_list]

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
            key               = key,
            title             = title,
            text_source       = text_source,
            raw_text          = raw_text,
            field_list        = MySQLNodeFieldMapper.from_rows(custom_field_rows, lecture_key=key),
            page_profile      = MySQLPageProfileMapper.from_row(page_profile_row, lecture_key=key),
            detected_concepts = MySQLConceptDetectionResultMapper.from_rows(concept_rows),
        )

    @staticmethod
    def to_basic_row(lecture: Lecture) -> dict[str, Any]:
        return {
            'object_title' : lecture.title,
            'text_source'  : lecture.text_source,
            'raw_text'     : lecture.raw_text,
        }

    @staticmethod
    def to_custom_field_rows(lecture: Lecture) -> list[dict[str, Any]]:
        return MySQLNodeFieldMapper.to_upsert_rows(lecture.field_list)

    @staticmethod
    def to_page_profile_row(lecture: Lecture) -> dict[str, Any]:
        assert lecture.page_profile is not None
        return MySQLPageProfileMapper.to_row(lecture.page_profile)

    @staticmethod
    def to_detected_concepts_rows(lecture: Lecture) -> list[dict[str, Any]]:
        return MySQLConceptDetectionResultMapper.to_upsert_rows(
            lecture_key          = lecture.key,
            text_source       = lecture.text_source,
            detected_concepts = lecture.detected_concepts,
        )

    @staticmethod
    def to_simplified_dict(lecture: Lecture) -> dict[str, Any]:
        return {
            'institution_id'    : lecture.key.institution_id,
            'object_type'       : lecture.key.object_type,
            'object_id'         : lecture.key.object_id,
            'object_title'      : lecture.title,
            'text_source'       : lecture.text_source,
            'raw_text'          : lecture.raw_text,
            'custom_fields'     : MySQLNodeFieldMapper.to_simplified_rows(lecture.field_list),
            'page_profile'      : (MySQLPageProfileMapper.to_row(lecture.page_profile) if lecture.page_profile is not None else {}),
            'detected_concepts' : [concept.to_json() for concept in lecture.detected_concepts.item_list]
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Lecture:
        key = NodeKey(
            institution_id = data['institution_id'],
            object_type    = data['object_type'],
            object_id      = data['object_id'],
        )
        return Lecture(
            key               = key,
            title             = str(data.get('object_title') or ""),
            text_source       = str(data.get('text_source') or ""),
            raw_text          = str(data.get('raw_text') or ""),
            field_list        = MySQLNodeFieldMapper.from_dict_list(data['custom_fields'], lecture_key=key),
            page_profile      = MySQLPageProfileMapper.from_row(data['page_profile'], lecture_key=key),
            detected_concepts = MySQLConceptDetectionResultMapper.from_rows([
                (item['concept_id'], item['score'])
                for item in data.get('detected_concepts', [])
            ]),
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
        keyframes: list[LectureSlideOCTandConcepts] = []

        # Build keyframes list from row data (if any)
        for row in (rows or []):

            # Build the keyframe object for this row
            keyframe = LectureSlideOCTandConcepts(
                slide_id    = row[0],
                ocr_content = row[1],
                concepts    = LectureConceptTitleList(
                    item_list = row[2].split('|') if row[2] else []
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

    @staticmethod
    def to_dict(enrich_task: LectureEnrichmentTask) -> dict[str, Any]:
        return {
            'lecture_id': enrich_task.lecture_id,
            'keyframes': [
                {
                    'keyframe_id': keyframe.slide_id,
                    'ocr_content': keyframe.ocr_content,
                    'concepts': keyframe.concepts.item_list,
                }
                for keyframe in enrich_task.keyframes
            ]
        }