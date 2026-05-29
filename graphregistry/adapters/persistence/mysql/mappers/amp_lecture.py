# graphregistry/adapters/persistence/mysql/mappers/amp_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_conceptdet import MySQLConceptDetectionResultMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import LectureFieldKey, LectureKey
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList, LectureField, LectureFieldList

# Class definition
class MySQLLectureFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain LectureField / LectureFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], lecture_key: LectureKey) -> LectureField:
        field_language, field_name, field_value = row
        return LectureField(
            key=LectureFieldKey(
                key            = lecture_key,
                field_language = field_language,
                field_name     = field_name,
            ),
            field_value = field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], lecture_key: LectureKey) -> LectureField:
        return LectureField(
            key=LectureFieldKey(
                key            = lecture_key,
                field_language = row['field_language'],
                field_name     = row['field_name'],
            ),
            field_value = row['field_value'],
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, lecture_key: LectureKey) -> LectureFieldList:
        return LectureFieldList(
            item_list=[
                MySQLLectureFieldMapper.from_row(row, lecture_key=lecture_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dict_list(rows: list[dict[str, Any]] | None, lecture_key: LectureKey) -> LectureFieldList:
        return LectureFieldList(
            item_list=[
                MySQLLectureFieldMapper.from_dict(row, lecture_key=lecture_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(field: LectureField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_dict(field: LectureField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: LectureField) -> dict[str, Any]:
        return {
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_upsert_rows(field_list: LectureFieldList) -> list[dict[str, Any]]:
        return [MySQLLectureFieldMapper.to_upsert_row(field) for field in field_list.item_list]

    @staticmethod
    def to_dict_list(field_list: LectureFieldList) -> list[dict[str, Any]]:
        return [MySQLLectureFieldMapper.to_dict(field) for field in field_list.item_list]

    @staticmethod
    def to_simplified_rows(field_list: LectureFieldList) -> list[dict[str, Any]]:
        return [MySQLLectureFieldMapper.to_simplified_row(field) for field in field_list.item_list]

# Class definition
class MySQLLectureMapper:
    """
    Maps between MySQL row shapes and the domain Lecture model.
    """

    @staticmethod
    def from_parts(
        key               : LectureKey,
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
            field_list        = MySQLLectureFieldMapper.from_rows(custom_field_rows, lecture_key=key),
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
        return MySQLLectureFieldMapper.to_upsert_rows(lecture.field_list)

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
            'custom_fields'     : MySQLLectureFieldMapper.to_simplified_rows(lecture.field_list),
            'page_profile'      : (MySQLPageProfileMapper.to_row(lecture.page_profile) if lecture.page_profile is not None else {}),
            'detected_concepts' : [concept.to_json() for concept in lecture.detected_concepts.item_list]
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Lecture:
        key = LectureKey(
            institution_id = data['institution_id'],
            object_type    = data['object_type'],
            object_id      = data['object_id'],
        )
        return Lecture(
            key               = key,
            title             = str(data.get('object_title') or ""),
            text_source       = str(data.get('text_source') or ""),
            raw_text          = str(data.get('raw_text') or ""),
            field_list        = MySQLLectureFieldMapper.from_dict_list(data['custom_fields'], lecture_key=key),
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
