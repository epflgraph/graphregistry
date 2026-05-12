# graphregistry/entrypoints/api/mappers/emp_edge.py
from __future__ import annotations
from typing import Any, cast
from graphregistry.entrypoints.api import schemas
from graphregistry.domain.models.entities.mdl_text import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_base import EdgeKey, EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList, EdgeFieldList

INSTITUTION_ID = "EPFL"

# Class definition
class EPEdgeMapper:
    """
    Maps between API custom-field input shapes and domain EdgeField / EdgeFieldList.
    """

    @staticmethod
    def to_get_request(edge: Edge | dict[str, Any]) -> schemas.EdgeMinimalFormat:

        # If input is a dict, convert to Edge model
        if isinstance(edge, dict):
            edge = Edge.model_validate(edge)

        #----------------------#
        # Handle custom fields #
        #----------------------#
        custom_fields = [
            schemas.CustomFieldInput(
                field_language = field.key.field_language if field.key.field_language in DEFAULT_LANGUAGE_CODES else "n/a",
                field_name     = field.key.field_name,
                field_value    = "" if field.field_value is None else str(field.field_value),
            )
            for field in edge.field_list.item_list
        ]

        # Return API edge object
        return schemas.EdgeMinimalFormat(
            from_type     = cast(schemas.ObjectType, edge.key.from_object_type),
            from_id       = edge.key.from_object_id,
            to_type       = cast(schemas.ObjectType, edge.key.to_object_type),
            to_id         = edge.key.to_object_id,
            context       = edge.key.context,
            custom_fields = custom_fields,
        )

    @staticmethod
    def to_get_request_list(edge_list: EdgeList | list[Edge | dict[str, Any]]) -> list[schemas.EdgeMinimalFormat]:
        if isinstance(edge_list, list):
            return [EPEdgeMapper.to_get_request(edge) for edge in edge_list]
        elif isinstance(edge_list, EdgeList):
            return [EPEdgeMapper.to_get_request(edge) for edge in edge_list.item_list]

    @staticmethod
    def from_save_request(request: schemas.EdgeSaveAPIRequest | dict[str, Any]) -> Edge:

        # If input is a dict, convert to EdgeSaveAPIRequest model
        if isinstance(request, dict):
            request = schemas.EdgeSaveAPIRequest.model_validate(request)

        # Create edge key
        edge_key = EdgeKey(
            from_institution_id = INSTITUTION_ID,
            from_object_type    = request.edge.from_type,
            from_object_id      = request.edge.from_id,
            to_institution_id   = INSTITUTION_ID,
            to_object_type      = request.edge.to_type,
            to_object_id        = request.edge.to_id,
            context             = request.edge.context,
        )

        # Initialise edge object
        edge = Edge(
            key = edge_key,
            field_list = EdgeFieldList.from_list(
                input_list = [cf.model_dump() for cf in (request.edge.custom_fields or [])],
                key = edge_key,
            ),
        )

        # Return edge object
        return edge

    @staticmethod
    def from_save_request_list(request_list: list[schemas.EdgeSaveAPIRequest | dict[str, Any]]) -> EdgeList:
        return EdgeList(item_list=[EPEdgeMapper.from_save_request(request) for request in request_list])

    @staticmethod
    def to_request_key(key: EdgeKey) -> dict[str, str]:
        return {
            'from_type' : key.from_object_type,
            'from_id'   : key.from_object_id,
            'to_type'   : key.to_object_type,
            'to_id'     : key.to_object_id,
            'context'   : key.context,
        }

    @staticmethod
    def from_request_key(key: schemas.EdgeSimplifiedKey) -> EdgeKey:
        return EdgeKey(
            from_institution_id = INSTITUTION_ID,
            from_object_type    = key.from_type,
            from_object_id      = key.from_id,
            to_institution_id   = INSTITUTION_ID,
            to_object_type      = key.to_type,
            to_object_id        = key.to_id,
            context             = key.context
        )

    @staticmethod
    def from_request_key_list(key_list: list[schemas.EdgeSimplifiedKey]) -> EdgeKeyList:
        return EdgeKeyList(item_list=[EPEdgeMapper.from_request_key(key) for key in key_list])
