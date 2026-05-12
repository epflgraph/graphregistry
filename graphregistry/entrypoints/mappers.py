# graphregistry/entrypoints/mappers.py
from __future__ import annotations
from typing import Any, cast
from graphregistry.entrypoints.api import schemas
from graphregistry.domain.models.entities.mdl_text import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_base import Field, NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList, NodeFieldList
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile
from graphregistry.domain.models.entities.mdl_text import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_base import EdgeKey, EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList, EdgeFieldList
from graphregistry.entrypoints.schemas import (
    MultilingualTextSpec,
    CustomFieldSpec,
    NodeKeySpec,
    NodeKeyListSpec,
    NodeSpec,
    NodeListSpec,
    EdgeKeySpec,
    EdgeKeyListSpec,
    EdgeSpec,
    EdgeListSpec,
)
from graphregistry.domain.types import TextLanguage, FieldLanguage, ObjectType

INSTITUTION_ID = "EPFL"

# Class definition
class SpecMapper:
    """
    Maps between API input shapes (NodeKeySpec, NodeSpec, EdgeKeySpec, EdgeSpec)
    and domain models (NodeKey, Node, EdgeKey, Edge), and vice versa.
    """

    @staticmethod
    def from_node_key_spec(key_spec: NodeKeySpec | dict[str, Any]) -> NodeKey:
        if isinstance(key_spec, dict):
            key_spec = NodeKeySpec.model_validate(key_spec)
        return NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = key_spec.type,
            object_id      = key_spec.id
        )

    @staticmethod
    def from_node_key_list_spec(key_list_spec: NodeKeyListSpec | list[NodeKeySpec] | list[dict[str, Any]]) -> NodeKeyList:
        if isinstance(key_list_spec, NodeKeyListSpec):
            return NodeKeyList(item_list=[SpecMapper.from_node_key_spec(key_spec) for key_spec in key_list_spec.item_list])
        elif isinstance(key_list_spec, list):
            return NodeKeyList(item_list=[SpecMapper.from_node_key_spec(key_spec) for key_spec in key_list_spec])

    @staticmethod
    def from_node_spec(node_spec: NodeSpec | dict[str, Any]) -> Node:

        # If input is a dict, convert to NodeSpec model
        if isinstance(node_spec, dict):
            node_spec = NodeSpec.model_validate(node_spec)

        # Create node key
        node_key = NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = node_spec.type,
            object_id      = node_spec.id
        )

        # Initialise node object
        node = Node(
            key = node_key,
            field_list = NodeFieldList.from_list(
                input_list = [cf.model_dump() for cf in (node_spec.custom_fields or [])],
                key = node_key
            ),
            page_profile = PageProfile(key=node_key)
        )

        #-----------------------#
        # Handle object subtype #
        #-----------------------#
        if node_spec.subtype is not None:

            # Input format: String
            if isinstance(node_spec.subtype, str):
                assert node.page_profile is not None
                node.page_profile.subtype.set(language='en', value=node_spec.subtype)

            # Input format: List of multilingual texts
            elif isinstance(node_spec.subtype, list):
                assert node.page_profile is not None
                for mt in node_spec.subtype:
                    # If the language is English, set the node title to this text (assuming it's the most complete title).
                    if mt.language == 'en':
                        node.title = mt.text
                    node.page_profile.subtype.set(language=mt.language, value=mt.text)

        #--------------------------#
        # Handle object name/title #
        #--------------------------#
        if node_spec.title is not None:

            # Input format: String
            if isinstance(node_spec.title, str):
                node.title = node_spec.title

            # Input format: List of multilingual texts
            elif isinstance(node_spec.title, list):
                assert node.page_profile is not None
                for mt in node_spec.title:
                    # If the language is English, set the node title to this text (assuming it's the most complete title).
                    if mt.language == 'en':
                        node.title = mt.text
                    node.page_profile.name.set(language=mt.language, value=mt.text)

        #---------------------------#
        # Handle object description #
        #---------------------------#
        if node_spec.description is not None:

            # Input format: String
            if isinstance(node_spec.description, str):
                node.text_source, node.raw_text = "user input", node_spec.description

            # Input format: List of multilingual texts (assumed to be long descriptions, since no length info provided)
            elif isinstance(node_spec.description, list):
                assert node.page_profile is not None
                for mt in node_spec.description:
                    if mt.language == 'en':
                        node.text_source, node.raw_text = "user input", mt.text
                    node.page_profile.description.long.set(language=mt.language, value=mt.text)

            # Input format: Dict of text length to list of multilingual texts
            elif isinstance(node_spec.description, dict):
                assert node.page_profile is not None
                for text_length in node_spec.description:
                    for mt in node_spec.description[text_length]:
                        # Set short description
                        if text_length == 'short':
                            node.page_profile.description.short.set(language=mt.language, value=mt.text)
                        # Set medium description
                        elif text_length == 'medium':
                            node.page_profile.description.medium.set(language=mt.language, value=mt.text)
                        # Set long description, and set node raw_text and text_source to the English long description (if exists)
                        elif text_length == 'long':
                            if mt.language == 'en':
                                node.text_source, node.raw_text = "user input", mt.text
                            node.page_profile.description.long.set(language=mt.language, value=mt.text)

        #-------------------#
        # Handle object url #
        #-------------------#
        if node_spec.url is not None:

            # Input format: String
            if isinstance(node_spec.url, str):
                assert node.page_profile is not None
                # Set the same URL for all languages, since no language info provided
                for lang in DEFAULT_LANGUAGE_CODES:
                    node.page_profile.external_url.set(language=lang, value=node_spec.url)

            # Input format: List of multilingual texts
            elif isinstance(node_spec.url, list):
                assert node.page_profile is not None
                for mt in node_spec.url:
                    node.page_profile.external_url.set(language=mt.language, value=mt.text)

        # Remove node object
        return node

    @staticmethod
    def from_node_list_spec(node_list_spec: NodeListSpec | list[NodeSpec] | list[dict[str, Any]]) -> NodeList:
        if isinstance(node_list_spec, NodeListSpec):
            return NodeList(item_list=[SpecMapper.from_node_spec(node_spec) for node_spec in node_list_spec.item_list])
        elif isinstance(node_list_spec, list):
            return NodeList(item_list=[SpecMapper.from_node_spec(node_spec) for node_spec in node_list_spec])

    @staticmethod
    def to_node_key_spec(key: NodeKey | dict[str, Any]) -> NodeKeySpec:
        if isinstance(key, dict):
            key = NodeKey.model_validate(key)
        return NodeKeySpec(
            type = key.object_type,
            id   = key.object_id
        )

    @staticmethod
    def to_node_key_list_spec(key_list: NodeKeyList | list[NodeKey] | list[dict[str, Any]]) -> NodeKeyListSpec:
        if isinstance(key_list, NodeKeyList):
            return NodeKeyListSpec(item_list=[SpecMapper.to_node_key_spec(key) for key in key_list.item_list])
        elif isinstance(key_list, list):
            return NodeKeyListSpec(item_list=[SpecMapper.to_node_key_spec(key) for key in key_list])

    @staticmethod
    def to_node_spec(node: Node | dict[str, Any]) -> NodeSpec:

        # If input is a dict, convert to Node model
        if isinstance(node, dict):
            node = Node.model_validate(node)

        # Initialise page profile shortcut
        page_profile = node.page_profile

        #-----------------------#
        # Handle object subtype #
        #-----------------------#
        subtype: str | list[MultilingualTextSpec] | None = None
        if page_profile is not None:
            subtype_list: list[MultilingualTextSpec] = [
                MultilingualTextSpec(language=cast(TextLanguage, language), text=value)
                for language in DEFAULT_LANGUAGE_CODES
                if language in page_profile.subtype.item_map
                if (value := page_profile.subtype.item_map.get(language, "")).strip()
            ]
            if len(subtype_list) == 1 and subtype_list[0].language == "en":
                subtype = subtype_list[0].text
            elif subtype_list:
                subtype = subtype_list

        #--------------------------#
        # Handle object name/title #
        #--------------------------#
        title_list: list[MultilingualTextSpec] = []
        if page_profile is not None:
            title_list = [
                MultilingualTextSpec(language=cast(TextLanguage, language), text=value)
                for language in DEFAULT_LANGUAGE_CODES
                if language in page_profile.name.item_map
                if (value := page_profile.name.get_value(language)).strip()
            ]
        title: str | list[MultilingualTextSpec] = title_list if title_list else node.title

        #---------------------------#
        # Handle object description #
        #---------------------------#
        description: str | list[MultilingualTextSpec] | dict[str, list[MultilingualTextSpec]] | None = None

        if page_profile is None:
            description = node.raw_text
        else:
            descriptions: dict[str, list[MultilingualTextSpec]] = {}

            for text_length in ("short", "medium", "long"):
                generated_text = getattr(page_profile.description, text_length)

                text_list = [
                    MultilingualTextSpec(language=cast(TextLanguage, language), text=value)
                    for language in DEFAULT_LANGUAGE_CODES
                    if language in generated_text.item_map
                    if (value := generated_text.get_value(language)).strip()
                ]

                if text_list:
                    descriptions[text_length] = text_list

            if not descriptions:
                description = node.raw_text
            elif set(descriptions) == {"long"}:
                description = descriptions["long"]
            else:
                description = descriptions

        #-------------------#
        # Handle object url #
        #-------------------#
        url: str | list[MultilingualTextSpec] | None = None

        if page_profile is not None:
            url_list: list[MultilingualTextSpec] = [
                MultilingualTextSpec(language=cast(TextLanguage, language), text=value)
                for language in DEFAULT_LANGUAGE_CODES
                if language in page_profile.external_url.item_map
                if (value := page_profile.external_url.item_map.get(language, "")).strip()
            ]

            if url_list:
                default_language_values = [
                    page_profile.external_url.item_map.get(language, "")
                    for language in DEFAULT_LANGUAGE_CODES
                ]

                if (
                    all(default_language_values)
                    and len(set(default_language_values)) == 1
                ):
                    url = default_language_values[0]
                else:
                    url = url_list

        #----------------------#
        # Handle custom fields #
        #----------------------#
        custom_fields : list[CustomFieldSpec] | None = Field(default_factory=list)

        custom_fields = [
            CustomFieldSpec(
                field_language = field.key.field_language if field.key.field_language in DEFAULT_LANGUAGE_CODES else "n/a",
                field_name     = field.key.field_name,
                field_value    = "" if field.field_value is None else str(field.field_value),
            )
            for field in node.field_list.item_list
        ]

        # Return API node object
        return NodeSpec(
            type          = cast(ObjectType, node.key.object_type),
            subtype       = subtype,
            id            = node.key.object_id,
            title         = title,
            description   = description,
            url           = url,
            custom_fields = custom_fields,
        )

    @staticmethod
    def to_node_list_spec(node_list: NodeList | list[Node] | list[dict[str, Any]]) -> NodeListSpec:
        if isinstance(node_list, NodeList):
            return NodeListSpec(item_list=[SpecMapper.to_node_spec(node) for node in node_list.item_list])
        elif isinstance(node_list, list):
            return NodeListSpec(item_list=[SpecMapper.to_node_spec(node) for node in node_list])

    @staticmethod
    def from_edge_key_spec(key: EdgeKeySpec | dict[str, Any]) -> EdgeKey:
        if isinstance(key, dict):
            key = EdgeKeySpec.model_validate(key)
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
    def from_edge_key_list_spec(key_list_spec: EdgeKeyListSpec | list[EdgeKeySpec] | list[dict[str, Any]]) -> EdgeKeyList:
        if isinstance(key_list_spec, EdgeKeyListSpec):
            return EdgeKeyList(item_list=[SpecMapper.from_edge_key_spec(key_spec) for key_spec in key_list_spec.item_list])
        elif isinstance(key_list_spec, list):
            return EdgeKeyList(item_list=[SpecMapper.from_edge_key_spec(key_spec) for key_spec in key_list_spec])

    @staticmethod
    def from_edge_spec(edge_spec: EdgeSpec | dict[str, Any]) -> Edge:

        # If input is a dict, convert to EdgeSpec model
        if isinstance(edge_spec, dict):
            edge_spec = EdgeSpec.model_validate(edge_spec)

        # Create edge key
        edge_key = EdgeKey(
            from_institution_id = INSTITUTION_ID,
            from_object_type    = edge_spec.from_type,
            from_object_id      = edge_spec.from_id,
            to_institution_id   = INSTITUTION_ID,
            to_object_type      = edge_spec.to_type,
            to_object_id        = edge_spec.to_id,
            context             = edge_spec.context,
        )

        # Initialise edge object
        edge = Edge(
            key = edge_key,
            field_list = EdgeFieldList.from_list(
                input_list = [cf.model_dump() for cf in (edge_spec.custom_fields or [])],
                key = edge_key,
            ),
        )

        # Return edge object
        return edge

    @staticmethod
    def from_edge_list_spec(edge_list_spec: EdgeListSpec | list[EdgeSpec] | list[dict[str, Any]]) -> EdgeList:
        if isinstance(edge_list_spec, EdgeListSpec):
            return EdgeList(item_list=[SpecMapper.from_edge_spec(edge_spec) for edge_spec in edge_list_spec.item_list])
        elif isinstance(edge_list_spec, list):
            return EdgeList(item_list=[SpecMapper.from_edge_spec(edge_spec) for edge_spec in edge_list_spec])

    @staticmethod
    def to_edge_key_spec(key: EdgeKey | dict[str, Any]) -> EdgeKeySpec:
        if isinstance(key, dict):
            key = EdgeKey.model_validate(key)
        return EdgeKeySpec(
            from_type = key.from_object_type,
            from_id   = key.from_object_id,
            to_type   = key.to_object_type,
            to_id     = key.to_object_id,
            context   = key.context,
        )

    @staticmethod
    def to_edge_key_list_spec(key_list: EdgeKeyList | list[EdgeKey] | list[dict[str, Any]]) -> EdgeKeyListSpec:
        if isinstance(key_list, EdgeKeyList):
            return EdgeKeyListSpec(item_list=[SpecMapper.to_edge_key_spec(key) for key in key_list.item_list])
        elif isinstance(key_list, list):
            return EdgeKeyListSpec(item_list=[SpecMapper.to_edge_key_spec(key) for key in key_list])

    @staticmethod
    def to_edge_spec(edge: Edge | dict[str, Any]) -> EdgeSpec:

        # If input is a dict, convert to Edge model
        if isinstance(edge, dict):
            edge = Edge.model_validate(edge)

        #----------------------#
        # Handle custom fields #
        #----------------------#
        custom_fields = [
            CustomFieldSpec(
                field_language = field.key.field_language if field.key.field_language in DEFAULT_LANGUAGE_CODES else "n/a",
                field_name     = field.key.field_name,
                field_value    = "" if field.field_value is None else str(field.field_value),
            )
            for field in edge.field_list.item_list
        ]

        # Return API edge object
        return EdgeSpec(
            from_type     = edge.key.from_object_type,
            from_id       = edge.key.from_object_id,
            to_type       = edge.key.to_object_type,
            to_id         = edge.key.to_object_id,
            context       = edge.key.context,
            custom_fields = custom_fields,
        )

    @staticmethod
    def to_edge_list_spec(edge_list: EdgeList | list[Edge] | list[dict[str, Any]]) -> EdgeListSpec:
        if isinstance(edge_list, EdgeList):
            return EdgeListSpec(item_list=[SpecMapper.to_edge_spec(edge) for edge in edge_list.item_list])
        elif isinstance(edge_list, list):
            return EdgeListSpec(item_list=[SpecMapper.to_edge_spec(edge) for edge in edge_list])
