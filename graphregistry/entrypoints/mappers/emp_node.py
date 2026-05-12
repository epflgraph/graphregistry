# graphregistry/entrypoints/api/mappers/emp_node.py
from __future__ import annotations
from typing import Any, cast
from graphregistry.entrypoints.api import schemas
from graphregistry.domain.models.entities.mdl_text import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_base import Field, NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList, NodeFieldList
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile

INSTITUTION_ID = "EPFL"

# Class definition
class EPNodeMapper:
    """
    Maps between API custom-field input shapes and domain NodeField / NodeFieldList.
    """

    @staticmethod
    def to_get_request(node: Node | dict[str, Any]) -> schemas.NodeMinimalFormat:

        # If input is a dict, convert to Node model
        if isinstance(node, dict):
            node = Node.model_validate(node)

        # Initialise page profile shortcut
        page_profile = node.page_profile

        #-----------------------#
        # Handle object subtype #
        #-----------------------#
        subtype: str | list[schemas.MultilingualText] | None = None
        if page_profile is not None:
            subtype_list: list[schemas.MultilingualText] = [
                schemas.MultilingualText(language=cast(schemas.TextLanguage, language), text=value)
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
        title_list: list[schemas.MultilingualText] = []
        if page_profile is not None:
            title_list = [
                schemas.MultilingualText(language=cast(schemas.TextLanguage, language), text=value)
                for language in DEFAULT_LANGUAGE_CODES
                if language in page_profile.name.item_map
                if (value := page_profile.name.get_value(language)).strip()
            ]
        title: str | list[schemas.MultilingualText] = title_list if title_list else node.title

        #---------------------------#
        # Handle object description #
        #---------------------------#
        description: str | list[schemas.MultilingualText] | dict[str, list[schemas.MultilingualText]] | None = None

        if page_profile is None:
            description = node.raw_text
        else:
            descriptions: dict[str, list[schemas.MultilingualText]] = {}

            for text_length in ("short", "medium", "long"):
                generated_text = getattr(page_profile.description, text_length)

                text_list = [
                    schemas.MultilingualText(language=cast(schemas.TextLanguage, language), text=value)
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
        url: str | list[schemas.MultilingualText] | None = None

        if page_profile is not None:
            url_list: list[schemas.MultilingualText] = [
                schemas.MultilingualText(language=cast(schemas.TextLanguage, language), text=value)
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
        custom_fields : list[schemas.CustomFieldInput] | None = Field(default_factory=list)

        custom_fields = [
            schemas.CustomFieldInput(
                field_language = field.key.field_language if field.key.field_language in DEFAULT_LANGUAGE_CODES else "n/a",
                field_name     = field.key.field_name,
                field_value    = "" if field.field_value is None else str(field.field_value),
            )
            for field in node.field_list.item_list
        ]

        # Return API node object
        return schemas.NodeMinimalFormat(
            type          = cast(schemas.ObjectType, node.key.object_type),
            subtype       = subtype,
            id            = node.key.object_id,
            title         = title,
            description   = description,
            url           = url,
            custom_fields = custom_fields,
        )

    @staticmethod
    def to_get_many_request(node_list: NodeList | list[Node | dict[str, Any]]) -> list[schemas.NodeMinimalFormat]:
        if isinstance(node_list, list):
            return [EPNodeMapper.to_get_request(node) for node in node_list]
        elif isinstance(node_list, NodeList):
            return [EPNodeMapper.to_get_request(node) for node in node_list.item_list]

    @staticmethod
    def from_save_request(request: schemas.NodeSaveAPIRequest | dict[str, Any]) -> Node:

        # If input is a dict, convert to NodeSaveAPIRequest model
        if isinstance(request, dict):
            request = schemas.NodeSaveAPIRequest.model_validate(request)

        # Create node key
        node_key = NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = request.node.type,
            object_id      = request.node.id
        )

        # Initialise node object
        node = Node(
            key = node_key,
            field_list = NodeFieldList.from_list(
                input_list = [cf.model_dump() for cf in (request.node.custom_fields or [])],
                key = node_key
            ),
            page_profile = PageProfile(key=node_key)
        )

        #-----------------------#
        # Handle object subtype #
        #-----------------------#
        if request.node.subtype is not None:

            # Input format: String
            if isinstance(request.node.subtype, str):
                assert node.page_profile is not None
                node.page_profile.subtype.set(language='en', value=request.node.subtype)

            # Input format: List of multilingual texts
            elif isinstance(request.node.subtype, list):
                assert node.page_profile is not None
                for mt in request.node.subtype:
                    # If the language is English, set the node title to this text (assuming it's the most complete title).
                    if mt.language == 'en':
                        node.title = mt.text
                    node.page_profile.subtype.set(language=mt.language, value=mt.text)

        #--------------------------#
        # Handle object name/title #
        #--------------------------#
        if request.node.title is not None:

            # Input format: String
            if isinstance(request.node.title, str):
                node.title = request.node.title

            # Input format: List of multilingual texts
            elif isinstance(request.node.title, list):
                assert node.page_profile is not None
                for mt in request.node.title:
                    # If the language is English, set the node title to this text (assuming it's the most complete title).
                    if mt.language == 'en':
                        node.title = mt.text
                    node.page_profile.name.set(language=mt.language, value=mt.text)

        #---------------------------#
        # Handle object description #
        #---------------------------#
        if request.node.description is not None:

            # Input format: String
            if isinstance(request.node.description, str):
                node.text_source, node.raw_text = "user input", request.node.description

            # Input format: List of multilingual texts (assumed to be long descriptions, since no length info provided)
            elif isinstance(request.node.description, list):
                assert node.page_profile is not None
                for mt in request.node.description:
                    if mt.language == 'en':
                        node.text_source, node.raw_text = "user input", mt.text
                    node.page_profile.description.long.set(language=mt.language, value=mt.text)

            # Input format: Dict of text length to list of multilingual texts
            elif isinstance(request.node.description, dict):
                assert node.page_profile is not None
                for text_length in request.node.description:
                    for mt in request.node.description[text_length]:
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
        if request.node.url is not None:

            # Input format: String
            if isinstance(request.node.url, str):
                assert node.page_profile is not None
                # Set the same URL for all languages, since no language info provided
                for lang in DEFAULT_LANGUAGE_CODES:
                    node.page_profile.external_url.set(language=lang, value=request.node.url)

            # Input format: List of multilingual texts
            elif isinstance(request.node.url, list):
                assert node.page_profile is not None
                for mt in request.node.url:
                    node.page_profile.external_url.set(language=mt.language, value=mt.text)

        # Remove node object
        return node

    @staticmethod
    def from_save_request_list(request_list: list[schemas.NodeSaveAPIRequest | dict[str, Any]]) -> NodeList:
        return NodeList(item_list=[EPNodeMapper.from_save_request(request) for request in request_list])

    @staticmethod
    def to_response_key(key: NodeKey) -> dict[str, str]:
        return {
            'type' : key.object_type,
            'id'   : key.object_id
        }

    @staticmethod
    def from_request_key(key: schemas.NodeSimplifiedKey) -> NodeKey:
        return NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = key.type,
            object_id      = key.id
        )

    @staticmethod
    def from_request_key_list(key_list: list[schemas.NodeSimplifiedKey]) -> NodeKeyList:
        return NodeKeyList(item_list=[EPNodeMapper.from_request_key(key) for key in key_list])
