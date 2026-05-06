# graphregistry/entrypoints/api/mappers/emp_node.py
from __future__ import annotations
from typing import Any
from graphregistry.entrypoints.api import schemas
from graphregistry.domain.models.entities.mdl_text import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeFieldList
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile

INSTITUTION_ID = "EPFL"

# Class definition
class APINodeMapper:
    """
    Maps between API custom-field input shapes and domain NodeField / NodeFieldList.
    """

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
