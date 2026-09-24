# graphregistry/domain/models/pipeline/mdl_indexdocs.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_text import MultilingualText
from graphregistry.domain.models.values.mdl_cachestate import CacheState
from graphregistry.domain.types import LinkSubtype, LinkTablePartition

# Mapping from the link subtype column values onto the table partitions: the
# organisational tables hold both parent-child directions, the semantic tables
# hold the concept-based links.
LINK_SUBTYPE_TO_PARTITION: dict[LinkSubtype, LinkTablePartition] = {
    'Parent-to-Child' : 'ORG',
    'Child-to-Parent' : 'ORG',
    'Semantic'        : 'SEM',
}

#==================#
# Class Definition #
#==================#
class DocKey(BaseModel):
    """Model representing the identity of one document in the search index. Doc types
    are the index-facing vocabulary of object types, defined by the index configuration
    rather than the ontology, and are therefore kept as free strings.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    doc_type: str
    doc_id: str

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    # Public Method: Build a document key from a plain (doc_type, doc_id) tuple.
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str]) -> "DocKey":
        return cls(doc_type=input_tuple[0], doc_id=input_tuple[1])

    # Public Method: Serialise the key to a plain (doc_type, doc_id) tuple.
    def to_tuple(self) -> tuple[str, str]:
        return (self.doc_type, self.doc_id)

#==================#
# Class Definition #
#==================#
class DocLinkTypeKey(BaseModel):
    """Model representing the identity of one index doc-link projection: the links of
    one doc type towards one link type, discriminated by table partition. It
    identifies the physical Index_D_{doc_type}_L_{link_type}_T_{partition} tables;
    the table naming itself remains a repository concern.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    doc_type: str
    link_type: str
    partition: LinkTablePartition

#==================#
# Class Definition #
#==================#
class IndexDocLink(BaseModel):
    """Model representing one link inside an index document: a denormalised reference
    to a target document, carrying the target's presentation fields so the search index
    can render links without re-joining the registry.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # The document the link points to; (link_type, link_id) in table vocabulary.
    target: DocKey
    # Subtype of the link, matching the link_subtype column: the parent-child
    # direction for organisational links, 'Semantic' for concept-based links.
    link_subtype: LinkSubtype
    # Presentation rank of the link inside the owning document's link list; None
    # while the link has not been ranked by the horizontal patch yet.
    rank: int | None = None
    # Relevance score driving the ranking: semantic score for SEM, degree for ORG.
    score: float | None = None
    names: MultilingualText = Field(default_factory=MultilingualText)
    short_descriptions: MultilingualText = Field(default_factory=MultilingualText)
    # Config-driven extra columns merged into the physical doc-link tables.
    custom_fields: dict[str, Any] = Field(default_factory=dict)

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Return the table partition the link belongs to, derived from
    # its subtype: both parent-child directions live in the organisational tables.
    @property
    def partition(self) -> LinkTablePartition:
        return LINK_SUBTYPE_TO_PARTITION[self.link_subtype]

#==================#
# Class Definition #
#==================#
class IndexDoc(BaseModel):
    """Model representing one document of the search index: the flattened, denormalised
    projection of a registry node together with its links. This is the unit consumed by
    the Elasticsearch export.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    key: DocKey
    degree_score: float | None = None
    short_code: str | None = None
    subtype: MultilingualText = Field(default_factory=MultilingualText)
    names: MultilingualText = Field(default_factory=MultilingualText)
    short_descriptions: MultilingualText = Field(default_factory=MultilingualText)
    long_descriptions: MultilingualText = Field(default_factory=MultilingualText)
    links: list[IndexDocLink] = Field(default_factory=list)
    # Config-driven extra columns merged into the physical index tables.
    custom_fields: dict[str, Any] = Field(default_factory=dict)

    #----------------#
    # Query methods #
    #----------------#
    # Public Method: Select the links belonging to one doc-link projection, identified
    # by owning doc type, target link type, and table partition.
    def links_of_type(self, link_type_key: DocLinkTypeKey) -> list[IndexDocLink]:
        # Links of another owning doc type belong to a different projection entirely.
        if self.key.doc_type != link_type_key.doc_type:
            return []
        return [
            link for link in self.links
            if link.target.doc_type == link_type_key.link_type
            and link.partition == link_type_key.partition
        ]

#==================#
# Class Definition #
#==================#
class DocLinkProjection(BaseModel):
    """Model representing one doc-link projection in the pipeline state machine: the
    dirty-flag state of one Index_D_*_L_*_T_* table. The content itself is accessed
    through the repository; the model carries identity and processing state only.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    key: DocLinkTypeKey
    state: CacheState = Field(default_factory=CacheState)
