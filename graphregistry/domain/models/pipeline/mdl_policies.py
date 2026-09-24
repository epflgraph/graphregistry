# graphregistry/domain/models/pipeline/mdl_policies.py
from __future__ import annotations
from datetime import timedelta
from itertools import combinations_with_replacement
from typing import Literal
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import LinkTablePartition, ObjectType, ScoreType

#==================#
# Class Definition #
#==================#
class ExpirationPolicy(BaseModel):
    """Model representing the policy applied by the expire command: which tracking
    records should have their staleness flag set on the next planning cycle.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Age threshold for expiration; a zero timedelta expires regardless of cache date.
    older_than: timedelta = timedelta(0)
    # Maximum number of records to expire per object type; None means no limit.
    limit_per_type: int | None = None
    # Flag families in scope: fields covers the FieldsChanged tables, scores the
    # ScoresExpired table, mirroring the legacy Orchestration.expire options.
    include_fields: bool = True
    include_scores: bool = True
    include_nodes: bool = True
    include_edges: bool = True
    # Optional restriction to a subset of object types; None means all active types.
    object_types: list[ObjectType] | None = None

#==================#
# Class Definition #
#==================#
class OrderRule(BaseModel):
    """Model representing one ordering rule of a link selection policy, applied
    before the score ordering when candidate links are ranked.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    field: str
    direction: Literal["ASC", "DESC"] = "DESC"

#==================#
# Class Definition #
#==================#
class LinkSelectionPolicy(BaseModel):
    """Model representing the selection and ranking policy of the horizontal
    patch: which score drives the ordering and how many links survive.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Score column that drives the primary ordering of candidate links.
    score_type: ScoreType
    # Maximum number of links kept per document; None means no truncation.
    rank_threshold: int | None = None
    # Additional ordering rules applied before the score ordering.
    ordering: list[OrderRule] = Field(default_factory=list)

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    # Public Method: Build the default policy for one table partition:
    # organisational links are structural and untruncated, semantic links are
    # ranked and truncated at 32.
    @classmethod
    def default_for_partition(cls, partition: LinkTablePartition) -> "LinkSelectionPolicy":
        if partition == "ORG":
            return cls(score_type="degree_score", rank_threshold=None)
        return cls(score_type="semantic_score", rank_threshold=32)

#==================#
# Class Definition #
#==================#
class LinkFieldProjection(BaseModel):
    """Model representing the content-drift refresh mapping: which source fields
    of the linked documents are denormalised into which fields of the doc-link
    projection.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    source_fields: list[str] = Field(default_factory=list)
    target_fields: list[str] = Field(default_factory=list)

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    # Internal Method: Reject mismatched source and target field lists since
    # the projection is applied pairwise.
    @model_validator(mode="after")
    def validate_same_length(self) -> "LinkFieldProjection":
        if len(self.source_fields) != len(self.target_fields):
            raise ValueError("source_fields and target_fields must have the same length")
        return self

#==================#
# Class Definition #
#==================#
class ProcessingScope(BaseModel):
    """Model representing the outcome of pipeline planning: which node types and edge
    families the next cycle should process, per flag family.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    node_fields_types: list[ObjectType] = Field(default_factory=list)
    node_scores_types: list[ObjectType] = Field(default_factory=list)
    edge_type_pairs: list[EdgeTypePair] = Field(default_factory=list)

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    # Public Method: Derive the scope from a typeflag configuration. When the
    # indexable edge pairs are provided (from the index configuration), the
    # edge families are restricted to them, mirroring the legacy
    # get_types_to_process intersection.
    @classmethod
    def from_type_flag_config(cls, flags: TypeFlagConfig, indexable_edge_pairs: "list[EdgeTypePair] | None" = None) -> "ProcessingScope":
        edge_pairs = flags.active_edge_pairs()
        if indexable_edge_pairs is not None:
            # Only families present in the index configuration remain in scope.
            indexable = {pair.canonical_order.as_tuple for pair in indexable_edge_pairs}
            edge_pairs = [pair for pair in edge_pairs if pair.canonical_order.as_tuple in indexable]
        return cls(
            node_fields_types = flags.active_node_types("fields"),
            node_scores_types = flags.active_node_types("scores"),
            edge_type_pairs   = edge_pairs,
        )

    #----------------#
    # Query methods #
    #----------------#
    # Public Method: Compute the score-matrix pairs implied by the score-activated node
    # types: every unordered combination with replacement of the activated types,
    # following the legacy get_types_to_process rule for scores.
    def scores_matrix_pairs(self) -> list[EdgeTypePair]:
        # Case-insensitive sort mirrors the legacy combination ordering.
        sorted_types = sorted(set(self.node_scores_types), key=str.casefold)
        return [
            EdgeTypePair(
                from_object_type = from_type,
                to_object_type   = to_type,
            )
            for from_type, to_type in combinations_with_replacement(sorted_types, 2)
        ]

    # Public Method: Check whether the scope selects anything at all.
    def is_empty(self) -> bool:
        return (
            not self.node_fields_types
            and not self.node_scores_types
            and not self.edge_type_pairs
        )
