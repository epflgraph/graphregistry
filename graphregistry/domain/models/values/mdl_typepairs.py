# graphregistry/domain/models/values/mdl_typepairs.py
from __future__ import annotations
from typing import Mapping
from pydantic import BaseModel
from graphregistry.domain.types import ObjectType, ScoreDomain

# Object types belonging to the ontology layer. Edge families touching any of these
# types are stored in the ontology scores matrices rather than the research and
# education matrices.
ONTOLOGY_OBJECT_TYPES: frozenset[str] = frozenset({"Category", "Concept", "Curated area"})

#==================#
# Class Definition #
#==================#
class EdgeTypePair(BaseModel):
    """Model representing an unordered pair of object types identifying one edge family
    in the graph (e.g. Person-Course). Scores matrices and airflow edge flags are keyed
    by the alphabetically sorted form of this pair, following the database convention
    for undirected edges.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    from_object_type: ObjectType
    to_object_type: ObjectType

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    # Public Method: Build a pair from a plain (from, to) tuple, as used in config keys.
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str]) -> "EdgeTypePair":
        return cls(
            from_object_type = input_tuple[0],
            to_object_type   = input_tuple[1],
        )

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Return the pair in alphabetical order, matching the database
    # convention for undirected edge families (LEAST/GREATEST in the typeflag queries).
    @property
    def canonical_order(self) -> "EdgeTypePair":
        # Tuple comparison provides the alphabetical order of the undirected family.
        forward  = (self.from_object_type, self.to_object_type)
        backward = (self.to_object_type, self.from_object_type)
        if backward < forward:
            return EdgeTypePair(
                from_object_type = self.to_object_type,
                to_object_type   = self.from_object_type,
            )
        return self

    # Public Method: Check whether the pair touches an ontology object type. Ontology
    # tuples follow dedicated scores matrices and are excluded from degree scoring.
    @property
    def is_ontology_tuple(self) -> bool:
        return (
            self.from_object_type in ONTOLOGY_OBJECT_TYPES
            or self.to_object_type in ONTOLOGY_OBJECT_TYPES
        )

    # Public Method: Return both object types as a plain tuple, suitable as dict key.
    @property
    def as_tuple(self) -> tuple[ObjectType, ObjectType]:
        return (self.from_object_type, self.to_object_type)

    #--------------------------#
    # Scores matrix mapping #
    #--------------------------#
    # Public Method: Resolve the scores-matrix domain of the pair. The research and
    # education assignment is configuration-driven and passed in by the caller; the
    # ontology special case is a fixed rule that takes precedence over the mapping.
    def matrix_domain(self, domain_mapping: Mapping[tuple[str, str], ScoreDomain]) -> ScoreDomain | None:
        if self.is_ontology_tuple:
            return "ontology"
        return domain_mapping.get(self.canonical_order.as_tuple)
