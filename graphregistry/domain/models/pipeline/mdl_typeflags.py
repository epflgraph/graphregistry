# graphregistry/domain/models/pipeline/mdl_typeflags.py
from __future__ import annotations
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import FlagType, ObjectType

#==================#
# Class Definition #
#==================#
class NodeTypeFlag(BaseModel):
    """Model representing the processing switch of one object type for one flag family
    ('fields' or 'scores'). Activating a type flag tells the pipeline to consider that
    object type during the next planning cycle.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type: ObjectType
    flag_type: FlagType
    to_process: bool = False

#==================#
# Class Definition #
#==================#
class EdgeTypeFlag(BaseModel):
    """Model representing the processing switch of one undirected edge family. The
    database stores undirected families as two directional rows; the model keeps the
    canonical alphabetical form, and the repository expands and collapses rows.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    from_object_type: ObjectType
    to_object_type: ObjectType
    to_process: bool = False

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    # Internal Method: Normalise the pair to alphabetical order so that both stored
    # directions of an undirected family collapse onto one logical flag.
    @model_validator(mode="after")
    def canonicalise_pair(self) -> "EdgeTypeFlag":
        canonical = EdgeTypePair(
            from_object_type = self.from_object_type,
            to_object_type   = self.to_object_type,
        ).canonical_order
        if canonical.as_tuple != (self.from_object_type, self.to_object_type):
            object.__setattr__(self, "from_object_type", canonical.from_object_type)
            object.__setattr__(self, "to_object_type", canonical.to_object_type)
        return self

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Return the edge family governed by this flag.
    @property
    def pair(self) -> EdgeTypePair:
        return EdgeTypePair(
            from_object_type = self.from_object_type,
            to_object_type   = self.to_object_type,
        )

#==================#
# Class Definition #
#==================#
class TypeFlagConfig(BaseModel):
    """Model representing the full activation state of the airflow type flags: which
    node types should have their fields and/or scores reprocessed, and which edge
    families should be reprocessed. It is the domain counterpart of the typeflags
    configuration JSON exchanged with the legacy config and get_config_json methods.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    nodes: list[NodeTypeFlag] = Field(default_factory=list)
    edges: list[EdgeTypeFlag] = Field(default_factory=list)

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    # Internal Method: Reject duplicate node flags so each (object_type, flag_type)
    # pair appears at most once.
    @model_validator(mode="after")
    def validate_unique_node_flags(self) -> "TypeFlagConfig":
        seen = set()
        for flag in self.nodes:
            key = (flag.object_type, flag.flag_type)
            if key in seen:
                raise ValueError(f"Duplicate node type flag: {flag.object_type} / {flag.flag_type}")
            seen.add(key)
        return self

    # Internal Method: Reject duplicate edge families; duplicates would surface as
    # contradictory activation rows once the repository expands them.
    @model_validator(mode="after")
    def validate_unique_edge_flags(self) -> "TypeFlagConfig":
        seen = set()
        for flag in self.edges:
            key = flag.pair.as_tuple
            if key in seen:
                raise ValueError(f"Duplicate edge type flag: {flag.from_object_type} - {flag.to_object_type}")
            seen.add(key)
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    # Public Method: Build the configuration from the legacy JSON format, which carries
    # one activation boolean per flag family per node type and drops inactive edges.
    @classmethod
    def from_json(cls, input_json: dict) -> "TypeFlagConfig":
        # Node rows carry one boolean per flag family; emit one flag per active family.
        node_flags = []
        for row in input_json.get("nodes", []):
            node_type, process_fields, process_scores = row
            if process_fields:
                node_flags.append(NodeTypeFlag(object_type=node_type, flag_type="fields", to_process=True))
            if process_scores:
                node_flags.append(NodeTypeFlag(object_type=node_type, flag_type="scores", to_process=True))
        # Edge rows are (from, to, process) triples; inactive rows are dropped.
        edge_flags = []
        for row in input_json.get("edges", []):
            from_object_type, to_object_type, process_fields = row
            if process_fields:
                edge_flags.append(EdgeTypeFlag(from_object_type=from_object_type, to_object_type=to_object_type, to_process=True))
        return cls(nodes=node_flags, edges=edge_flags)

    # Public Method: Serialise back to the legacy JSON format so adapters can round-trip
    # the configuration through the typeflags table.
    def to_json(self) -> dict:
        # Collect node activation per type, preserving the (fields, scores) column order.
        node_rows = {}
        for flag in self.nodes:
            fields_active, scores_active = node_rows.get(flag.object_type, (False, False))
            if flag.flag_type == "fields":
                fields_active = flag.to_process
            else:
                scores_active = flag.to_process
            node_rows[flag.object_type] = (fields_active, scores_active)
        return {
            "nodes" : [[node_type, fields_active, scores_active] for node_type, (fields_active, scores_active) in node_rows.items()],
            "edges" : [[flag.from_object_type, flag.to_object_type, flag.to_process] for flag in self.edges],
        }

    #----------------#
    # Query methods #
    #----------------#
    # Public Method: List the node types activated for one flag family.
    def active_node_types(self, flag_type: FlagType) -> list[ObjectType]:
        return [flag.object_type for flag in self.nodes if flag.flag_type == flag_type and flag.to_process]

    # Public Method: List the activated edge families in canonical order.
    def active_edge_pairs(self) -> list[EdgeTypePair]:
        return [flag.pair for flag in self.edges if flag.to_process]

    # Public Method: Check whether anything is activated at all.
    def is_empty(self) -> bool:
        return not any(flag.to_process for flag in self.nodes) and not any(flag.to_process for flag in self.edges)
