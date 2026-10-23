# graphregistry/domain/models/pipeline/mdl_stats.py
from __future__ import annotations
from pydantic import BaseModel
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import ObjectType

#==================#
# Class Definition #
#==================#
class RefreshStats(BaseModel):
    """Base model for refresh statistics, counting why records were scheduled for
    processing: never cached, checksum drift, or policy expiry. Concrete statistics
    are grouped either by node type or by edge family.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    new_or_never_cached: int = 0
    checksum_changed: int = 0
    cache_expired: int = 0
    to_process: int = 0

#==================#
# Class Definition #
#==================#
class NodeRefreshStats(RefreshStats):
    """Model representing refresh statistics aggregated over one node type.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type: ObjectType

#==================#
# Class Definition #
#==================#
class EdgeRefreshStats(RefreshStats):
    """Model representing refresh statistics aggregated over one edge family.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    edge_type_pair: EdgeTypePair

#==================#
# Class Definition #
#==================#
class PropagationStats(BaseModel):
    """Model representing the outcome of one propagation step: how many rows were
    flagged for reprocessing in one cache target.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Logical name of the flagged projection, e.g. a schema-qualified table name.
    target: str
    rows_flagged: int = 0

#==================#
# Class Definition #
#==================#
class TrackingStatus(BaseModel):
    """Model representing the processing-status overview of the airflow
    tracking tables: the per-type counts of pending node and edge change
    records and pending score expiries, as displayed by the status command.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    node_fields_counts  : list[tuple[str, int]] = []
    edge_fields_counts  : list[tuple[str, str, int]] = []
    scores_counts       : list[tuple[str, int]] = []
