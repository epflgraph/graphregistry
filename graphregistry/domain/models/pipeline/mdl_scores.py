# graphregistry/domain/models/pipeline/mdl_scores.py
from __future__ import annotations
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.values.mdl_cachestate import CacheState
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair

#==================#
# Class Definition #
#==================#
class ScoredEdge(BaseModel):
    """Model representing one entry of a scores matrix: the undirected semantic
    similarity between two objects, aggregated over the concepts they share. Matrix
    entries carry their own cache state so the pipeline can flag individual edges for
    re-scoring.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Endpoints of the undirected scored relation; matrices do not carry edge context.
    from_key: NodeKey
    to_key: NodeKey
    score: float = Field(ge=0)
    state: CacheState = Field(default_factory=CacheState)
    # Soft-deletion marker for matrix rows whose relation left the graph.
    deleted: bool = False

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Return the edge family this scored edge belongs to.
    @property
    def type_pair(self) -> EdgeTypePair:
        return EdgeTypePair(
            from_object_type = self.from_key.object_type,
            to_object_type   = self.to_key.object_type,
        )

#==================#
# Class Definition #
#==================#
class ScoreConsolidationParams(BaseModel):
    """Model bundling the thresholds applied when scores matrices are calculated and
    consolidated. The defaults mirror the legacy calculate and consolidate commands.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Minimum semantic score for an edge to be kept in the matrix.
    score_threshold: float = 0.1
    # Minimum number of shared concepts for two objects to be considered related.
    min_shared_concepts: int = 4
