# graphregistry/domain/models/pipeline/mdl_changetracking.py
from __future__ import annotations
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.values.mdl_cachestate import CacheState
from graphregistry.domain.models.values.mdl_checksums import ChecksumPair

#==================#
# Class Definition #
#==================#
class ObjectChangeRecord(BaseModel):
    """Model representing the change-tracking record of a single node in the airflow
    FieldsChanged family. It pairs the node identity with its checksum drift state and
    its cache state, and is the unit the refresh, expire, and rollover cycle operates on.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    key: NodeKey
    checksums: ChecksumPair = Field(default_factory=ChecksumPair)
    state: CacheState = Field(default_factory=CacheState)
    # Soft-deletion marker for retired tracking rows (object removed from the registry).
    deleted: bool = False

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Expose checksum drift as a direct predicate on the record.
    @property
    def has_changed(self) -> bool:
        return self.checksums.has_changed

    # Public Method: Decide whether this record must be scheduled for processing,
    # following the refresh rule: detected drift, policy expiry, or never cached.
    @property
    def requires_processing(self) -> bool:
        return self.has_changed or self.state.has_expired or self.state.never_cached

#==================#
# Class Definition #
#==================#
class EdgeChangeRecord(BaseModel):
    """Model representing the change-tracking record of a single edge in the airflow
    FieldsChanged family. It pairs the edge identity (including context) with its
    checksum drift state and its cache state.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    key: EdgeKey
    checksums: ChecksumPair = Field(default_factory=ChecksumPair)
    state: CacheState = Field(default_factory=CacheState)
    # Soft-deletion marker for retired tracking rows (edge removed from the registry).
    deleted: bool = False

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Expose checksum drift as a direct predicate on the record.
    @property
    def has_changed(self) -> bool:
        return self.checksums.has_changed

    # Public Method: Decide whether this record must be scheduled for processing,
    # following the refresh rule: detected drift, policy expiry, or never cached.
    @property
    def requires_processing(self) -> bool:
        return self.has_changed or self.state.has_expired or self.state.never_cached

#==================#
# Class Definition #
#==================#
class ObjectScoreExpiryRecord(BaseModel):
    """Model representing the score-expiry tracking record of a single node in the
    airflow ScoresExpired family. Score staleness is time-based only: no checksums are
    kept for scores, so the record carries just the cache state.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    key: NodeKey
    state: CacheState = Field(default_factory=CacheState)
    # Soft-deletion marker for retired tracking rows (object removed from the registry).
    deleted: bool = False

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Decide whether this record must be scheduled for re-scoring,
    # following the refresh rule for scores: policy expiry or never cached.
    @property
    def requires_processing(self) -> bool:
        return self.state.has_expired or self.state.never_cached
