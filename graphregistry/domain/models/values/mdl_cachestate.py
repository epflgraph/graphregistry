# graphregistry/domain/models/values/mdl_cachestate.py
from __future__ import annotations
from datetime import date
from pydantic import BaseModel

#==================#
# Class Definition #
#==================#
class CacheState(BaseModel):
    """Model representing the processing state of one cache projection relative to its
    source of truth. This is the shared dirty-flag vocabulary of the indexing pipeline:
    every projection (change-tracking record, scores matrix, index table) carries the
    same state, and the pipeline transitions it through clean -> dirty -> recompute ->
    clean cycles.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    # Date of the last successful processing; None means the item was never cached.
    last_date_cached: date | None = None
    # Staleness by policy: the cached copy is too old to trust even without observed
    # drift (set by the expire command on a timer).
    has_expired: bool = False
    # Known staleness (dirty flag): the source of truth has moved and the projection
    # is scheduled for reprocessing (set by refresh and propagate).
    to_process: bool = False

    #----------------#
    # Derived state #
    #----------------#
    # Public Method: Expose the dirty-flag vocabulary alongside the persisted column name.
    @property
    def is_dirty(self) -> bool:
        return self.to_process

    # Public Method: Expose the staleness vocabulary alongside the persisted column name.
    @property
    def is_stale(self) -> bool:
        return self.has_expired

    # Public Method: Check whether the item has never been successfully cached.
    @property
    def never_cached(self) -> bool:
        return self.last_date_cached is None

    #---------------------#
    # Lifecycle methods #
    #---------------------#
    # Public Method: Mark the projection as scheduled for reprocessing, mirroring the
    # propagate and refresh commands of the airflow pipeline.
    def mark_dirty(self) -> None:
        self.to_process = True

    # Public Method: Mark the projection as clean after a successful recompute, clearing
    # the expiry flag and stamping the cache date, mirroring the update-dates command.
    def mark_clean(self, cached_on: date | None = None) -> None:
        if cached_on is None:
            cached_on = date.today()
        self.to_process = False
        self.has_expired = False
        self.last_date_cached = cached_on
