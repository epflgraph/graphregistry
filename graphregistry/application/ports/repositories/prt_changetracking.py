# graphregistry/application/ports/repositories/prt_changetracking.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.pipeline.mdl_changetracking import (
    EdgeChangeRecord,
    ObjectChangeRecord,
    ObjectScoreExpiryRecord,
)
from graphregistry.domain.models.pipeline.mdl_policies import ExpirationPolicy, ProcessingScope
from graphregistry.domain.models.pipeline.mdl_stats import EdgeRefreshStats, NodeRefreshStats, PropagationStats, TrackingStatus
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class ChangeTrackingRepository(Protocol):
    """Persistence port for the airflow change-tracking state machine, covering the
    FieldsChanged and ScoresExpired families. Replaces the legacy
    GraphRegistry.Orchestration FieldsChanged and ScoresExpired classes together
    with their Orchestration wrapper methods.
    """

    # Public Method: Insert tracking rows for registry objects and edges that lack
    # them, mirroring the legacy sync command.
    def sync_new_records(self, include_lectures: bool = False, include_ontology: bool = False, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        ...

    # Public Method: Recompute the current checksums of tracked objects and edges
    # from the registry sources and store them on the tracking rows (fields family),
    # mirroring the legacy update_checksums_v2 command.
    def update_current_checksums(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Derive has_changed from the stored checksums and schedule the
    # records that drifted, expired, or were never cached, mirroring the legacy
    # refresh command.
    def refresh_flags(self, scope: ProcessingScope, limit_per_type: int | None = None, actions: ActionSet = ('commit',)) -> list[NodeRefreshStats | EdgeRefreshStats]:
        ...

    # Public Method: Set the staleness flag on tracking records according to the
    # expiration policy and the processing scope; count_only evaluates the
    # affected rows without writing.
    def apply_expiration(self, policy: ExpirationPolicy, scope: ProcessingScope, count_only: bool = False, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        ...

    # Public Method: Close the drift window by adopting the current checksums as the
    # previous ones, mirroring the legacy rollover command.
    def rollover_checksums(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Stamp the cache date of the processed records, mirroring the
    # legacy update_dates command.
    def update_cache_dates(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Clear the to_process flags of the tracked records in scope.
    def reset_flags(self, scope: ProcessingScope, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Clear the processing, drift, and staleness flags of all
    # tracking records unconditionally, mirroring the airflow option of the
    # legacy Orchestration.reset command.
    def clear_all_flags(self, clear_has_expired: bool = True, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Retrieve the change-tracking record of a single node.
    def get_node_record(self, key: NodeKey) -> ObjectChangeRecord | None:
        ...

    # Public Method: Retrieve the change-tracking record of a single edge.
    def get_edge_record(self, key: EdgeKey) -> EdgeChangeRecord | None:
        ...

    # Public Method: Retrieve the score-expiry record of a single node.
    def get_score_expiry_record(self, key: NodeKey) -> ObjectScoreExpiryRecord | None:
        ...

    # Public Method: Retrieve the processing-status overview of the tracking
    # tables, mirroring the display of the legacy status command.
    def get_status(self) -> TrackingStatus:
        ...
