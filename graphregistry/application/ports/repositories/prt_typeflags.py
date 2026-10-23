# graphregistry/application/ports/repositories/prt_typeflags.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class TypeFlagsRepository(Protocol):
    """Persistence port for the airflow type flags that scope each pipeline cycle.

    The repository stores node flags per (object type, flag family) and undirected
    edge flags as canonical pairs; the bidirectional row expansion of undirected
    families is an adapter concern. Replaces the legacy
    GraphRegistry.Orchestration.TypeFlags class.
    """

    # Public Method: Load the full activation state of the type flags.
    def load(self) -> TypeFlagConfig:
        ...

    # Public Method: Save the full activation state, replacing the previous
    # configuration after a reset, as the legacy config command does.
    def save(self, config: TypeFlagConfig, actions: ActionSet = ('commit',)) -> None:
        ...

    # Public Method: Deactivate every node and edge type flag.
    def reset(self, actions: ActionSet = ('commit',)) -> None:
        ...
