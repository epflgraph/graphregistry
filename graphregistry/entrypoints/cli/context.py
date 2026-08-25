# graphregistry/entrypoints/cli/context.py
from functools import cached_property
from typing import TYPE_CHECKING, Any

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.config import GraphDBConfig
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.clients.elasticsearch import GraphES
    from graphregistry.application.core.cor_registry import GraphRegistry
    from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig


# Define a class to hold shared context for CLI commands.
# GraphDB is initialized lazily so commands that do not need MySQL start quickly.
#================================================================#
# Class Definition                                               #
#================================================================#
class CLIContext:
    """Shared context for CLI commands.

    GraphDB is initialized lazily so commands that do not need MySQL start quickly.
    """

    # Class initialization and dependency injection
    def __init__(
        self,
        *,
        global_config: "GlobalConfig",
        index_config: "IndexConfig",
        scores_config: "ScoresConfig",
        db_config: "GraphDBConfig",
        registry: "GraphRegistry | None",
        ai: "Any | None",
    ) -> None:
        self.global_config = global_config
        self.index_config = index_config
        self.scores_config = scores_config
        self.db_config = db_config
        self.registry = registry
        self.ai = ai

    @cached_property
    def db(self) -> "GraphDB":
        from graphregistry.entrypoints.dependencies import build_db

        return build_db(config=self.db_config)

    @cached_property
    def es(self) -> "GraphES":
        from graphregistry.adapters.clients.elasticsearch import GraphES

        return GraphES()
