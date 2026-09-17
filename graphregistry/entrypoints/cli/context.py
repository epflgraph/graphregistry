# graphregistry/entrypoints/cli/context.py
from __future__ import annotations
from functools import cached_property
from typing import TYPE_CHECKING, Any
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig

# Import heavy types only for type checking to keep runtime startup fast.
if TYPE_CHECKING:
    from graphdb.core.config import GraphDBConfig
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.clients.elasticsearch import GraphES
    from graphregistry.application.core.cor_registry import GraphRegistry

#==================#
# Class Definition #
#==================#
class CLIContext:
    """Shared context for CLI commands.

    GraphDB and Elasticsearch are initialized lazily so commands that do not
    need them start quickly. The registry and AI client are also lazy.
    """

    #----------------------------------------------------------------#
    # Class initialization and dependency injection
    #----------------------------------------------------------------#
    # Public Method: Initialize the shared CLI context with lightweight config objects.
    def __init__(self) -> None:
    #----------------------------------------------------------------#
        # Process-wide configuration objects.
        self.global_config = GlobalConfig()
        self.index_config = IndexConfig()
        self.scores_config = ScoresConfig()
        # GraphDB configuration used to build the database client lazily.
        self.db_config = self._load_db_config()

    # Public Method: Build the GraphDB client lazily from the provided configuration.
    @cached_property
    def db(self) -> "GraphDB":
        from graphregistry.entrypoints.dependencies import build_db

        # Construct and cache the GraphDB client from the stored configuration.
        return build_db(config=self.db_config)

    # Public Method: Build the Elasticsearch client lazily.
    @cached_property
    def es(self) -> "GraphES":
        from graphregistry.adapters.clients.elasticsearch import GraphES

        # Construct and cache the Elasticsearch client.
        return GraphES()

    # Public Method: Build the registry application instance lazily.
    @cached_property
    def registry(self) -> "GraphRegistry":
        from graphregistry.application.core.cor_registry import GraphRegistry

        # Construct and cache the registry orchestrator/cache manager.
        return GraphRegistry()

    # Public Method: Build the GraphAI client module lazily.
    @cached_property
    def ai(self) -> Any:
        import graphai_client as GraphAI

        # Construct and cache the legacy GraphAI client module.
        return GraphAI

    # Internal Function: Load GraphDB configuration from the project config file.
    @staticmethod
    def _load_db_config() -> "GraphDBConfig":
        from graphdb.core.config import GraphDBConfig
        from graphregistry.common.paths import CONFIG_DB_PATH

        # Load GraphDB config so the lazy DB client can be built on demand.
        return GraphDBConfig.from_file(CONFIG_DB_PATH)
