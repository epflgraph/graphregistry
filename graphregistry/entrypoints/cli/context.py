from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
    from graphdb.core.config import GraphDBConfig
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.clients.elasticsearch import GraphES
    from graphregistry.application.core.cor_registry import GraphRegistry


# Define a dataclass to hold shared context for CLI commands
@dataclass
class CLIContext:
    global_config: "GlobalConfig"
    index_config: "IndexConfig"
    scores_config: "ScoresConfig"
    db: "GraphDB"
    db_config: "GraphDBConfig"
    es: "GraphES"
    registry: "GraphRegistry | None"
    ai: "Any | None"
    graphai_auth_token: "dict[str, Any] | None"
