# graphregistry/cli/context.py
from dataclasses import dataclass
from typing import TYPE_CHECKING

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
    from graphregistry.clients.mysql import GraphDB
    from graphregistry.clients.elasticsearch import GraphES
    from graphregistry.core.registry import GraphRegistry
    import graphai_client as GraphAI

# Define a dataclass to hold shared context for CLI commands
@dataclass
class CLIContext:
    global_config : "GlobalConfig"
    index_config  : "IndexConfig"
    scores_config : "ScoresConfig"
    db       : "GraphDB"
    index    : "GraphES"
    registry : "GraphRegistry"
    ai       : "GraphAI" # type: ignore
    graphai_auth_token : str