# graphregistry/cli/context.py
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # These imports are only for type checkers / IDE, not at runtime
    from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
    from graphregistry.clients.mysql import GraphDB
    from graphregistry.clients.elasticsearch import GraphES
    from graphregistry.core.registry import GraphRegistry

@dataclass
class CLIContext:
    global_config : "GlobalConfig"
    index_config  : "IndexConfig"
    scores_config : "ScoresConfig"
    db       : "GraphDB"
    index    : "GraphES"
    registry : "GraphRegistry"