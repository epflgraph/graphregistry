# graphregistry/entrypoints/cli/common.py
from __future__ import annotations
from typing import Annotated
import typer
from graphdb.core.config import GraphDBConfig
from graphregistry.common.paths import CONFIG_DB_PATH

#================================================================#
# Function Group: Environment option helpers                     #
#================================================================#

# Internal Function: Load the default environment from the GraphDB config file.
def _default_env() -> str:
    """Return the default environment declared in config-graphdb.yml."""
    cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    return cfg.default_env

# Internal Function: Return the list of valid environment names.
def _env_choices() -> list[str]:
    """Return the environment names declared in config-graphdb.yml."""
    cfg = GraphDBConfig.from_file(CONFIG_DB_PATH)
    return list(cfg.environments.keys())

# Process-wide default environment, loaded once.
DEFAULT_ENV = _default_env()

# Public Method: Return the shared --env option declaration.
def EnvOption() -> typer.Option:
    """Return the shared --env option declaration."""
    return typer.Option(
        "--env",
        help            = "Specify environment.",
        show_choices    = True,
        autocompletion  = _env_choices,
        rich_help_panel = "Common",
    )

# Public Method: Return the shared --verbose option declaration.
def VerboseOption() -> typer.Option:
    """Return the shared --verbose option declaration."""
    return typer.Option(
        "--verbose",
        "-v",
        help            = "Display detailed output.",
        rich_help_panel = "Common",
    )

# Convenience type alias for the common environment annotation.
EnvArg = Annotated[str, EnvOption()]
