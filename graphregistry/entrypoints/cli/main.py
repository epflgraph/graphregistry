# graphregistry/entrypoints/cli/main.py
import argparse
from typing import Any

from graphdb.core.config import GraphDBConfig

from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.register import register

#---------------------------------------------------------------------#
# Function to build the main argument parser and register subcommands #
#---------------------------------------------------------------------#
def build_parser() -> argparse.ArgumentParser:

    # Initialize main parser
    parser = argparse.ArgumentParser(
        prog="graphregistry",
        description="GraphRegistry command-line interface (CLI) for managing MySQL, Elasticsearch, registry cache/index pipelines, and Airflow orchestration.",
    )

    # Register subparsers for each domain
    subparsers = parser.add_subparsers(dest="domain", required=True)

    # Register commands for each domain
    for cmd_name in ["config", "es", "ai", "data", "airflow", "cache", "run", "index", "setup"]:
        register(subparsers, cmd_name)

    # Return the fully built parser
    return parser


#-------------------------------------------------------------------------#
# Main function to execute the CLI with proper context and error handling #
#-------------------------------------------------------------------------#
def main(argv=None) -> int:

    # Build the argument parser
    parser = build_parser()

    # Parse the command-line arguments
    args = parser.parse_args(argv)

    # If no command was provided, print help and exit
    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    # Create shared config objects. Heavy clients (MySQL, ES) are initialized lazily.
    global_config = GlobalConfig()
    index_config  = IndexConfig()
    scores_config = ScoresConfig()

    # Load MySQL config; the client itself is initialized lazily by CLIContext.db.
    from graphregistry.common.paths import CONFIG_DB_PATH

    db_config = GraphDBConfig.from_file(CONFIG_DB_PATH)

    # Registry is only required for selected domains.
    registry: Any | None = None
    if args.domain in {"airflow", "cache", "index"}:
        from graphregistry.application.core.cor_registry import GraphRegistry

        registry = GraphRegistry()

    # GraphAI module is only required when an ai subcommand wires a GraphAI gateway.
    # Authentication is handled lazily inside the hexagonal gateway adapters, not here.
    ai: Any | None = None
    if args.domain == "ai":
        import graphai_client as GraphAI

        ai = GraphAI

    # Create CLI context
    ctx = CLIContext(
        global_config=global_config,
        index_config=index_config,
        scores_config=scores_config,
        db_config=db_config,
        registry=registry,
        ai=ai,
    )

    # Attach to args for all subcommands
    args.ctx = ctx

    # Execute the command function and return its exit code
    return args.func(args) or 0
