# graphregistry/cli/main.py
import sys, argparse
from graphregistry.cli.context           import CLIContext
from graphregistry.cli.register          import register
from graphregistry.common.config         import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.clients.mysql         import GraphDB
from graphregistry.clients.elasticsearch import GraphES
from graphregistry.core.registry         import GraphRegistry

# Import GraphAI client
import graphai_client as GraphAI
import graphai_client.client_api
import graphai_client.client as GraphAIClient

#---------------------------------------------------------------------#
# Function to build the main argument parser and register subcommands #
#---------------------------------------------------------------------#
def build_parser() -> argparse.ArgumentParser:

    # Initialize main parser
    parser = argparse.ArgumentParser(
        prog="graphregistry",
        description="GraphRegistry command-line interface (CLI) for managing MySQL, Elasticsearch, registry cache/index pipelines, and Airflow orchestration."
    )

    # Register subparsers for each domain
    subparsers = parser.add_subparsers(dest="domain", required=True)

    # Register commands for each domain
    for cmd_name in ['config', 'db', 'es', 'ai', 'airflow', 'cache', 'index']:
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

    # 🔑 Create shared context once
    global_config = GlobalConfig()
    index_config  = IndexConfig()
    scores_config = ScoresConfig()
    db       = GraphDB()
    index    = GraphES()
    registry = GraphRegistry()
    ai       = GraphAI

    # Login to GraphAI and get auth token
    graphai_auth_token = GraphAIClient.login(global_config.settings['graphai']['client_config_file'])

    # Typecheck ai object
    if not isinstance(ai, type(GraphAI)):
        print("Error: Failed to initialize GraphAI client.")
        return 1

    # Typecheck token
    if not isinstance(graphai_auth_token, dict) or not graphai_auth_token:
        print("Error: Failed to obtain valid GraphAI auth token.")
        return 1

    # Create CLI context
    ctx = CLIContext(
        global_config = global_config,
        index_config  = index_config,
        scores_config = scores_config,
        db       = db,
        index    = index,
        registry = registry,
        ai       = ai, # type: ignore
        graphai_auth_token = graphai_auth_token # type: ignore
    )

    # Attach to args for all subcommands
    args.ctx = ctx

    # Execute the command function and return its exit code
    return args.func(args) or 0
