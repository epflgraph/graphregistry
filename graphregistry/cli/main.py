# graphregistry/cli/main.py
import sys, argparse
from graphregistry.cli.context           import CLIContext
from graphregistry.cli.register          import register
from graphregistry.common.config         import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.clients.mysql         import GraphDB
from graphregistry.clients.elasticsearch import GraphES
# from graphregistry.core.registry         import GraphRegistry

# Import GraphAI client
import graphai_client as GraphAI
import graphai_client.client_api
import graphai_client.client


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="graphregistry",
        description="GraphRegistry command-line interface."
    )
    subparsers = parser.add_subparsers(dest="domain", required=True)

    for cmd_name in ['ai', 'airflow', 'cache', 'db', 'index']:
        register(subparsers, cmd_name)

    return parser

def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    # 🔑 Create shared context once
    global_config = GlobalConfig()
    index_config  = IndexConfig()
    scores_config = ScoresConfig()
    db       = GraphDB()
    index    = GraphES()
    # registry = GraphRegistry()
    ai       = GraphAI

    # Login to GraphAI and get auth token
    graphai_auth_token = ai.client.login(global_config.settings['graphai']['client_config_file'])

    # Create CLI context
    ctx = CLIContext(
        global_config = global_config,
        index_config  = index_config,
        scores_config = scores_config,
        db    = db,
        index = index,
        ai    = ai,
        graphai_auth_token = graphai_auth_token
    ) #, registry=registry)
    args.ctx = ctx   # attach to args for all subcommands

    return args.func(args) or 0
    # try:
    #     return args.func(args) or 0
    # except KeyboardInterrupt:
    #     print("\nInterrupted by user.", file=sys.stderr)
    #     return 130
    # except Exception as exc:
    #     print(f"❌ ERROR: {exc}", file=sys.stderr)
    #     return 1
    # finally:
    #     # Optionally close resources explicitly if your clients expose it
    #     pass