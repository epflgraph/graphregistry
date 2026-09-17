# graphregistry/entrypoints/cli/main.py
from __future__ import annotations
import typer
from graphregistry.entrypoints.cli.commands import (
    cmd_ai,
    cmd_airflow,
    cmd_config,
    cmd_data,
    cmd_devtools,
    cmd_init,
    cmd_kgraph,
    cmd_test,
)
from graphregistry.entrypoints.cli.context import CLIContext

#==================#
# App Definition   #
#==================#
app = typer.Typer(
    name             = "graphregistry",
    help             = "GraphRegistry CLI for managing MySQL, ElasticSearch, registry cache/index pipelines, and Airflow orchestration.",
    no_args_is_help  = True,
    context_settings = {"help_option_names": ["--help", "-h"]},
)

# Register each domain sub-app under the top-level CLI.
app.add_typer(cmd_config.app, name="config")
app.add_typer(cmd_init.app, name="init")
app.command(name="test")(cmd_test.cmd_test)
app.add_typer(cmd_data.app, name="data")
app.add_typer(cmd_airflow.app, name="airflow")
app.add_typer(cmd_ai.app, name="ai")
app.add_typer(cmd_kgraph.app, name="kgraph")
app.add_typer(cmd_devtools.app, name="devtools")

# Public Function: Build the shared CLI context once per invocation.
@app.callback()
def main_callback(ctx: typer.Context) -> None:
    """Attach the shared CLI context to the Typer invocation context."""
    ctx.obj = CLIContext()

# Public Function: Entry point for the console script.
def main() -> None:
    app()
