# simulate_cli_help.py
"""Simulate the help output of the proposed Typer CLI using Rich.

Run with:
    python simulate_cli_help.py
"""
from __future__ import annotations

from datetime import date

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()

DEFAULT_ENV = "test"
TODAY = date.today().isoformat()


def _make_options_table(options: list[tuple[str, str, str]]) -> Table:
    """Build a Rich table that mimics Typer's options panel."""
    table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    table.add_column(style="cyan", no_wrap=True)
    table.add_column(style="default", ratio=1)
    for flag, help_text, default in options:
        row_text = help_text
        if default:
            row_text += f" [dim][default: {default}][/dim]"
        table.add_row(flag, row_text)
    return table


def _make_subcommands_table(subcommands: list[tuple[str, str]]) -> Table:
    """Build a Rich table that mimics Typer's commands panel."""
    table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
    table.add_column(style="green", no_wrap=True)
    table.add_column(style="default", ratio=1)
    for name, help_text in subcommands:
        table.add_row(name, help_text)
    return table


def show_help(
    *,
    usage: str,
    description: str,
    options: list[tuple[str, str, str]] | None = None,
    common: list[tuple[str, str, str]] | None = None,
    subcommands: list[tuple[str, str]] | None = None,
    arguments: list[tuple[str, str]] | None = None,
) -> None:
    """Print one simulated help screen.

    Command-specific options are rendered under "Options"; shared CLI flags
    (--env, --verbose, --help) are rendered under "Common".
    """
    header = Text()
    header.append(f"Usage: {usage}\n\n", style="bold")
    header.append(description)

    panels: list[Panel] = []
    if arguments:
        arg_table = Table(box=box.ROUNDED, show_header=False, padding=(0, 2))
        arg_table.add_column(style="yellow", no_wrap=True)
        arg_table.add_column(style="default", ratio=1)
        for name, help_text in arguments:
            arg_table.add_row(name, help_text)
        panels.append(Panel(arg_table, title="Arguments", title_align="left", border_style="blue"))

    if options:
        panels.append(Panel(_make_options_table(options), title="Options", title_align="left", border_style="blue"))

    if common:
        panels.append(Panel(_make_options_table(common), title="Common", title_align="left", border_style="blue"))

    if subcommands:
        panels.append(
            Panel(_make_subcommands_table(subcommands), title="Commands", title_align="left", border_style="blue")
        )

    console.print(Panel(header, border_style="blue"))
    for panel in panels:
        console.print(panel)
    console.print()


def common_options() -> list[tuple[str, str, str]]:
    """Return the options shared by most commands."""
    return [
        ("--env", "Specify environment.", DEFAULT_ENV),
        ("--verbose", "Display detailed output.", ""),
        ("--help, -h", "Show this message and exit.", ""),
    ]


def main() -> None:
    # Top-level help
    show_help(
        usage="graphregistry [OPTIONS] COMMAND [ARGS]...",
        description="GraphRegistry CLI for managing MySQL, ElasticSearch, registry cache/index pipelines, and airflow orchestration.",
        options=[
            ("--install-completion", "Install completion for the current shell.", ""),
            ("--show-completion", "Show completion for the current shell, to copy or customize.", ""),
            ("--help, -h", "Show this message and exit.", ""),
        ],
        subcommands=[
            ("test", "Run a safe smoke test of configuration and service connectivity."),
            ("config", "Inspect and validate Registry configuration files."),
            ("data", "Manage base registry data."),
            ("airflow", "Manage airflow orchestrator operations."),
            ("ai", "Interact with GraphAI API."),
            ("kgraph", "High-level knowledge-graph construction workflows."),
            ("devtools", "Low-level building blocks for advanced users."),
        ],
    )

    # test
    show_help(
        usage="graphregistry test [OPTIONS]",
        description="Run a safe smoke test of configuration and service connectivity.",
        options=[
            ("--db", "Execute only/also database connectivity check.", ""),
            ("--es", "Execute only/also ElasticSearch connectivity check.", ""),
            ("--ai", "Execute only/also GraphAI connectivity check.", ""),
            ("--gen", "Execute only/also GenAI connectivity check.", ""),
        ],
        common=[
            ("--env", "Specify environment.", DEFAULT_ENV),
            ("--help, -h", "Show this message and exit.", ""),
        ],
    )

    print('\n\n\n\n', "graphregistry config", '\n\n\n\n\n')

    # config
    show_help(
        usage="graphregistry config [OPTIONS] COMMAND [ARGS]...",
        description="Inspect and validate Registry configuration files.",
        options=[("--help, -h", "Show this message and exit.", "")],
        subcommands=[
            ("validate", "Validate configuration files and their structure."),
            ("show", "Pretty-print configuration."),
        ],
    )

    show_help(
        usage="graphregistry config show [OPTIONS]",
        description="Display Registry configuration.",
        options=[
            ("--registry", "Show only/also registry configuration.", ""),
            ("--api", "Show only/also API allowed types.", ""),
            ("--scores", "Show only/also scoring configuration.", ""),
            ("--index", "Show only/also index configuration.", ""),
        ],
        common=[
            ("--env", "Specify environment.", DEFAULT_ENV),
            ("--help, -h", "Show this message and exit.", ""),
        ],
    )

    show_help(
        usage="graphregistry config validate [OPTIONS]",
        description="Validate configuration files and their structure.",
        options=[("--strict", "Treat missing optional configs as warnings.", "")],
        common=common_options(),
    )

    print('\n\n\n\n', "graphregistry data", '\n\n\n\n\n')

    # data
    show_help(
        usage="graphregistry data [OPTIONS] COMMAND [ARGS]...",
        description="Manage base registry data.",
        common=common_options(),
        subcommands=[
            ("list", "List existing node(s) or edge(s)."),
            ("exists", "Check if node(s) or edge(s) exist in the registry."),
            ("get", "Fetch node(s) or edge(s) from the registry."),
            ("save", "Save node(s) or edge(s) from a JSON file."),
            ("delete", "Delete node(s) or edge(s) from the registry."),
        ],
    )

    show_help(
        usage="graphregistry data list [OPTIONS] JSON_FILE",
        description="List existing nodes or edges from a JSON request file.",
        arguments=[("JSON_FILE", "Path to the JSON request file.")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry data exists [OPTIONS] JSON_FILE",
        description="Check if node(s) or edge(s) exist in the registry.",
        arguments=[("JSON_FILE", "Path to the JSON request file.")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry data get [OPTIONS] JSON_FILE",
        description="Fetch node(s) or edge(s) from the registry.",
        arguments=[("JSON_FILE", "Path to the JSON request file.")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry data save [OPTIONS] JSON_FILE",
        description="Save node(s) or edge(s) from a JSON file.",
        arguments=[("JSON_FILE", "Path to the JSON save file.")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry data delete [OPTIONS] JSON_FILE",
        description="Delete node(s) or edge(s) from the registry.",
        arguments=[("JSON_FILE", "Path to the JSON request file.")],
        common=common_options(),
    )

    print('\n\n\n\n', "graphregistry airflow", '\n\n\n\n\n')

    # airflow
    show_help(
        usage="graphregistry airflow [OPTIONS] COMMAND [ARGS]...",
        description="Manage airflow orchestrator operations.",
        common=common_options(),
        subcommands=[
            ("sync", "Sync new data (Registry -> Airflow)."),
            ("config", "Setup the object and content types to process."),
            ("expire", "Mark objects as expired based on last cached date."),
            ("plan", "Generate execution plan based on airflow conditions."),
            ("status", "Display airflow execution plan."),
            ("rollover", "Rollover states after a processing cycle."),
        ],
    )

    show_help(
        usage="graphregistry airflow sync [OPTIONS]",
        description="Sync new data (Registry -> Airflow).",
        options=[
            ("--include-lectures", "Also sync lectures objects.", ""),
            ("--include-ontology", "Also sync ontology objects.", ""),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry airflow config [OPTIONS] JSON_FILE",
        description="Setup the object and content types to process.",
        arguments=[("JSON_FILE", "Path to the typeflags JSON file.")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry airflow expire [OPTIONS]",
        description="Mark objects as expired based on last cached date.",
        options=[
            ("--fields", "Include 'fields changed' airflow tables.", ""),
            ("--scores", "Include 'scores expired' airflow table.", ""),
            ("--older-than, -d", "Expire objects last cached more than N days ago.", ""),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry airflow plan [OPTIONS]",
        description="Generate execution plan based on airflow conditions.",
        options=[
            ("--update-checksums, -c", "Recompute and persist checksums before expiring.", ""),
            ("--expire, -e", "Comma-separated scopes to expire: fields,scores.", ""),
            ("--older-than, -d", "Expire objects last cached more than N days ago.", ""),
            ("--limit-per-type, -l", "Maximum number of objects to expire per document type.", ""),
            ("--skip-hard-reset, -shr", "Reset only airflow states; no full pipeline reset.", "")
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry airflow status [OPTIONS]",
        description="Display airflow execution plan.",
        common=common_options(),
    )

    show_help(
        usage="graphregistry airflow rollover [OPTIONS]",
        description="Rollover states (new checksums and expiration dates) after a processing cycle.",
        options=[("--skip-hard-reset, -shr", "Reset only airflow states; no full pipeline reset.", "")],
        common=common_options(),
    )

    print('\n\n\n\n', "graphregistry ai", '\n\n\n\n\n')

    # ai
    show_help(
        usage="graphregistry ai [OPTIONS] COMMAND [ARGS]...",
        description="Execute GraphAI operations.",
        common=common_options(),
        subcommands=[("detect-concepts", "Detect concepts for nodes using GraphAI.")],
    )

    show_help(
        usage="graphregistry ai detect-concepts [OPTIONS]",
        description="Detect concepts for objects active on airflow",
        common=common_options(),
    )

    print('\n\n\n\n', "graphregistry kgraph", '\n\n\n\n\n')

    # kgraph
    show_help(
        usage="graphregistry kgraph [OPTIONS] COMMAND [ARGS]...",
        description="Knowledge Graph construction workflows.",
        common=common_options(),
        subcommands=[
            ("compute", "Execute graph (re)computation formulas."),
            ("patch", "(Re)build and patch database for Graph Search."),
            ("prune", "Prune the knowledge graph."),
            ("index", "Generate graph index for ElasticSearch."),
        ],
    )

    show_help(
        usage="graphregistry kgraph compute [OPTIONS]",
        description=(
            "Execute cache formulas and update the scores matrix. "
            "Equivalent to:\n"
            "  graphregistry cache update --formulas reset,fields,views,traversals,scores --actions commit\n"
            "  graphregistry cache update --matrix --actions commit"
        ),
        common=common_options(),
    )

    show_help(
        usage="graphregistry kgraph patch [OPTIONS]",
        description=(
            "Build and patch index field tables. "
            "Equivalent to:\n"
            "  graphregistry index build --actions commit,eval\n"
            "  graphregistry index patch --actions commit,eval"
        ),
        common=common_options(),
    )

    show_help(
        usage="graphregistry kgraph prune [OPTIONS]",
        description="Prune orphan nodes, loose edges, and small islands from the knowledge graph.",
        common=common_options(),
    )

    show_help(
        usage="graphregistry kgraph index [OPTIONS]",
        description="Generate graph index for ElasticSearch.",
        options=[
            ("--index-name, -n", "Index name on ElasticSearch.", f"graphsearch_test_{TODAY}"),
            ("--replace-existing, -r", "Replace existing local cache and index files.", ""),
            ("--force-replace, -f", "Force replace without prompting.", ""),
            ("--from-cache, -c", "Import index file from existing local cache.", ""),
            ("--ignore-warnings, -i", "Ignore warning messages.", ""),
        ],
        common=common_options(),
    )

    print('\n\n\n\n', "graphregistry devtools", '\n\n\n\n\n')

    # devtools
    show_help(
        usage="graphregistry devtools [OPTIONS] COMMAND [ARGS]...",
        description="Low-level building blocks for advanced users.",
        common=common_options(),
        subcommands=[
            ("data-save", "Save node(s) or edge(s) from a JSON file."),
            ("data-delete", "Delete node(s) or edge(s) from a JSON file."),
            ("airflow-inspect-to-process", "Inspect or reset 'to_process' flags in airflow tables."),
            ("airflow-reset", "Reset orchestrator 'to_process' flags."),
            ("airflow-update-checksums", "Update object checksums based on typeflag activation."),
            ("airflow-expire", "Mark objects as expired based on last cached date."),
            ("airflow-refresh", "Refresh 'to_process' flags."),
            ("airflow-propagate", "Propagate 'to_process' flags to cache tables."),
            ("cache-formulas-update", "Execute cache SQL formulas."),
            ("cache-scores-matrix", "Recalculate the scores matrix."),
            ("index-build", "Build up and/or update index field tables."),
            ("index-patch", "Apply vertical and horizontal patching to index tables."),
            ("index-delete-loose-ends", "Delete loose ends from cache/graphsearch tables."),
            ("index-es-generate", "Generate ElasticSearch index files from MySQL."),
            ("index-es-import", "Import an ElasticSearch index from a local folder."),
        ],
    )

    show_help(
        usage="graphregistry devtools data-save [OPTIONS] JSON_FILE",
        description="Save node(s) or edge(s) from a JSON file.",
        arguments=[("JSON_FILE", "Path to the JSON save file.")],
        options=[("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools data-delete [OPTIONS] JSON_FILE",
        description="Delete node(s) or edge(s) from a JSON file.",
        arguments=[("JSON_FILE", "Path to the JSON request file.")],
        options=[("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-inspect-to-process [OPTIONS]",
        description="Inspect or reset the airflow 'to_process' flags across airflow tables.",
        options=[
            ("--count", "Count rows with to_process=1 in each airflow table.", ""),
            ("--reset", "Set to_process=0 for all rows currently marked to_process=1.", ""),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-reset [OPTIONS]",
        description="Reset orchestrator 'to_process' flags across Registry tables.",
        options=[
            (
                "--options",
                "Comma-separated options: typeflags,airflow,traversals,cache.",
                "typeflags,airflow",
            )
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-update-checksums [OPTIONS]",
        description="Update object checksums based on typeflag activation.",
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-expire [OPTIONS]",
        description="Mark objects as expired (has_expired=1) based on when they were last cached.",
        options=[
            ("--fields", "Include 'fields changed' airflow tables.", ""),
            ("--scores", "Include 'scores expired' airflow table.", ""),
            ("--types", "Comma-separated object types to restrict expiration to.", "Exercise,Notebook"),
            ("--older-than", "Expire objects last cached more than N days ago.", "300"),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-refresh [OPTIONS]",
        description="Refresh 'to_process' flags based on changed checksums, expired dates, and/or new objects.",
        options=[("--limit-per-type", "Maximum number of objects to refresh per document type.", "1000")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools airflow-propagate [OPTIONS]",
        description="Propagate airflow 'to_process' flags to graph_cache tables.",
        options=[
            ("--fields", "Propagate fields-changed flags only.", ""),
            ("--scores", "Propagate scores-expired flags only.", ""),
            ("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit"),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools cache-formulas-update [OPTIONS]",
        description="Execute cache SQL formulas.",
        options=[
            (
                "--formulas",
                "Comma-separated formulas: reset,fields,views,traversals,scores.",
                "reset,fields,views,traversals,scores",
            ),
            ("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit"),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools cache-scores-matrix [OPTIONS]",
        description="Recalculate the scores matrix.",
        options=[("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools index-build [OPTIONS]",
        description="Build up and/or update index field tables.",
        options=[("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools index-patch [OPTIONS]",
        description="Apply vertical and horizontal patching to all index tables.",
        options=[("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit")],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools index-delete-loose-ends [OPTIONS]",
        description="Delete loose ends from cache and graphsearch index tables.",
        options=[
            ("--use-cache", "Use the cached largest connected graph instead of recalculating.", ""),
            ("--actions", "Comma-separated actions: print,eval,commit.", "print,eval,commit"),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools index-es-generate [OPTIONS]",
        description="Generate ElasticSearch index files from MySQL.",
        options=[
            ("--index-date", "Date of the ElasticSearch index (YYYY-MM-DD).", TODAY),
            ("--ignore-warnings, -i", "Ignore warning messages.", ""),
            ("--replace-existing, -r", "Replace existing local cache and index files.", ""),
            ("--force-replace, -f", "Force replace without prompting.", ""),
            ("--local-cache-only, -lco", "Generate local cache only.", ""),
            ("--index-file-only, -ifo", "Generate index file only (from existing local cache).", ""),
        ],
        common=common_options(),
    )

    show_help(
        usage="graphregistry devtools index-es-import [OPTIONS]",
        description="Import an ElasticSearch index from a local folder.",
        options=[
            ("--input-folder", "Input folder containing the exported index.", ""),
            ("--rename-to", "Rename index to this name on target server.", ""),
            ("--chunk-size", "Number of documents to import per batch.", "1000000"),
            ("--replace-existing, -r", "Replace existing files in the output folder.", ""),
            ("--force, -f", "Force replace without prompting for confirmation.", ""),
        ],
        common=common_options(),
    )


if __name__ == "__main__":
    main()
