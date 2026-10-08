# graphregistry/examples/adapters/genai/lecture_enrich/example.py
"""Example: enrich one lecture through the chunked GenAI enrichment gateway.

Takes a lecture id as input, builds the enrichment task from the MySQL
repository, and runs the full pipeline (chunked map/reduce enrichment plus
Wikipedia post-validation), printing the resulting LectureEnrichmentResult
with rich.
"""
from __future__ import annotations
import argparse
import json
import pickle
from graphdb.core.config import GraphDBConfig
from rich import print as rich_print
from rich.table import Table
from rich.tree import Tree
from graphregistry.adapters.gateways.genai.gtw_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.common.config import GlobalConfig
from graphregistry.common.paths import CONFIG_DB_PATH, CONFIG_MODELS_PATH, REPO_ROOT
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult
from graphregistry.entrypoints.cli.dependencies import build_lecture_operations, build_schema_resolver
from graphregistry.entrypoints.dependencies import build_db

# Output folder for the pickled results, one file per lecture id.
OUTPUT_DIR = REPO_ROOT / "data" / "branches" / "genai_tables_and_adapters" / "lecture_enrich_adapter"

# Public Function: Parse the command-line arguments.
def parse_args() -> argparse.Namespace:
    """Parse the lecture id and the optional registry environment."""
    parser = argparse.ArgumentParser(
        description="Enrich one lecture through the chunked GenAI gateway and print the result with rich.",
    )
    parser.add_argument("lecture_id", help="the lecture identifier to enrich")
    parser.add_argument(
        "--env",
        default = None,
        help    = "registry environment (defaults to the environment declared in config-graphdb.yml)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action  = "store_true",
        help    = "print the resolved environment/schema and the full LLM prompts",
    )
    parser.add_argument(
        "--model",
        default = None,
        help    = "model NAME from config_models.json (defaults to the config's default-model)",
    )
    parser.add_argument(
        "--max-keyframes-per-chunk",
        default = None,
        type    = int,
        help    = "per-chunk keyframe cap (defaults to 50); raise it to exploit bigger-context models",
    )
    parser.add_argument(
        "--json",
        action  = "store_true",
        help    = "print the raw json export instead of the rich tree",
    )
    return parser.parse_args()

# Public Function: Build a rich tree rendering of the enriched lecture result.
def render_result(result: LectureEnrichmentResult) -> Tree:
    """Render the enriched lecture as a rich tree: metadata, top concepts, keyframes."""
    tree = Tree(f"[bold green]🎓 Lecture[/] [cyan]{result.lecture_id}[/] — [bold]{result.title}[/]")

    # Metadata branch: the lecture-level descriptions from the reduce call.
    meta = tree.add("[bold]📝 Metadata[/]")
    meta.add(f"[bold]short :[/] {result.short_description}")
    meta.add(f"[bold]medium:[/] {result.medium_description}")
    meta.add(f"[bold]long  :[/] {result.long_description}")

    # Top concepts branch: the strict ontology matches with their ids, plus
    # the fuzzy wikify suggestions with their suggested scores.
    strict = result.top_concepts.ontology_strict_list.item_list
    fuzzy = result.top_concepts.ontology_fuzzy_list.item_list
    concepts = tree.add(
        f"[bold]🧬 Top concepts[/] [dim]({len(strict)} strict, {len(fuzzy)} fuzzy)[/]"
    )
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("#", justify="right", style="dim")
    table.add_column("concept")
    table.add_column("wiki id", style="dim")
    table.add_column("match", justify="center")
    table.add_column("score", justify="right")
    for i, scored in enumerate(strict, 1):
        table.add_row(str(i), scored.concept.name, scored.concept.id, "[green]strict[/]", "1")
    for i, scored in enumerate(fuzzy, len(strict) + 1):
        table.add_row(str(i), scored.concept.name, scored.concept.id, "[yellow]fuzzy[/]", f"{scored.score:g}")
    concepts.add(table)

    # Keyframes branch: one compact line per keyframe with its strict concepts.
    keyframes = tree.add(f"[bold]🖼️ Keyframes[/] [dim]({len(result.keyframes)})[/]")
    for keyframe in result.keyframes:
        names = [scored.concept.name for scored in keyframe.refined_concepts.ontology_strict_list.item_list]
        fuzzy_count = len(keyframe.refined_concepts.ontology_fuzzy_list.item_list)
        shown = ", ".join(names[:8]) + (", …" if len(names) > 8 else "")
        label = shown if names else "[dim]no strict matches[/]"
        keyframes.add(f"[cyan]{keyframe.keyframe_id}[/] [dim]· {len(names)} strict, {fuzzy_count} fuzzy:[/] {label}")

    # Return the tree so the caller prints it with rich.
    return tree

# Public Function: Run the enrichment pipeline for one lecture and print the result.
def main() -> None:
    args = parse_args()

    # Resolve the environment: an explicit --env wins, otherwise the config default.
    db_config = GraphDBConfig.from_file(CONFIG_DB_PATH)
    env = args.env or db_config.default_env

    # Load the GenAI model registry and select the model: an explicit --model
    # name wins, otherwise the config's default-model is respected. The
    # selected model's max-tokens drives the chunk budget.
    models_config = json.loads(CONFIG_MODELS_PATH.read_text(encoding="utf-8"))
    model_display_name = args.model or models_config["default-model"]
    model_entry = models_config["llm-models"].get(model_display_name)
    if model_entry is None:
        available = ", ".join(sorted(models_config["llm-models"]))
        rich_print(f"[red]Unknown model name [bold]{model_display_name}[/bold]. Available: {available}[/red]")
        raise SystemExit(1)

    # Verbose diagnostics: show where the lecture node will be read from, the
    # selected model, and which other environments exist (a wrong environment
    # is the usual reason for a not-found lecture).
    if args.verbose:
        resolver = build_schema_resolver(engine_name=env, global_config=GlobalConfig())
        engine_name, schema_name = resolver.for_node(NodeKey(object_type='Lecture', object_id=args.lecture_id))
        rich_print(f"[bold]environment[/bold] = {env}   [bold]engine[/bold] = {engine_name}   [bold]lecture schema[/bold] = {schema_name}")
        rich_print(
            f"[bold]model[/bold] = {model_display_name}   "
            f"[bold]serving name[/bold] = {model_entry['model-name']}   "
            f"[bold]max-tokens[/bold] = {model_entry['max-tokens']}"
        )
        rich_print(f"[bold]available environments[/bold] = {list(db_config.environments)}")

    # Wire the lecture operations through the dependency builders; the
    # concept-detection gateway is required by the post-validation step, and
    # the enrichment gateway is pre-built from the selected model's config.
    ops = build_lecture_operations(
        db                         = build_db(config=db_config),
        engine_name                = env,
        global_config              = GlobalConfig(),
        include_video_gateway      = False,
        include_concept_gateway    = True,
        lecture_enrichment_gateway = GenAILectureEnrichmentGateway(
            llm_model               = model_entry["model-name"],
            max_context_length      = model_entry["max-tokens"],
            max_keyframes_per_chunk = args.max_keyframes_per_chunk,
        ),
    )

    # Run the full enrichment: task from the repo, parallel chunk map calls,
    # one reduce call, then post-validation of every refined concept.
    result = ops.enrich(args.lecture_id, verbose=args.verbose)
    if result is None:
        rich_print(
            f"[red]Enrichment produced no result for lecture_id=[bold]{args.lecture_id}[/bold] "
            f"(not found, skipped, or failed in environment [bold]{env}[/bold]).[/red]"
        )
        rich_print(
            "[dim]hint: a wrong environment is the usual cause - try another one with --env; "
            "-v shows the resolved schema and the full LLM prompts.[/dim]"
        )
        raise SystemExit(1)

    # Save the raw result as a pickle (one file per lecture id) so runs can be
    # diffed offline against baselines without touching the database again.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{args.lecture_id}.pkl"
    with open(output_path, "wb") as f:
        pickle.dump(result, f)
    rich_print(f"[dim]saved {output_path}[/dim]")

    # Print the result: the rich tree by default, the raw json with --json.
    if args.json:
        rich_print(result.to_json())
    else:
        rich_print(result)

# Run the example when executed directly.
if __name__ == "__main__":
    main()
