# graphregistry/entrypoints/cli/commands/cmd_data.py
from __future__ import annotations
from pathlib import Path
from typing import Annotated, Any
import rich
import typer
from graphregistry.entrypoints.cli.common import DEFAULT_ENV, EnvOption, VerboseOption
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.cli.dependencies import build_registry_operations_from_cli
from graphregistry.entrypoints.mappers import SpecMapper

# Create the Typer sub-app for data commands.
app = typer.Typer(help="Manage base registry data.")

#================================================================#
# Function Group: Input path helpers                             #
#================================================================#

# Internal Function: Find the repository root from a starting path.
def _find_repo_root(start: Path | None = None) -> Path:
    """Find the project root by walking upward until we find a marker."""
    start = (start or Path(__file__)).resolve()

    # Iterate over the collection.
    for parent in [start, *start.parents]:
        if (
            (parent / "graphregistry").is_dir()
            and (
                (parent / "pyproject.toml").exists()
                or (parent / "requirements.txt").exists()
                or (parent / ".git").exists()
            )
        ):
            return parent

    # Raise the encountered error.
    raise RuntimeError(f"Could not find repository root from: {start}")

# Internal Function: Resolve a CLI input path against cwd and repo root.
def _resolve_input_path(path_arg: str) -> Path:
    """Resolve CLI input paths.

    Supports:
      examples/entrypoints/node_save/request.json
      @examples/entrypoints/node_save/request.json
      request.json
      @request.json
      ~/some/file.json
      /absolute/file.json

    Resolution order for relative paths:
      1. current working directory
      2. repository root
    """
    raw = path_arg[1:] if path_arg.startswith("@") else path_arg
    path = Path(raw).expanduser()

    # Handle the conditional case.
    if path.is_absolute():
        return path.resolve(strict=True)

    # Prepare candidates for the following steps.
    candidates = [
        Path.cwd() / path,
        _find_repo_root() / path,
    ]

    # Iterate over the collection.
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve(strict=True)

    # Prepare checked for the following steps.
    checked = "\n".join(f"  - {candidate.resolve(strict=False)}" for candidate in candidates)
    raise FileNotFoundError(
        f"Input file not found: {path_arg}\n"
        f"Checked:\n{checked}"
    )

# Internal Function: Load JSON from a CLI input path.
def _load_json_input(raw_input: str) -> Any:
    """Load JSON from a CLI input path.

    Supports both:
      --node examples/entrypoints/node_save/request.json
      --node @examples/entrypoints/node_save/request.json
    """
    path_str = raw_input[1:] if raw_input.startswith("@") else raw_input
    json_path = _resolve_input_path(path_str)

    # Manage the resource context.
    with json_path.open("r", encoding="utf-8") as fp:
        return __import__("json").load(fp)

#================================================================#
# Function Group: Command handlers                               #
#================================================================#

# Public Method: List existing node(s) or edge(s).
@app.command(name="list")
def cmd_data_list(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON request file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """List existing nodes or edges from a JSON request file."""
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)

    # Continue with the next step.
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)

    # Handle the conditional case.
    if "type" in payload and "from_type" not in payload and "to_type" not in payload:
        node_key_list = node_ops.list(
            object_type = payload["type"],
            id_pattern  = payload.get("id_pattern"),
        )
        if len(node_key_list) > 0:
            rich.print("✅ Node(s) found matching request parameters:")
            for node_key in node_key_list:
                rich.print(node_key)
        else:
            rich.print("❌ No node(s) found matching request parameters.")
    elif "from_type" in payload or "to_type" in payload:
        edge_key_list = edge_ops.list(
            object_type = (payload.get("from_type"), payload.get("to_type")),
            id_pattern  = payload.get("id_pattern"),
        )
        if len(edge_key_list) > 0:
            rich.print("✅ Edge(s) found matching request parameters:")
            for edge_key in edge_key_list:
                rich.print(edge_key)
        else:
            rich.print("❌ No edge(s) found matching request parameters.")
    else:
        raise typer.BadParameter("JSON file must contain 'type' for nodes or 'from_type'/'to_type' for edges.")

# Public Method: Check if node(s) or edge(s) exist in the registry.
@app.command(name="exists")
def cmd_data_exists(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON request file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Check if node(s) or edge(s) exist in the registry."""
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)

    # Handle the conditional case.
    if _is_node_key(payload):
        node_key = SpecMapper.from_node_key_spec(_unwrap_key(payload))
        exists = node_ops.exists(node_key)
        rich.print_json(data={"exists": exists})
    elif _is_edge_key(payload):
        edge_key = SpecMapper.from_edge_key_spec(_unwrap_key(payload))
        exists = edge_ops.exists(edge_key)
        rich.print_json(data={"exists": exists})
    elif _is_node_key_list(payload):
        node_key_list = SpecMapper.from_node_key_list_spec(_unwrap_key_list(payload))
        exists_list = node_ops.exists_many(node_key_list)
        rich.print_json(data={"exist_keys": exists_list, "count": len(exists_list)})
    elif _is_edge_key_list(payload):
        edge_key_list = SpecMapper.from_edge_key_list_spec(_unwrap_key_list(payload))
        exists_list = edge_ops.exists_many(edge_key_list)
        rich.print_json(data={"exist_keys": exists_list, "count": len(exists_list)})
    else:
        raise typer.BadParameter("JSON file must contain a node key, edge key, or key list.")

# Public Method: Fetch node(s) or edge(s) from the registry.
@app.command(name="get")
def cmd_data_get(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON key file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Fetch node(s) or edge(s) from the registry."""
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)

    # Handle the conditional case.
    if _is_node_key(payload):
        node_key = SpecMapper.from_node_key_spec(_unwrap_key(payload))
        node = node_ops.get(node_key)
        if node:
            rich.print_json(data=SpecMapper.to_node_spec(node).model_dump(exclude_none=True))
    elif _is_edge_key(payload):
        edge_key = SpecMapper.from_edge_key_spec(_unwrap_key(payload))
        edge = edge_ops.get(edge_key)
        if edge:
            rich.print_json(data=SpecMapper.to_edge_spec(edge).model_dump(exclude_none=True))
    elif _is_node_key_list(payload):
        node_key_list = SpecMapper.from_node_key_list_spec(_unwrap_key_list(payload))
        node_list = node_ops.get_many(node_key_list)
        if node_list:
            rich.print_json(data=[SpecMapper.to_node_spec(node).model_dump(exclude_none=True) for node in node_list.item_list])
    elif _is_edge_key_list(payload):
        edge_key_list = SpecMapper.from_edge_key_list_spec(_unwrap_key_list(payload))
        edge_list = edge_ops.get_many(edge_key_list)
        if edge_list:
            rich.print_json(data=[SpecMapper.to_edge_spec(edge).model_dump(exclude_none=True) for edge in edge_list.item_list])
    else:
        raise typer.BadParameter("JSON file must contain a node key, edge key, or key list.")

# Public Method: Save node(s) or edge(s) from a JSON file.
@app.command(name="save")
def cmd_data_save(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON save file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Save node(s) or edge(s) from a JSON file."""
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)

    # Handle the conditional case.
    if _is_node(payload):
        node = SpecMapper.from_node_spec(_unwrap_node(payload))
        node_ops.save(node, actions=("commit",))
    elif _is_edge(payload):
        edge = SpecMapper.from_edge_spec(_unwrap_edge(payload))
        edge_ops.save(edge, actions=("commit",))
    elif _is_node_list(payload):
        node_list = SpecMapper.from_node_list_spec(_unwrap_node_list(payload))
        node_ops.save_many(node_list, actions=("commit",))
    elif _is_edge_list(payload):
        edge_list = SpecMapper.from_edge_list_spec(_unwrap_edge_list(payload))
        edge_ops.save_many(edge_list, actions=("commit",))
    else:
        raise typer.BadParameter("JSON file must contain a node, edge, node_list, or edge_list.")

# Public Method: Delete node(s) or edge(s) from the registry.
@app.command(name="delete")
def cmd_data_delete(
    ctx: typer.Context,
    json_file: Annotated[str, typer.Argument(help="Path to the JSON key file.")],
    env: Annotated[str, EnvOption()] = DEFAULT_ENV,
    verbose: Annotated[bool, VerboseOption()] = False,
) -> None:
    """Delete node(s) or edge(s) from the registry."""
    cli_ctx: CLIContext = ctx.obj
    payload = _load_json_input(json_file)
    node_ops, edge_ops = build_registry_operations_from_cli(ctx=cli_ctx, env=env, verbose=verbose)

    # Handle the conditional case.
    if _is_node_key(payload):
        node_key = SpecMapper.from_node_key_spec(_unwrap_key(payload))
        node_ops.delete(node_key, actions=("commit",))
    elif _is_edge_key(payload):
        edge_key = SpecMapper.from_edge_key_spec(_unwrap_key(payload))
        edge_ops.delete(edge_key, actions=("commit",))
    elif _is_node_key_list(payload):
        node_key_list = SpecMapper.from_node_key_list_spec(_unwrap_key_list(payload))
        node_ops.delete_many(node_key_list, actions=("commit",))
    elif _is_edge_key_list(payload):
        edge_key_list = SpecMapper.from_edge_key_list_spec(_unwrap_key_list(payload))
        edge_ops.delete_many(edge_key_list, actions=("commit",))
    else:
        raise typer.BadParameter("JSON file must contain a node key, edge key, or key list.")

#================================================================#
# Function Group: Payload shape detectors                        #
#================================================================#

# Internal Function: Detect a single-node payload shape.
def _is_node(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a single node."""
    keys = set(payload.keys())
    if keys == {"node"}:
        return True
    return "object_type" in payload and "object_id" in payload and "title" in payload

# Internal Function: Detect a single-edge payload shape.
def _is_edge(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a single edge."""
    keys = set(payload.keys())
    if keys == {"edge"}:
        return True
    return "from_object_type" in payload and "to_object_type" in payload and "context" in payload

# Internal Function: Detect a node-list payload shape.
def _is_node_list(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a list of nodes."""
    keys = set(payload.keys())
    if keys == {"node_list"}:
        return True
    if "node_list" in payload and isinstance(payload["node_list"], list):
        return True
    if isinstance(payload, list):
        return len(payload) == 0 or ("object_type" in payload[0] and "title" in payload[0])
    return False

# Internal Function: Detect an edge-list payload shape.
def _is_edge_list(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a list of edges."""
    keys = set(payload.keys())
    if keys == {"edge_list"}:
        return True
    if "edge_list" in payload and isinstance(payload["edge_list"], list):
        return True
    if isinstance(payload, list):
        return len(payload) == 0 or ("from_object_type" in payload[0] and "context" in payload[0])
    return False

# Internal Function: Detect a single node-key payload shape.
def _is_node_key(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a single node key."""
    keys = set(payload.keys())
    if keys == {"key"}:
        inner = payload["key"]
        return isinstance(inner, dict) and "object_type" in inner and "object_id" in inner
    return "object_type" in payload and "object_id" in payload and len(payload) == 2

# Internal Function: Detect a single edge-key payload shape.
def _is_edge_key(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a single edge key."""
    keys = set(payload.keys())
    if keys == {"key"}:
        inner = payload["key"]
        return isinstance(inner, dict) and "from_object_type" in inner and "to_object_type" in inner
    return (
        "from_object_type" in payload
        and "to_object_type" in payload
        and "context" in payload
        and len(payload) == 5
        and "from_object_id" in payload
        and "to_object_id" in payload
    )

# Internal Function: Detect a node-key-list payload shape.
def _is_node_key_list(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a list of node keys."""
    if "key_list" in payload and isinstance(payload["key_list"], list):
        return len(payload["key_list"]) == 0 or (
            isinstance(payload["key_list"][0], dict)
            and "object_type" in payload["key_list"][0]
        )
    if isinstance(payload, list):
        return len(payload) == 0 or ("object_type" in payload[0] and "object_id" in payload[0] and len(payload[0]) == 2)
    return False

# Internal Function: Detect an edge-key-list payload shape.
def _is_edge_key_list(payload: dict[str, Any]) -> bool:
    """Return True if the payload describes a list of edge keys."""
    if "key_list" in payload and isinstance(payload["key_list"], list):
        return len(payload["key_list"]) == 0 or (
            isinstance(payload["key_list"][0], dict)
            and "from_object_type" in payload["key_list"][0]
        )
    if isinstance(payload, list):
        return len(payload) == 0 or (
            "from_object_type" in payload[0]
            and "to_object_type" in payload[0]
            and "context" in payload[0]
        )
    return False

# Internal Function: Unwrap a single-key payload from its optional wrapper.
def _unwrap_key(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the inner key spec, handling optional 'key' wrapper."""
    return payload["key"] if list(payload.keys()) == ["key"] else payload

# Internal Function: Unwrap a key-list payload from its optional wrapper.
def _unwrap_key_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the inner key-list spec, handling optional 'key_list' wrapper."""
    return payload["key_list"] if list(payload.keys()) == ["key_list"] else payload

# Internal Function: Unwrap a single-node payload from its optional wrapper.
def _unwrap_node(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the inner node spec, handling optional 'node' wrapper."""
    return payload["node"] if list(payload.keys()) == ["node"] else payload

# Internal Function: Unwrap a single-edge payload from its optional wrapper.
def _unwrap_edge(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the inner edge spec, handling optional 'edge' wrapper."""
    return payload["edge"] if list(payload.keys()) == ["edge"] else payload

# Internal Function: Unwrap a node-list payload from its optional wrapper.
def _unwrap_node_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the inner node-list spec, handling optional 'node_list' wrapper."""
    return payload["node_list"] if list(payload.keys()) == ["node_list"] else payload

# Internal Function: Unwrap an edge-list payload from its optional wrapper.
def _unwrap_edge_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the inner edge-list spec, handling optional 'edge_list' wrapper."""
    return payload["edge_list"] if list(payload.keys()) == ["edge_list"] else payload
