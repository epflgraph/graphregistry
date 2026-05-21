# graphregistry/entrypoints/cli/cmd_data.py
from __future__ import annotations
from pathlib import Path
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList, EdgeKey, EdgeKeyList
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.domain.models.entities.mdl_edge import EdgeList
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.factories.fct_node import NodeFactory
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.entrypoints.mappers import SpecMapper
from graphregistry.adapters.services.schema.asv_schema_default import DefaultSchemaResolver
import rich, json
from graphregistry.domain.types import ActionSet, ActionName

# Helper: Build default schema resolver
def _make_schema_resolver(args) -> DefaultSchemaResolver:
    return DefaultSchemaResolver(
        engine_name=args.env,
        glbcfg=args.ctx.global_config,
    )

# Helper: Build node repository
def _make_node_repo(args) -> NodeRepository:
    return MySQLNodeRepository(
        db=args.ctx.db,
        schema_resolver=_make_schema_resolver(args),
    )

# Helper: Build edge repository
def _make_edge_repo(args) -> EdgeRepository:
    return MySQLEdgeRepository(
        db=args.ctx.db,
        schema_resolver=_make_schema_resolver(args),
    )

# Helper: Find repository root
def _find_repo_root(start: Path | None = None) -> Path:
    """
    Find the project root by walking upward until we find a marker.
    """
    start = (start or Path(__file__)).resolve()

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

    raise RuntimeError(f"Could not find repository root from: {start}")

# Helper: Resolve input paths
def _resolve_input_path(path_arg: str) -> Path:
    """
    Resolve CLI input paths.

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

    if path.is_absolute():
        return path.resolve(strict=True)

    candidates = [
        Path.cwd() / path,
        _find_repo_root() / path,
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve(strict=True)

    checked = "\n".join(f"  - {candidate.resolve(strict=False)}" for candidate in candidates)

    raise FileNotFoundError(
        f"Input file not found: {path_arg}\n"
        f"Checked:\n{checked}"
    )

# Helper: Load JSON from file path
def _load_json_input(raw_input: str, label: str):

    # Support both:
    #   --node examples/entrypoints/node_save/request.json
    #   --node @examples/entrypoints/node_save/request.json
    path_str = raw_input[1:] if raw_input.startswith("@") else raw_input

    # Resolve path
    json_path = _resolve_input_path(path_str)

    # Open JSON file and load data
    with json_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)

# Handler: Check if node or edge exists in Registry
def cmd_data_list(args):

    # Fetch input options
    node_request_input = args.node_request
    edge_request_input = args.edge_request

    # Node list requested
    if node_request_input:

        # Load JSON data from input file
        json_input = _load_json_input(node_request_input, "--node_request")

        # Initialize node repository
        node_repo: NodeRepository = _make_node_repo(args)

        # Get list of nodes matching request parameters
        node_key_list = node_repo.list(object_type=json_input['type'], id_pattern=json_input.get('id_pattern'))

        # Print results
        if len(node_key_list)>0:
            rich.print("✅ Node(s) found matching request parameters:")
            for node_key in node_key_list:
                rich.print(node_key)
        else:
            rich.print("❌ No node(s) found matching request parameters.")

    # Edge list requested
    if edge_request_input:

        # Load JSON data from input file
        json_input = _load_json_input(edge_request_input, "--edge_request")

        # Initialize edge repository
        edge_repo: EdgeRepository = _make_edge_repo(args)

        # Get list of edges matching request parameters
        edge_key_list = edge_repo.list(object_type=(json_input.get('from_type'), json_input.get('to_type')), id_pattern=json_input.get('id_pattern'))

        # Print results
        if len(edge_key_list)>0:
            rich.print("✅ Edge(s) found matching request parameters:")
            for edge_key in edge_key_list:
                rich.print(edge_key)
        else:
            rich.print("❌ No edge(s) found matching request parameters.")

# Handler: Check if node or edge exists in Registry
def cmd_data_exists(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    node_key_input      = args.node_key
    edge_key_input      = args.edge_key
    node_key_list_input = args.node_key_list
    edge_key_list_input = args.edge_key_list

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)

    # Process node input
    if node_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(node_key_input, "--node_key")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key = SpecMapper.from_node_key_spec(node_key_spec)

        # Check if node exists
        exists = node_repo.exists(node_key)

        # Print result as JSON
        rich.print_json(data={"exists": exists})

    # Process edge input
    if edge_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(edge_key_input, "--edge_key")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key = SpecMapper.from_edge_key_spec(edge_key_spec)

        # Check if edge exists
        exists = edge_repo.exists(edge_key)

        # Print result as JSON
        rich.print_json(data={"exists": exists})

    # Process node list input
    if node_key_list_input:

        # Load JSON data from input file
        node_list_json_data = _load_json_input(node_key_list_input, "--node_key_list")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_list_spec = node_list_json_data['key_list'] if list(node_list_json_data.keys())==['key_list'] else node_list_json_data

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key_list = SpecMapper.from_node_key_list_spec(node_key_list_spec)

        # Check if nodes exist
        exists_list = node_repo.exists_many(node_key_list)

        # Print result as JSON
        rich.print_json(data={
            "exist_keys": exists_list,
            "count": len(exists_list)
        })

    # Process edge list input
    if edge_key_list_input:

        # Load JSON data from input file
        edge_list_json_data = _load_json_input(edge_key_list_input, "--edge_key_list")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_list_spec = edge_list_json_data['key_list'] if list(edge_list_json_data.keys())==['key_list'] else edge_list_json_data

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key_list = SpecMapper.from_edge_key_list_spec(edge_key_list_spec)

        # Check if edges exist
        exists_list = edge_repo.exists_many(edge_key_list)

        # Print result as JSON
        rich.print_json(data={
            "exist_keys": exists_list,
            "count": len(exists_list)
        })

# Handler: Fetch node or edge from Registry
def cmd_data_get(args):

    # Fetch input options
    node_key_input      = args.node_key
    edge_key_input      = args.edge_key
    node_key_list_input = args.node_key_list
    edge_key_list_input = args.edge_key_list

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)

    # Process node input
    if node_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(node_key_input, "--node_key")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key = SpecMapper.from_node_key_spec(node_key_spec)

        # Fetch node from registry
        node = node_repo.get(node_key)

        # Print node as JSON
        if node:
            rich.print_json(data=SpecMapper.to_node_spec(node).model_dump(exclude_none=True))

    # Process edge input
    if edge_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(edge_key_input, "--edge_key")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key = SpecMapper.from_edge_key_spec(edge_key_spec)

        # Fetch edge from registry
        edge = edge_repo.get(edge_key)

        # Print edge as JSON
        if edge:
            rich.print_json(data=SpecMapper.to_edge_spec(edge).model_dump(exclude_none=True))

    # Process node list input
    if node_key_list_input:

        # Load JSON data from input file
        node_list_json_data = _load_json_input(node_key_list_input, "--node_key_list")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_list_spec = node_list_json_data['key_list'] if list(node_list_json_data.keys())==['key_list'] else node_list_json_data

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key_list = SpecMapper.from_node_key_list_spec(node_key_list_spec)

        # Fetch node from registry
        node_list = node_repo.get_many(node_key_list)

        # Print node as JSON
        if node_list:
            rich.print_json(data=[SpecMapper.to_node_spec(node).model_dump(exclude_none=True) for node in node_list.item_list])

    # Process edge list input
    if edge_key_list_input:

        # Load JSON data from input file
        edge_list_json_data = _load_json_input(edge_key_list_input, "--edge_key_list")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_list_spec = edge_list_json_data['key_list'] if list(edge_list_json_data.keys())==['key_list'] else edge_list_json_data

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key_list = SpecMapper.from_edge_key_list_spec(edge_key_list_spec)

        # Fetch edge from registry
        edge_list = edge_repo.get_many(edge_key_list)

        # Print edge as JSON
        if edge_list:
            rich.print_json(data=[SpecMapper.to_edge_spec(edge).model_dump(exclude_none=True) for edge in edge_list.item_list])

# Handler: Insert node or edge in Registry
def cmd_data_save(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    node_input      = args.node
    edge_input      = args.edge
    node_list_input = args.node_list
    edge_list_input = args.edge_list
    # subgraph_input  = args.subgraph
    actions         = tuple(args.actions.split(',')) if args.actions else ()

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)
    node_ops = NodeOperations(repo=node_repo)
    edge_ops = EdgeOperations(repo=edge_repo)

    # Process node input
    if node_input:

        # Load JSON data from input file
        node_json_data = _load_json_input(node_input, "--node")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_spec = node_json_data['node'] if list(node_json_data.keys())==['node'] else node_json_data

        # Create node object from JSON data using factory to leverage concept detection if requested
        node = SpecMapper.from_node_spec(node_spec)

        # Insert node into registry
        node_ops.save(node, actions=actions)

    # Process edge input
    if edge_input:

        # Load JSON data from input file
        edge_json_data = _load_json_input(edge_input, "--edge")

        # Handle case where user passes in a full node spec with "edge" wrapper vs just the edge spec directly
        edge_spec = edge_json_data['edge'] if list(edge_json_data.keys())==['edge'] else edge_json_data

        # Create edge object from JSON data
        edge = SpecMapper.from_edge_spec(edge_spec)

        # Insert edge into registry
        edge_ops.save(edge, actions=actions)

    # Process node list input
    if node_list_input:

        # Load JSON data from input file
        node_list_json_data = _load_json_input(node_list_input, "--node_list")

        # Handle case where user passes in a full node spec with "node_list" wrapper vs just the node list spec directly
        node_list_spec = node_list_json_data['node_list'] if list(node_list_json_data.keys())==['node_list'] else node_list_json_data

        # Create node list object from JSON data
        node_list = SpecMapper.from_node_list_spec(node_list_spec)

        # Insert node list into registry
        node_ops.save_many(node_list, actions=actions)

    # Process edge list input
    if edge_list_input:

        # Load JSON data from input file
        edge_list_json_data = _load_json_input(edge_list_input, "--edge_list")

        # Handle case where user passes in a full edge list spec with "edge_list" wrapper vs just the edge list spec directly
        edge_list_spec = edge_list_json_data['edge_list'] if list(edge_list_json_data.keys())==['edge_list'] else edge_list_json_data

        # Create edge list object from JSON data
        edge_list = SpecMapper.from_edge_list_spec(edge_list_spec)

        # Insert edge list into registry
        edge_ops.save_many(edge_list, actions=actions)

# Handler: Delete node or edge from Registry
def cmd_data_delete(args):

    # Fetch context objects
    actions: ActionSet = tuple(args.actions.split(',')) if args.actions else ()

    # Fetch input options
    node_key_input      = args.node_key
    edge_key_input      = args.edge_key
    node_key_list_input = args.node_key_list
    edge_key_list_input = args.edge_key_list

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)

    # Process node input
    if node_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(node_key_input, "--node_key")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key = SpecMapper.from_node_key_spec(node_key_spec)

        # Delete node from registry
        result = node_repo.delete(node_key, actions=actions)

        # Print result as JSON
        if result==False and 'commit' not in actions:
            print("⚠️  Node not deleted. To delete, add 'commit' to actions.")

    # Process edge input
    if edge_key_input:

        # Load JSON data from input file
        json_input = _load_json_input(edge_key_input, "--edge_key")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_spec = json_input['key'] if list(json_input.keys())==['key'] else json_input

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key = SpecMapper.from_edge_key_spec(edge_key_spec)

        # Delete edge from registry
        result = edge_repo.delete(edge_key, actions=actions)

        # Print result as JSON
        if result==False and 'commit' not in actions:
            print("⚠️  Edge not deleted. To delete, add 'commit' to actions.")

    # Process node list input
    if node_key_list_input:

        # Load JSON data from input file
        node_list_json_data = _load_json_input(node_key_list_input, "--node_key_list")

        # Handle case where user passes in a full node spec with "node" wrapper vs just the node spec directly
        node_key_list_spec = node_list_json_data['key_list'] if list(node_list_json_data.keys())==['key_list'] else node_list_json_data

        # Create node object from JSON data using factory to leverage concept detection if requested
        node_key_list = SpecMapper.from_node_key_list_spec(node_key_list_spec)

        # Delete nodes from registry
        result = node_repo.delete_many(node_key_list, actions=actions)

        # Print result as JSON
        if result==False and 'commit' not in actions:
            print("⚠️  Node list not deleted. To delete, add 'commit' to actions.")

    # Process edge list input
    if edge_key_list_input:

        # Load JSON data from input file
        edge_list_json_data = _load_json_input(edge_key_list_input, "--edge_key_list")

        # Handle case where user passes in a full edge spec with "edge" wrapper vs just the edge spec directly
        edge_key_list_spec = edge_list_json_data['key_list'] if list(edge_list_json_data.keys())==['key_list'] else edge_list_json_data

        # Create edge object from JSON data using factory to leverage concept detection if requested
        edge_key_list = SpecMapper.from_edge_key_list_spec(edge_key_list_spec)

        # Delete edges from registry
        result = edge_repo.delete_many(edge_key_list, actions=actions)

        # Print result as JSON
        if result==False and 'commit' not in actions:
            print("⚠️  Edge list not deleted. To delete, add 'commit' to actions.")

# Handler: Import data from JSON file into Registry
def cmd_data_import(args):
    """
    Handle:
      graphregistry data import [...]
    """

    # Fetch context objects
    registry = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import data from json file.")

    # Get input options
    input_file      = args.input_file
    import_method   = args.import_method
    actions         = tuple(args.actions.split(','))  if args.actions  else ()
    detect_concepts = args.detect_concepts

    # -----------------#
    # Execute commands #
    # -----------------#

    # Open JSON sample set
    with open(input_file, 'r', encoding='utf-8') as fp:
        sample_set = json.load(fp)

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)
    node_ops = NodeOperations(repo=node_repo)
    edge_ops = EdgeOperations(repo=edge_repo)

    # Method 1: Process and commit object by object
    if import_method == 'object':

        # Process nodes
        for node_json in sample_set.get('nodes', []):
            node = SpecMapper.from_save_request(node_json)
            node_ops.save(node, actions=actions)

            if detect_concepts:
                print(f"⚠️  detect_concepts requested but not yet wired into the new CLI workflow for node {node.key.to_tuple()}.")

        # Process edges
        for edge_json in sample_set.get('edges', []):
            edge = SpecMapper.from_save_request(edge_json)
            edge_ops.save(edge, actions=actions)

    # Method 2: Process and commit as list of objects
    elif import_method == 'list':

        # Process nodes list
        node_list = NodeList(
            item_list=[
                SpecMapper.from_save_request(node_json)
                for node_json in sample_set.get('nodes', [])
            ]
        )
        node_ops.save_many(node_list, actions=actions)

        # Process edges list
        edge_list = EdgeList(
            item_list=[
                SpecMapper.from_save_request(edge_json)
                for edge_json in sample_set.get('edges', [])
            ]
        )
        edge_ops.save_many(edge_list, actions=actions)

        if detect_concepts:
            print("⚠️  detect_concepts requested but not yet wired into the new CLI workflow for list import.")

    else:
        raise ValueError("import_method must be either 'object' or 'list'.")

    # Print footers
    print("🖥️  ~ Done.")
