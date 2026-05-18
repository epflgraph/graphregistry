# graphregistry/entrypoints/cli/cmd_data.py
from __future__ import annotations
from pathlib import Path
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList, EdgeKey, EdgeKeyList
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.domain.models.entities.mdl_edge import EdgeList
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.workflows.operations.entities.ops_node import NodeOperations
from graphregistry.workflows.operations.entities.ops_edge import EdgeOperations
from graphregistry.workflows.factories.fct_node import NodeFactory
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.entrypoints.mappers.emp_node import EPNodeMapper
from graphregistry.entrypoints.mappers.emp_edge import EPEdgeMapper
from graphregistry.adapters.services.schema.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.domain.models.entities.mdl_subgraph import SubGraph
import rich, json

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

# Helper: Load JSON from inline string or file
def _load_json_input(raw_input: str, label: str):
    # Case 1: --xxx=@path/to/file.json
    if raw_input.startswith("@"):

        # Extract path from input
        path_str = raw_input[1:]
        json_path = Path(path_str)

        # Check if file exists
        if not json_path.exists():
            raise FileNotFoundError(f"{label} data file not found: {json_path}")

        # Open JSON file and load data
        with json_path.open("r", encoding="utf-8") as fp:
            return json.load(fp)

    # Case 2: --xxx='<json>'
    else:
        print(f"Parsing inline JSON {label}.")
        try:
            return json.loads(raw_input)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON passed to {label}: {e}") from e

# Handler: Check if node or edge exists in Registry
def cmd_data_list(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    object_type = tuple(args.object_type.split(',')) if ',' in args.object_type else str(args.object_type)
    id_pattern = args.id_pattern

    # Node list requested
    if type(object_type) is str:
        node_repo: NodeRepository = _make_node_repo(args)
        node_list = node_repo.list(object_type=object_type, id_pattern=id_pattern)
        rich.print(node_list)

    # Edge list requested
    elif type(object_type) is tuple:
        edge_repo: EdgeRepository = _make_edge_repo(args)
        edge_list = edge_repo.list(object_type=object_type, id_pattern=id_pattern)
        rich.print(edge_list)

# Handler: Check if node or edge exists in Registry
def cmd_data_exists(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    node_key_tuple = tuple(args.node.split(',')) if args.node else None
    edge_key_tuple = tuple(args.edge.split(',')) if args.edge else None

    # Get input options
    node_key = NodeKey.from_tuple(node_key_tuple) if node_key_tuple else None
    edge_key = EdgeKey.from_tuple(edge_key_tuple) if edge_key_tuple else None

    # Check if node and print results
    if node_key:
        node_repo: NodeRepository = _make_node_repo(args)
        print(f"""{"✅ Exists" if node_repo.exists(node_key) else "❌ Not found"}: Node ~ ({node_key.institution_id}, {node_key.object_type}, {node_key.object_id})""")

    # Check if edge and print results
    if edge_key:
        edge_repo: EdgeRepository = _make_edge_repo(args)
        print(f"""{"✅ Exists" if edge_repo.exists(edge_key) else "❌ Not found"}: Edge ~ ({edge_key.from_institution_id}, {edge_key.from_object_type}, {edge_key.from_object_id}, {edge_key.to_institution_id}, {edge_key.to_object_type}, {edge_key.to_object_id}, {edge_key.context})""")

# Handler: Fetch node or edge from Registry
def cmd_data_get(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    node_key_tuple = tuple(args.node.split(',')) if args.node else None
    edge_key_tuple = tuple(args.edge.split(',')) if args.edge else None

    # Get input options
    node_key = NodeKey.from_tuple(node_key_tuple) if node_key_tuple else None
    edge_key = EdgeKey.from_tuple(edge_key_tuple) if edge_key_tuple else None

    # Fetch node and print results
    if node_key:
        node_repo: NodeRepository = _make_node_repo(args)
        node = node_repo.get(node_key)
        if node:
            rich.print_json(data=EPNodeMapper.to_get_request(node).model_dump(exclude_none=True))

    # Fetch edge and print results
    if edge_key:
        edge_repo: EdgeRepository = _make_edge_repo(args)
        edge = edge_repo.get(edge_key)
        if edge:
            rich.print_json(data=EPEdgeMapper.to_get_request(edge).model_dump(exclude_none=True))

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
    subgraph_input  = args.subgraph
    actions         = tuple(args.actions.split(',')) if args.actions else ()
    detect_concepts = args.detect_concepts

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)
    node_ops = NodeOperations(repo=node_repo)
    edge_ops = EdgeOperations(repo=edge_repo)

    # Initialize concept detection gateway and node factory (if needed)
    gtw = GraphAIConceptGateway(debug=True)
    node_factory = NodeFactory(concept_gateway=gtw)

    # Process node input
    if node_input:

        # Case 1: --node=@path/to/file.json
        # Case 2: --node='<json>'
        node_json_data = _load_json_input(node_input, "--node")

        # Create node object from JSON data using factory to leverage concept detection if requested
        node = EPNodeMapper.from_save_request(node_json_data)

        # Insert node into registry
        node_ops.save(node, actions=actions)

    # Process edge input
    if edge_input:

        # Case 1: --edge=@path/to/file.json
        # Case 2: --edge='<json>'
        edge_json_data = _load_json_input(edge_input, "--edge")

        # Create edge object from JSON data
        edge = EPEdgeMapper.from_save_request(edge_json_data)

        # Insert edge into registry
        edge_ops.save(edge, actions=actions)

    # Process node list input
    if node_list_input:

        # Case 1: --node_list=@path/to/file.json
        # Case 2: --node_list='<json>'
        node_list_json_data = _load_json_input(node_list_input, "--node_list")

        # Create node list object from JSON data
        node_list = EPNodeMapper.from_save_request_list(node_list_json_data)

        # Insert node list into registry
        node_ops.save_many(node_list, actions=actions)

        if detect_concepts:
            print("⚠️  detect_concepts requested but not yet wired into the new CLI workflow for node_list.")

    # Process edge list input
    if edge_list_input:

        # Case 1: --edge_list=@path/to/file.json
        # Case 2: --edge_list='<json>'
        edge_list_json_data = _load_json_input(edge_list_input, "--edge_list")

        # Create edge list object from JSON data
        edge_list = EPEdgeMapper.from_save_request_list(edge_list_json_data)

        # Insert edge list into registry
        edge_ops.save_many(edge_list, actions=actions)

    # Process subgraph input
    if subgraph_input:

        # Case 1: --subgraph=@path/to/file.json
        # Case 2: --subgraph='<json>'
        subgraph_json_data = _load_json_input(subgraph_input, "--subgraph")

        # Create subgraph object from JSON data
        subgraph = SubGraph(
            nodes=NodeList(
                item_list=[
                    EPNodeMapper.from_save_request(node_json)
                    for node_json in subgraph_json_data.get("nodes", [])
                ]
            ),
            edges=EdgeList(
                item_list=[
                    EPEdgeMapper.from_save_request(edge_json)
                    for edge_json in subgraph_json_data.get("edges", [])
                ]
            ),
        )

        # Insert subgraph into registry
        node_ops.save_many(subgraph.nodes, actions=actions)
        edge_ops.save_many(subgraph.edges, actions=actions)

        if detect_concepts:
            print("⚠️  detect_concepts requested but not yet wired into the new CLI workflow for subgraph.")

# Handler: Check if node or edge exists in Registry
def cmd_data_delete(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch input options
    env = args.env
    node_key_tuple = tuple(args.node.split(',')) if args.node else None
    edge_key_tuple = tuple(args.edge.split(',')) if args.edge else None

    # Get input options
    node_key = NodeKey.from_tuple(node_key_tuple) if node_key_tuple else None
    edge_key = EdgeKey.from_tuple(edge_key_tuple) if edge_key_tuple else None
    actions  = tuple(args.actions.split(','))  if args.actions  else ()

    # Build repositories
    node_repo: NodeRepository = _make_node_repo(args)
    edge_repo: EdgeRepository = _make_edge_repo(args)

    # Fetch node and print results
    if node_key:
        node_repo.delete(node_key, actions=actions)

    # Fetch edge and print results
    if edge_key:
        edge_repo.delete(edge_key, actions=actions)

    # Fetch node list and print results
    if args.node_list:

        # Case 1: --node_list=@path/to/file.json
        # Case 2: --node_list='<json>'
        node_list_json_data = _load_json_input(args.node_list, "--node_list")

        # Build key list
        node_key_list = NodeKeyList.from_tuple_list(node_list_json_data)

        # Delete node list
        node_repo.delete_many(node_key_list, actions=actions)

    # Fetch edge list and print results
    if args.edge_list:

        # Case 1: --edge_list=@path/to/file.json
        # Case 2: --edge_list='<json>'
        edge_list_json_data = _load_json_input(args.edge_list, "--edge_list")

        # Build key list
        edge_key_list = EdgeKeyList.from_tuple_list(edge_list_json_data)

        # Delete edge list
        edge_repo.delete_many(edge_key_list, actions=actions)

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
            node = EPNodeMapper.from_save_request(node_json)
            node_ops.save(node, actions=actions)

            if detect_concepts:
                print(f"⚠️  detect_concepts requested but not yet wired into the new CLI workflow for node {node.key.to_tuple()}.")

        # Process edges
        for edge_json in sample_set.get('edges', []):
            edge = EPEdgeMapper.from_save_request(edge_json)
            edge_ops.save(edge, actions=actions)

    # Method 2: Process and commit as list of objects
    elif import_method == 'list':

        # Process nodes list
        node_list = NodeList(
            item_list=[
                EPNodeMapper.from_save_request(node_json)
                for node_json in sample_set.get('nodes', [])
            ]
        )
        node_ops.save_many(node_list, actions=actions)

        # Process edges list
        edge_list = EdgeList(
            item_list=[
                EPEdgeMapper.from_save_request(edge_json)
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
