# graphregistry/cli/cmd_data.py
from graphregistry.domain.models.mdl_base import NodeKey, EdgeKey
from graphregistry.domain.models.mdl_node import Node, NodeList
from graphregistry.domain.models.mdl_edge import Edge, EdgeList
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.workflows.operations.ops_node import NodeOperations
from graphregistry.workflows.operations.ops_edge import EdgeOperations
from graphregistry.adapters.mysql.adp_noderepo import MySQLNodeRepository
from graphregistry.adapters.mysql.adp_edgerepo import MySQLEdgeRepository
from pathlib import Path
import json, rich

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
    with open(input_file, 'r') as fp:
        sample_set = json.load(fp)

    # Method 1: Process and commit object by object
    if import_method == 'object':

        # Process nodes
        for node_json in sample_set['nodes']:
            node = registry.Node()
            node.set_from_json(doc_json=node_json, detect_concepts=detect_concepts)
            node.commit(actions=actions)

        # Process edges
        for edge_json in sample_set['edges']:
            edge = registry.Edge()
            edge.set_from_json(doc_json=edge_json)
            edge.commit(actions=actions)

    # Method 2: Process and commit as list of objects
    elif import_method == 'list':

        # Process nodes list
        node_list = registry.NodeList()
        node_list.set_from_json(doc_json_list=sample_set['nodes'], detect_concepts=detect_concepts)
        node_list.commit(actions=actions)

        # Process edges list
        edge_list = registry.EdgeList()
        edge_list.set_from_json(doc_json_list=sample_set['edges'])
        edge_list.commit(actions=actions)

    # Print footers
    print("🖥️  ~ Done.")

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
        node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
        print(f"""{"✅ Exists" if node_repo.exists(node_key) else "❌ Not found"}: Node ~ ({node_key.institution_id}, {node_key.object_type}, {node_key.object_id})""")

    # Check if edge and print results
    if edge_key:
        edge_repo: EdgeRepository = MySQLEdgeRepository(engine_name=env, db=db)
        print(f"""{"✅ Exists" if edge_repo.exists(edge_key) else "❌ Not found"}: Edge ~ ({edge_key.from_institution_id}, {edge_key.from_object_type}, {edge_key.from_object_id}, {edge_key.to_institution_id}, {edge_key.to_object_type}, {edge_key.to_object_id}, {edge_key.context})""")

# Handler: Fetch node or edge from Registry
def cmd_data_fetch(args):

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
        node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
        node = node_repo.get(node_key)
        if node:
            rich.print_json(data=node.to_simplified_dict())
        else:
            print(f"❌ Not found: Node{node_key_tuple}")

    # Fetch edge and print results
    if edge_key:
        edge_repo: EdgeRepository = MySQLEdgeRepository(engine_name=env, db=db)
        edge = edge_repo.get(edge_key)
        if edge:
            rich.print_json(data=edge.to_simplified_dict())
        else:
            print(f"❌ Not found: Edge{edge_key_tuple}")

# Handler: Insert node or edge in Registry
def cmd_data_insert(args):

    # Fetch context objects
    db = args.ctx.db

    # Fetch environment from input options
    env = args.env

    # Fetch input options
    node_input = args.node
    edge_input = args.edge
    node_list_input = args.node_list
    edge_list_input = args.edge_list
    actions = tuple(args.actions.split(',')) if args.actions else ()

    # Process node input
    if node_input:

        # Case 1: --node=@path/to/file.json
        if node_input.startswith("@"):

            # Extract path from input
            path_str = node_input[1:]
            node_json_path = Path(path_str)

            # Check if file exists
            if not node_json_path.exists():
                raise FileNotFoundError(f"Node data file not found: {node_json_path}")

            # Open JSON file and load data
            with node_json_path.open("r") as fp:

                # Load JSON data from file
                node_json_data = json.load(fp)

        # Case 2: --node='<json>'
        else:
            print("Parsing inline JSON node.")
            try:
                node_json_data = json.loads(node_input)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON passed to --node: {e}") from e

        # Create node key from JSON data
        node_key = NodeKey(institution_id=node_json_data["institution_id"], object_type=node_json_data["object_type"], object_id=node_json_data["object_id"])

        # Create node object from JSON data
        node = Node(key=node_key)
        node.from_simplified_dict(node_json_data)

        # Insert node into registry
        node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
        node_repo.save(node, actions=actions)

    # Process edge input
    if edge_input:

        # Case 1: --edge=@path/to/file.json
        if edge_input.startswith("@"):

            # Extract path from input
            path_str = edge_input[1:]
            edge_json_path = Path(path_str)

            # Check if file exists
            if not edge_json_path.exists():
                raise FileNotFoundError(f"Edge data file not found: {edge_json_path}")

            # Open JSON file and load data
            with edge_json_path.open("r") as fp:

                # Load JSON data from file
                edge_json_data = json.load(fp)

        # Case 2: --edge='<json>'
        else:
            print("Parsing inline JSON edge.")
            try:
                edge_json_data = json.loads(edge_input)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON passed to --edge: {e}") from e

        # Create edge key from JSON data
        edge_key = EdgeKey(from_institution_id=edge_json_data["from_institution_id"], from_object_type=edge_json_data["from_object_type"], from_object_id=edge_json_data["from_object_id"], to_institution_id=edge_json_data["to_institution_id"], to_object_type=edge_json_data["to_object_type"], to_object_id=edge_json_data["to_object_id"], context=edge_json_data["context"])

        # Create edge object from JSON data
        edge = Edge(key=edge_key)
        edge.from_simplified_dict(edge_json_data)

        # Insert edge into registry
        edge_repo: EdgeRepository = MySQLEdgeRepository(engine_name=env, db=db)
        edge_repo.save(edge, actions=actions)

    # Process node list input
    if node_list_input:

        # Case 1: --node_list=@path/to/file.json
        if node_list_input.startswith("@"):

            # Extract path from input
            path_str = node_list_input[1:]
            node_list_json_path = Path(path_str)

            # Check if file exists
            if not node_list_json_path.exists():
                raise FileNotFoundError(f"Node list data file not found: {node_list_json_path}")

            # Open JSON file
            with node_list_json_path.open("r") as fp:

                # Load JSON data from file
                node_list_json_data = json.load(fp)

        # Case 2: --node_list='<json>'
        else:
            print("Parsing inline JSON node list.")
            try:
                node_list_json_data = json.loads(node_list_input)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON passed to --node_list: {e}") from e

        # Create node list object from JSON data
        node_list = NodeList()
        node_list.from_simplified_dict_list(node_list_json_data)

        # Insert node list into registry
        node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
        node_repo.save_many(node_list, actions=actions)

    # Process edge list input
    if edge_list_input:

        # Case 1: --edge_list=@path/to/file.json
        if edge_list_input.startswith("@"):

            # Extract path from input
            path_str = edge_list_input[1:]
            edge_list_json_path = Path(path_str)

            # Check if file exists
            if not edge_list_json_path.exists():
                raise FileNotFoundError(f"Edge list data file not found: {edge_list_json_path}")

            # Open JSON file and load data
            with edge_list_json_path.open("r") as fp:
                edge_list_json_data = json.load(fp)

        # Case 2: --edge_list='<json>'
        else:
            print("Parsing inline JSON edge list.")
            try:
                edge_list_json_data = json.loads(edge_list_input)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON passed to --edge_list: {e}") from e

        # Create edge list object from JSON data
        edge_list = EdgeList()
        edge_list.from_simplified_dict_list(edge_list_json_data)

        # Insert edge list into registry
        edge_repo: EdgeRepository = MySQLEdgeRepository(engine_name=env, db=db)
        edge_repo.save_many(edge_list, actions=actions)

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

    # Fetch node and print results
    if node_key:
        node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
        node_repo.delete(node_key, actions=actions)

    # Fetch edge and print results
    if edge_key:
        edge_repo: EdgeRepository = MySQLEdgeRepository(engine_name=env, db=db)
        edge_repo.delete(edge_key, actions=actions)

# Handler: Genereal debug command for data operations
def cmd_data_debug(args):
    """
    Handle:
      graphregistry data fetch [...]
    """

    # Fetch context objects
    registry = args.ctx.registry
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Fetch node or edge from Registry.")

    # Get input options
    env = args.env
    node_key_tuple = tuple(args.node.split(',')) if args.node else None
    edge_key = tuple(args.edge.split(',')) if args.edge else None

    node = registry.Node(object_key=node_key_tuple)
    # rich.print_json(data=node.to_json())
    # return

    json_data = node.to_json()

    # Create node key object from input tuple
    node_key = NodeKey.from_tuple(tuple(args.node.split(','))) if node_key_tuple else None
    if node_key is None:
        raise ValueError("node_key is required")

    # Create node object
    node = Node(key=node_key)

    node_repo: NodeRepository = MySQLNodeRepository(engine_name=env, db=db)
    node_ops = NodeOperations(repo=node_repo)
    node_ops.insert(node, actions=('eval',))

    return

    # Set page profile from JSON data
    node.title        = json_data["object_title"]
    node.text_source  = json_data["text_source"]
    node.raw_text     = json_data["raw_text"]
    node.field_list.set_from_json(json_data["custom_fields"], node_key=node_key)
    node.page_profile.set_from_json(json_data["page_profile"])


    from deepdiff import DeepDiff

    diff = DeepDiff(
        json_data,
        node.to_simplified_dict(),
        ignore_order=True,  # key for your case
    )

    # print(diff)

    def remove_field(obj, field_name):
        if isinstance(obj, dict):
            return {
                k: remove_field(v, field_name)
                for k, v in obj.items()
                if k != field_name
            }
        elif isinstance(obj, list):
            return [remove_field(item, field_name) for item in obj]
        else:
            return obj

    json_data = remove_field(json_data, "record_created_date")
    json_data = remove_field(json_data, "record_updated_date")


    d1 = dict(sorted(json_data['page_profile'].items()))
    d2 = dict(sorted(node.page_profile.to_flattened_dict().items()))

    # Remove all fields that end with "_it" or "_de"
    d2 = {k: v for k, v in d2.items() if not ("_it" in k or "_de" in k)}
    d2['is_visible'] = int(d2['is_visible']) if 'is_visible' in d2 else None

    # Remove keys institution_id, object_type, object_id
    d2.pop("institution_id", None)
    d2.pop("object_type", None)
    d2.pop("object_id", None)

    # rich.print_json(data=d1)
    # print('\n\n\n\n\n\n')
    # rich.print_json(data=d2)

    diff = DeepDiff(
        json_data['custom_fields'],
        node.field_list.to_simplified_list(),
        ignore_order=True,  # key for your case
    )
    rich.print_json(data=diff)

    # rich.print_json(data=node.field_list.to_list())
    # rich.print_json(data=json_data['custom_fields'])


    d1 = json_data
    d1.pop('page_profile', None)
    d1.pop('custom_fields', None)
    d1.pop('concepts_detection', None)
    d1.pop('manual_mapping', None)
    # rich.print_json(data=d1)

    d2 = node.to_simplified_dict()
    d2.pop('page_profile', None)
    d2.pop('custom_fields', None)
    # rich.print_json(data=d2)

    diff = DeepDiff(
        d1,
        d2,
        ignore_order=True,  # key for your case
    )
    rich.print_json(data=dict(diff))

    return

    # -----------------#
    # Execute commands #
    # -----------------#

    # # Open JSON sample set
    # if node_key:
    #     node = registry.Node(object_key=node_key)
    #     node.info()

    # Print footers
    print("🖥️  ~ Done.")
