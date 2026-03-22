from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_node import Node
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.workflows.operations.ops_node import NodeOperations
from graphregistry.adapters.mysql.adp_noderepo import MySQLNodeRepository
import json, rich

#-----------------------------------------#
# Handler: ... #
#-----------------------------------------#
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

#-----------------------------------------#
# Handler: ... #
#-----------------------------------------#
def cmd_data_insert(args):
    pass

#-----------------------------------------#
# Handler: ... #
#-----------------------------------------#
def cmd_data_fetch(args):
    """
    Handle:
      graphregistry data fetch [...]
    """

    # Fetch context objects
    registry = args.ctx.registry

    # Print headers
    print("🖥️  ~ Graph Registry CLI. Import data from json file.")

    # Get input options
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

    node_repo: NodeRepository = MySQLNodeRepository()
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
