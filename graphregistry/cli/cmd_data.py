import json

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
