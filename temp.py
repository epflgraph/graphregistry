from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_edge import MySQLEdgeMapper
from graphregistry.entrypoints.mappers.emp_node import EPNodeMapper
from graphregistry.entrypoints.mappers.emp_edge import EPEdgeMapper
import json, rich

# with open('sample_course_node.json', 'r') as f:
#    node_list_json = json.loads(f.read())
# node_list = MySQLNodeMapper.from_simplified_dict_list(node_list_json)
# simple_node_list = EPNodeMapper.to_get_request_list(node_list)
# rich.print_json(data=[x.model_dump(exclude_none=True) for x in simple_node_list])

# with open('epfl_graph_sample_set_EDGEs.json', 'r') as f:
#    edge_list_json = json.loads(f.read())
# edge_list = MySQLEdgeMapper.from_simplified_dict_list(edge_list_json)
# simple_edge_list = EPEdgeMapper.to_get_request_list(edge_list)
# rich.print_json(data=[x.model_dump(exclude_none=True) for x in simple_edge_list])

# with open('sample_course_node.json', 'r') as f:
#    node_json = json.loads(f.read())
# node = MySQLNodeMapper.from_simplified_dict(node_json)
# simple_node = EPNodeMapper.to_get_request(node)
# rich.print_json(data=simple_node.model_dump(exclude_none=True))

# with open('sample_course_edge.json', 'r') as f:
#    edge_json = json.loads(f.read())
# edge = MySQLEdgeMapper.from_simplified_dict(edge_json)
# simple_edge = EPEdgeMapper.to_get_request(edge)
# rich.print_json(data=simple_edge.model_dump(exclude_none=True))
