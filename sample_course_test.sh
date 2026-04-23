graphregistry data exists --node=EPFL,Course,TEST-101
graphregistry data insert --node=@sample_course_node.json -d --actions=commit
graphregistry data fetch --node=EPFL,Course,TEST-101
graphregistry data delete --node=EPFL,Course,TEST-101 --actions=commit

graphregistry data exists --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data insert --edge=@sample_course_edge.json --actions=commit
graphregistry data fetch --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data delete --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher --actions=commit


# graphregistry data insert --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json --actions=commit
# graphregistry data insert --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json --actions=commit