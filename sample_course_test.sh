graphregistry data exists --node=EPFL,Course,TEST-101
graphregistry data fetch --node=EPFL,Course,TEST-101
graphregistry data insert --node=@sample_course_node.json --actions=commit
graphregistry data fetch --node=EPFL,Course,TEST-101

graphregistry data exists --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data fetch --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data insert --edge=@sample_course_edge.json --actions=commit
graphregistry data fetch --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher


graphregistry data insert --node=@sample_course_node.json --actions=commit
graphregistry data delete --node=EPFL,Course,TEST-101 --actions=commit

graphregistry data insert --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json --actions=eval
graphregistry data insert --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json --actions=eval