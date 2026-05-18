graphregistry data exists --node=EPFL,Course,TEST-101
graphregistry data get    --node=EPFL,Course,TEST-101
graphregistry data save   --node=@sample_course_node.json -d --actions=commit
graphregistry data delete --node=EPFL,Course,TEST-101 --actions=commit

graphregistry data exists --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data get    --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher
graphregistry data save   --edge=@sample_course_edge.json --actions=commit
graphregistry data delete --edge=EPFL,Course,TEST-101,EPFL,Person,01010101,teacher --actions=commit


# graphregistry data save --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json --actions=commit
# graphregistry data save --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json --actions=commit