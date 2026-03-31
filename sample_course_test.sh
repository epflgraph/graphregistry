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
