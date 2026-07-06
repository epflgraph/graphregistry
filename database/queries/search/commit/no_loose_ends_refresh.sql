TRUNCATE TABLE graph_cache.Operations_N_Object_T_NoLooseEnds;
   INSERT INTO graph_cache.Operations_N_Object_T_NoLooseEnds (object_type, object_id) SELECT object_type, object_id FROM graph_registry.Data_N_Object_T_PageProfile;
   INSERT INTO graph_cache.Operations_N_Object_T_NoLooseEnds (object_type, object_id) SELECT object_type, object_id FROM graph_lectures.Data_N_Object_T_PageProfile;
   INSERT INTO graph_cache.Operations_N_Object_T_NoLooseEnds (object_type, object_id) SELECT object_type, object_id FROM graph_ontology.Data_N_Object_T_PageProfile;