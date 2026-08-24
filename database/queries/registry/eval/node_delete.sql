     -- Evaluate if node exists in registry
 SELECT SUM(node_exists) > 0 AS node_exists
   FROM (SELECT COUNT(*) > 0 AS node_exists -- Scan basic nodes table
           FROM [[registry]].Nodes_N_Object
          WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS node_exists -- Scan page profile table
           FROM [[registry]].Data_N_Object_T_PageProfile
          WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS node_exists -- Scan custom fields table
           FROM [[registry]].Data_N_Object_T_CustomFields
          WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS node_exists -- Scan concept detection table
           FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection
          WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
        ) t
