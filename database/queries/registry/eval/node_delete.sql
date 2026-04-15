     -- Evaluate if node exists in registry
 SELECT SUM(node_exists) > 0 AS node_exists
   FROM (SELECT COUNT(*) > 0 AS node_exists -- Scan basic nodes table
           FROM [[registry]].Nodes_N_Object
          WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS node_exists -- Scan page profile table
           FROM [[registry]].Data_N_Object_T_PageProfile
          WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS node_exists -- Scan custom fields table
           FROM [[registry]].Data_N_Object_T_CustomFields
          WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
        ) t
