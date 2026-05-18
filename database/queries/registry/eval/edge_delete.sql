     -- Evaluate if edge exists in registry
 SELECT SUM(edge_exists) > 0 AS edge_exists
   FROM (SELECT COUNT(*) > 0 AS edge_exists -- Scan basic edges table
           FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent
          WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
              = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]')
      UNION ALL
         SELECT COUNT(*) > 0 AS edge_exists -- Scan custom fields table
           FROM [[registry]].Data_N_Object_N_Object_T_CustomFields
          WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
              = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]')
        ) t
