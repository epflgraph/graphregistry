    -- Evaluate if edge exists in registry
SELECT COUNT(*) > 0 AS edge_exists
  FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent
 WHERE (from_object_type, from_object_id, to_object_type, to_object_id, context)
     = ('[[from_object_type]]', '[[from_object_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]')
   AND record_deleted = 0;
