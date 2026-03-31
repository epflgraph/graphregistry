    -- Evaluate if edge exists in registry
SELECT COUNT(*) > 0 AS edge_exists
  FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent
 WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
     = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');