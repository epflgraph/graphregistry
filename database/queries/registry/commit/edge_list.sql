    -- List non-deleted edges in registry
SELECT from_object_type, from_object_id,
         to_object_type, to_object_id, context
  FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent
 WHERE (from_object_type, to_object_type) = ('[[from_object_type]]', '[[to_object_type]]')
   AND (from_object_id LIKE '[[id_pattern]]' OR to_object_id LIKE '[[id_pattern]]')
   AND record_deleted = 0;
