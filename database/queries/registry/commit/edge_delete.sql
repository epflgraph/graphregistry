    -- Delete node from basic edges table
UPDATE [[registry]].Edges_N_Object_N_Object_T_ChildToParent
   SET record_deleted = 1
 WHERE (from_object_type, from_object_id, to_object_type, to_object_id, context)
     = ('[[from_object_type]]', '[[from_object_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');

    -- Delete edge from custom fields table
UPDATE [[registry]].Data_N_Object_N_Object_T_CustomFields
   SET record_deleted = 1
 WHERE (from_object_type, from_object_id, to_object_type, to_object_id, context)
     = ('[[from_object_type]]', '[[from_object_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');
