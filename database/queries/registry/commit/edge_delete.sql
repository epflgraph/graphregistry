DELETE -- Delete node from basic edges table
  FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent
 WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
     = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');

DELETE -- Delete edge from custom fields table
  FROM [[registry]].Data_N_Object_N_Object_T_CustomFields
 WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
     = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');
