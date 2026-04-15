    -- Get edge custom fields from registry
SELECT field_language, field_name, field_value
  FROM [[registry]].Data_N_Object_N_Object_T_CustomFields
 WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
     = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');
