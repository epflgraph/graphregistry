    -- Nullify existing custom fields for edge before upserting new set
UPDATE [[registry]].Data_N_Object_N_Object_T_CustomFields
   SET from_object_id = CONCAT('__deleted__', row_id, '__', from_object_id),
       to_object_id   = CONCAT('__deleted__', row_id, '__', to_object_id)
 WHERE (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context)
     = ('[[from_institution_id]]', '[[from_object_type]]', '[[from_object_id]]', '[[to_institution_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');
