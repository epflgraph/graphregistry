    -- Nullify existing custom fields for edge before upserting new set
UPDATE [[registry]].Data_N_Object_N_Object_T_CustomFields
   SET record_deleted = 1
 WHERE (from_object_type, from_object_id, to_object_type, to_object_id, context)
     = ('[[from_object_type]]', '[[from_object_id]]', '[[to_object_type]]', '[[to_object_id]]', '[[context]]');
