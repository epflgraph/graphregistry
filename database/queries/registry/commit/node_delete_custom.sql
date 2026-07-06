    -- Nullify existing custom fields for node before upserting new set
UPDATE [[registry]].Data_N_Object_T_CustomFields
   SET field_value = "__DELETED__"
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
