    -- Nullify existing custom fields for node before upserting new set
UPDATE [[registry]].Data_N_Object_T_CustomFields
   SET record_deleted = 1
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]');
