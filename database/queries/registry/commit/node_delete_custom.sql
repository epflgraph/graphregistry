DELETE -- Delete existing custom fields for node before upserting new set
  FROM [[registry]].Data_N_Object_T_CustomFields
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
