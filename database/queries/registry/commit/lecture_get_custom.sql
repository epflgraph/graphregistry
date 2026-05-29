    -- Get node custom fields from lectures
SELECT field_language, field_name, field_value
  FROM [[lectures]].Data_N_Object_T_CustomFields
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
