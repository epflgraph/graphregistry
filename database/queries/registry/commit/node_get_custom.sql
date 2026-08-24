    -- Get node custom fields from registry
SELECT field_language, field_name, field_value
  FROM [[registry]].Data_N_Object_T_CustomFields
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
   AND record_deleted = 0;
