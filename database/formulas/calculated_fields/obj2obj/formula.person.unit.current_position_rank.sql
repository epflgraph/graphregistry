    SELECT cf1.from_object_type    AS from_object_type,
           cf1.from_object_id      AS from_object_id,
           cf1.to_object_type      AS to_object_type,
           cf1.to_object_id        AS to_object_id,
           'accreditation'         AS context,
           'n/a'                   AS field_language,
 	       'current_position_rank' AS field_name,
           m2.to_field_value       AS field_value

        -- Start with people-unit affiliations
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields cf1

        -- Join mapping from position name to position group
INNER JOIN [[registry]].Mapping_N_Field_N_Field m1
        ON cf1.field_value = m1.from_field_value

        -- Join mapping from position group to position rank
INNER JOIN [[registry]].Mapping_N_Field_N_Field m2
        ON m1.to_field_value = m2.from_field_value

        -- Join affiliation temination date (to determine if affiliations are active, and keep only those that are)
INNER JOIN [[registry]].Data_N_Object_N_Object_T_CustomFields cf2
        ON (cf1.from_object_type, cf1.from_object_id, cf1.to_object_type, cf1.to_object_id)
         = (cf2.from_object_type, cf2.from_object_id, cf2.to_object_type, cf2.to_object_id)

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
        ON (cf1.from_object_type, cf1.from_object_id, cf1.to_object_type, cf1.to_object_id)
         = ( tp.from_object_type,  tp.from_object_id,  tp.to_object_type,  tp.to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
        ON (tp.from_object_type, tp.to_object_type)
         = (tf.from_object_type, tf.to_object_type)

     WHERE cf1.from_object_type = 'Person'
       AND cf1.to_object_type   = 'Unit'
       AND cf1.context          = 'accreditation'
       AND cf1.field_name       = 'last_position_name'
       AND cf2.context          = 'accreditation'
       AND cf2.field_name       = 'end_datetime'
       AND m1.context           = 'position grouping'
       AND m2.context           = 'position group ranking'
       AND m1.from_field_name   = 'position_name'
       AND m2.from_field_name   = 'position_group'
       AND tp.to_process = 1
       AND tf.to_process = 1
        -- Conditions for being an active Person-Unit affiliation: no end date or end date in the future
       AND (cf2.field_value IS NULL OR CAST(cf2.field_value AS DATETIME) > NOW()) = 1
