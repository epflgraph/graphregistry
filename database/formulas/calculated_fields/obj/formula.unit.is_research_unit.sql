    SELECT institution_id, object_type, object_id,
           'n/a' AS field_language, 'is_research_unit' AS field_name,
           subtype_en IS NOT NULL AND subtype_en IN ('Laboratory', 'Group', 'Chair', 'Center', 'Institute') AS field_value
      FROM [[registry]].Data_N_Object_T_PageProfile p

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged tp
     USING (institution_id, object_type, object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
     USING (institution_id, object_type)

     WHERE p.object_type = 'Unit'

       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
