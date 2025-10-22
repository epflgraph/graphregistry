    SELECT n.from_institution_id, n.from_object_type, n.from_object_id,
           n.to_institution_id,   n.to_object_type,   n.to_object_id,
           'n/a' AS field_language, 'is_active_affiliation' AS field_name,
           CASE
           WHEN EXISTS (SELECT 1
                          FROM [[registry]].Data_N_Object_N_Object_T_CustomFields cf
                         WHERE (cf.from_object_id, cf.from_object_type, cf.to_object_id, cf.to_object_type, cf.field_language, cf.field_name)
                             = (n.from_object_id, 'Person', n.to_object_id, 'Unit', 'n/a', 'end_datetime')
                           AND cf.field_value IS NOT NULL
                           AND CAST(cf.field_value AS DATETIME) < NOW()
           ) THEN 0 ELSE 1 END AS field_value
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields n

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)

     WHERE (n.from_object_type, n.to_object_type) = ('Person', 'Unit')

       AND tp.to_process = 1
       AND tf.to_process = 1
