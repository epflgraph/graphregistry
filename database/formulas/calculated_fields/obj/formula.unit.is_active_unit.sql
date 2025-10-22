    SELECT n.institution_id, n.object_type, n.object_id, 'n/a' AS field_language, 'is_active_unit' AS field_name,
           CASE
           WHEN EXISTS (SELECT 1
                          FROM [[registry]].Data_N_Object_T_CustomFields cf
                         WHERE (cf.object_id, cf.object_type, cf.field_language, cf.field_name)
                             = (n.object_id, 'Unit', 'n/a', 'date_terminated')
                           AND cf.field_value IS NOT NULL
                           AND CAST(cf.field_value AS DATETIME) < NOW()
           ) THEN 0 ELSE 1 END AS field_value
      FROM [[registry]].Nodes_N_Object n

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged tp
     USING (institution_id, object_type, object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
     USING (institution_id, object_type)

     WHERE n.object_type = 'Unit'

       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
