    SELECT n.institution_id, n.object_type, n.object_id, 'n/a' AS field_language, 'is_active_unit' AS field_name,
           
           CASE
           WHEN EXISTS (SELECT 1
                          FROM (SELECT cf.object_id,
                                  CASE
                                  WHEN cf.field_value REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                                  THEN STR_TO_DATE(cf.field_value, '%Y-%m-%d')
                                  WHEN cf.field_value REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                                  THEN STR_TO_DATE(cf.field_value, '%Y-%m-%d %H:%i:%s')
                                  ELSE NULL END AS dt
                                  FROM [[registry]].Data_N_Object_T_CustomFields cf
                                 WHERE cf.object_type    = 'Unit'
                                   AND cf.field_language = 'n/a'
                                   AND cf.field_name     = 'date_terminated'
                                   AND cf.field_value IS NOT NULL) AS dt_cf
                         WHERE dt_cf.object_id = n.object_id
                           AND dt_cf.dt IS NOT NULL
                           AND dt_cf.dt < NOW()) THEN 0 ELSE 1 END AS field_value

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
