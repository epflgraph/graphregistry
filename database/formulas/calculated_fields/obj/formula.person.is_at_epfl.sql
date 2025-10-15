    SELECT e.from_institution_id AS institution_id, e.from_object_type AS object_type, e.from_object_id AS object_id, 'n/a' AS field_language, 'is_at_epfl' AS field_name,
           CASE
           WHEN EXISTS (SELECT 1
                          FROM [[registry]].Data_N_Object_N_Object_T_CustomFields cf
                         WHERE (cf.from_institution_id, cf.from_object_type, cf.from_object_id, cf.to_institution_id, cf.to_object_type, cf.to_object_id, cf.context)
                             = ( e.from_institution_id,  e.from_object_type,  e.from_object_id,  e.to_institution_id,  e.to_object_type,  e.to_object_id,  e.context)
                           AND (cf.field_language, cf.field_name) = ('n/a', 'end_datetime')
                           AND cf.field_value IS NOT NULL
                           AND CAST(cf.field_value AS DATETIME) < NOW()
           ) THEN 0 ELSE 1 END AS field_value
      FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
         
        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
         
     WHERE (e.from_object_type, e.to_object_type) = ('Person', 'Unit')
     
       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
