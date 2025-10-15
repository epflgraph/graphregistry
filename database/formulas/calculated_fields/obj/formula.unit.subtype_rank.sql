    SELECT institution_id, object_type, object_id, 
           'n/a' AS field_language, 'subtype_rank' AS field_name, m1.to_field_value AS field_value
      FROM [[registry]].Data_N_Object_T_PageProfile e
 LEFT JOIN [[registry]].Mapping_N_Field_N_Field m1 
        ON e.subtype_en = m1.from_field_value

        -- Check object flags      
INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged tp
     USING (institution_id, object_type, object_id)
         
        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
     USING (institution_id, object_type)

     WHERE e.object_type = 'Unit'
       AND (m1.from_object_type, m1.to_object_type, m1.context) = ('Unit', 'Unit', 'subtype ranking')

       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
