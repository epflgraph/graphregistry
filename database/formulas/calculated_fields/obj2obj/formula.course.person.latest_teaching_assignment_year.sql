    SELECT from_institution_id, from_object_type, from_object_id,
           to_institution_id,   to_object_type,   to_object_id,
           'n/a' AS field_language, 'latest_teaching_assignment_year' AS field_name, MAX(field_value) AS field_value
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)

     WHERE (from_object_type, to_object_type) = ('Course', 'Person')
       AND field_name = 'teaching_assignment_year'

       AND tp.to_process = 1
       AND tf.to_process = 1

  GROUP BY from_institution_id, from_object_id, to_institution_id, to_object_id
