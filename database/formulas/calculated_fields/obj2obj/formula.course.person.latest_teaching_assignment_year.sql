    SELECT e.from_object_type AS from_object_type,
           e.from_object_id   AS from_object_id,
           e.to_object_type   AS to_object_type,
           e.to_object_id     AS to_object_id,
           'teacher'          AS context,
           'n/a' AS field_language, 'latest_teaching_assignment_year' AS field_name, MAX(e.field_value) AS field_value
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields e

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_object_type, from_object_id, to_object_type, to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_object_type, to_object_type)

     WHERE (e.from_object_type, e.to_object_type, e.context) = ('Course', 'Person', 'teacher')
       AND e.field_name = 'teaching_assignment_year'

       AND tp.to_process = 1
       AND tf.to_process = 1

  GROUP BY e.from_object_id, e.to_object_id
