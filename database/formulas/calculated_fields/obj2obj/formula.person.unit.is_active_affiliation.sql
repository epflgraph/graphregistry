    SELECT e.from_object_type      AS from_object_type,
           e.from_object_id        AS from_object_id,
           e.to_object_type        AS to_object_type,
           e.to_object_id          AS to_object_id,
           'accreditation'         AS context,
           'n/a'                   AS field_language,
           'is_active_affiliation' AS field_name,

        -- Conditions for being an active Person-Unit affiliation: no end date or end date in the future
           (e.field_value IS NULL OR CAST(e.field_value AS DATETIME) > NOW()) AS field_value

        -- Start with people-unit affiliations
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields e

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_object_type, from_object_id, to_object_type, to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_object_type, to_object_type)

     WHERE (e.from_object_type, e.to_object_type, e.context) = ('Person', 'Unit', 'accreditation')
       AND e.field_name = 'end_datetime'
       AND e.record_deleted = 0
       AND tp.to_process = 1
       AND tf.to_process = 1
