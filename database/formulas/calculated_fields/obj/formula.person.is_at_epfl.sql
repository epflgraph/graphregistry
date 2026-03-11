    SELECT e.from_institution_id AS institution_id,
           e.from_object_type    AS object_type,
           e.from_object_id      AS object_id,
           'n/a'                 AS field_language,
           'is_at_epfl'          AS field_name,
        -- TODO: Add context field to formulas
        -- Conditions for being at EPFL: no end date or end date in the future
        -- NOTE: the max() is needed to aggregate multiple affiliations and keep a zero value if
        -- all affiliations are expired.
           MAX( (e.field_value IS NULL OR CAST(e.field_value AS DATETIME) > NOW()) ) AS field_value

        -- Start with people-unit affiliations
      FROM [[registry]].Data_N_Object_N_Object_T_CustomFields e

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
     USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
     USING (from_institution_id, from_object_type, to_institution_id, to_object_type)

     WHERE (e.from_object_type, e.to_object_type) = ('Person', 'Unit')
       AND e.field_name = 'end_datetime'
       AND tp.to_process = 1
       AND tf.to_process = 1

  GROUP BY e.from_object_id
