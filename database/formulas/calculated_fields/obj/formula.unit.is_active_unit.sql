    SELECT n.object_type, n.object_id,
           'n/a' AS field_language,
           'is_active_unit' AS field_name,

        -- Conditions for the Unit being active: no end date or end date in the future
           (e2.field_value IS NULL OR CAST(e2.field_value AS DATETIME) > NOW()) AS field_value

        -- Start with all Unit objects
      FROM [[registry]].Nodes_N_Object n
     WHERE n.object_type = 'Unit'
       AND n.record_deleted = 0

        -- Append 'established' and 'terminated' dates
 LEFT JOIN [[registry]].Data_N_Object_T_CustomFields e1
        ON (n.object_type, n.object_id) = (e1.object_type, e1.object_id)
       AND e1.field_name = 'date_established'
       AND e1.record_deleted = 0
 LEFT JOIN [[registry]].Data_N_Object_T_CustomFields e2
        ON (n.object_type, n.object_id) = (e2.object_type, e2.object_id)
       AND e2.field_name = 'date_terminated'
       AND e2.record_deleted = 0

        -- Check object flags
INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged tp
        ON (n.object_type, n.object_id) = (tp.object_type, tp.object_id)

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
        ON (n.object_type) = (tf.object_type)

       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
