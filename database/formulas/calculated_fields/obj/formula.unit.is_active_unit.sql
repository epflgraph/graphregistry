    SELECT n.institution_id, n.object_type, n.object_id,
           'n/a' AS field_language,
           'is_active_unit' AS field_name,
      
        -- Conditions for the Unit being active: no end date or end date in the future
           (e2.field_value IS NULL OR CAST(e2.field_value AS DATETIME) > NOW()) AS field_value
	  
        -- Start with all Unit objects
      FROM graph_registry.Nodes_N_Object n

        -- Append 'established' and 'terminated' dates
 LEFT JOIN graph_registry.Data_N_Object_T_CustomFields e1
        ON (n.institution_id, n.object_type, n.object_id) = (e1.institution_id, e1.object_type, e1.object_id)
       AND e1.field_name = 'date_established'
 LEFT JOIN graph_registry.Data_N_Object_T_CustomFields e2
        ON (n.institution_id, n.object_type, n.object_id) = (e2.institution_id, e2.object_type, e2.object_id)
       AND e2.field_name = 'date_terminated'
        
        -- Check object flags
INNER JOIN graph_airflow.Operations_N_Object_T_FieldsChanged tp
        ON (n.institution_id, n.object_type, n.object_id) = (tp.institution_id, tp.object_type, tp.object_id)

        -- Check type flags
INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
        ON (n.institution_id, n.object_type) = (tf.institution_id, tf.object_type)

     WHERE n.object_type = 'Unit'
       AND tp.to_process = 1
       AND tf.to_process = 1
       AND tf.flag_type = 'fields'
