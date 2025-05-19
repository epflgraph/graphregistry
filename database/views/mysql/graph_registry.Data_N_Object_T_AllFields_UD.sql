CREATE OR REPLACE VIEW graph_registry.Data_N_Object_T_AllFields_UD AS
         
                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_T_CustomFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL

                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_T_CalculatedFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1

             UNION ALL
    
                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_T_CustomFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL

                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_T_CalculatedFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL

                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_T_CustomFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL

                SELECT cf.institution_id, cf.object_type, cf.object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_T_CalculatedFields cf
                 USING (institution_id, object_type, object_id)
            INNER JOIN graph_registry.Operations_N_Object_T_TypeFlags tf
                 USING (institution_id, object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1;