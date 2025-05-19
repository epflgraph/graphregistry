CREATE OR REPLACE VIEW graph_registry.Data_N_Object_N_Object_T_AllFieldsSymmetric_ToProcess AS

                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1

             UNION ALL
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1

             UNION ALL

                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
       
             UNION ALL
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1

             UNION ALL

                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
       
             UNION ALL
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_N_Object_T_CustomFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.from_institution_id, cf.from_object_type, cf.from_object_id,
                         cf.to_institution_id,   cf.to_object_type,   cf.to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
    
             UNION ALL 
    
                SELECT cf.to_institution_id   AS from_institution_id,
                       cf.to_object_type      AS from_object_type,
                       cf.to_object_id        AS from_object_id,
                       cf.from_institution_id AS to_institution_id,
                       cf.from_object_type    AS to_object_type,
                       cf.from_object_id      AS to_object_id,
                       cf.field_language, cf.field_name, cf.field_value
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_ontology.Data_N_Object_N_Object_T_CalculatedFields cf
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1;