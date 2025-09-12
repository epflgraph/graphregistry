CREATE OR REPLACE VIEW graph_registry.Data_N_Object_T_AllFields AS
         
                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_T_CalculatedFields

             UNION ALL

                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_T_CalculatedFields
                  
             UNION ALL

                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT institution_id, object_type, object_id, field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_T_CalculatedFields;