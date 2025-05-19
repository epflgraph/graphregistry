CREATE OR REPLACE VIEW graph_registry.Data_N_Object_N_Object_T_AllFieldsSymmetric AS
    
                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL 
    
                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_N_Object_T_CalculatedFields 
    
             UNION ALL 
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_registry.Data_N_Object_N_Object_T_CalculatedFields

             UNION ALL

                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL 
    
                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_N_Object_T_CalculatedFields 
    
             UNION ALL 
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_lectures.Data_N_Object_N_Object_T_CalculatedFields

             UNION ALL
    
                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_N_Object_T_CustomFields 
    
             UNION ALL 
    
                SELECT from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_N_Object_T_CalculatedFields 
    
             UNION ALL 
    
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       field_language, field_name, field_value
                  FROM graph_ontology.Data_N_Object_N_Object_T_CalculatedFields;
