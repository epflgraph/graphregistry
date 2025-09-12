CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ParentChildSymmetric AS
     
                SELECT 'Child-to-Parent' AS edge_type,
                       from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       COALESCE(context, 'n/a') AS context
                  FROM graph_registry.Edges_N_Object_N_Object_T_ChildToParent 
     
             UNION ALL
                
                SELECT 'Parent-to-Child'   AS edge_type,
                       to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       COALESCE(CONCAT(context, ' (mirror)'), 'n/a') AS context
                  FROM graph_registry.Edges_N_Object_N_Object_T_ChildToParent

             UNION ALL

                SELECT 'Child-to-Parent' AS edge_type,
                       from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       COALESCE(context, 'n/a') AS context
                  FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent 
     
             UNION ALL
                
                SELECT 'Parent-to-Child'   AS edge_type,
                       to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       COALESCE(CONCAT(context, ' (mirror)'), 'n/a') AS context
                  FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent

             UNION ALL

                SELECT 'Child-to-Parent' AS edge_type,
                       from_institution_id, from_object_type, from_object_id,
                         to_institution_id,   to_object_type,   to_object_id,
                       COALESCE(context, 'n/a') AS context
                  FROM graph_ontology.Edges_N_Object_N_Object_T_ChildToParent 
     
             UNION ALL
                
                SELECT 'Parent-to-Child'   AS edge_type,
                       to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id,
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       COALESCE(CONCAT(context, ' (mirror)'), 'n/a') AS context
                  FROM graph_ontology.Edges_N_Object_N_Object_T_ChildToParent;