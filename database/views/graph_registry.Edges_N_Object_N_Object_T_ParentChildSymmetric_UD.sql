CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ParentChildSymmetric_UD AS

                SELECT 'Child-to-Parent' AS edge_type,
                       c2p.from_institution_id, c2p.from_object_type, c2p.from_object_id,
                         c2p.to_institution_id,   c2p.to_object_type,   c2p.to_object_id,
                       COALESCE(c2p.context, 'n/a') AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent c2p
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
       
             UNION ALL

                SELECT 'Parent-to-Child' AS edge_type,
                       c2p.to_institution_id   AS from_institution_id,
                       c2p.to_object_type      AS from_object_type,
                       c2p.to_object_id        AS from_object_id,
                       c2p.from_institution_id AS to_institution_id,
                       c2p.from_object_type    AS to_object_type,
                       c2p.from_object_id      AS to_object_id,
                       COALESCE(CONCAT(c2p.context, ' (mirror)'), 'n/a') AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent c2p
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1

             UNION ALL

                SELECT 'Child-to-Parent' AS edge_type,
                       c2p.from_institution_id, c2p.from_object_type, c2p.from_object_id,
                         c2p.to_institution_id,   c2p.to_object_type,   c2p.to_object_id,
                       COALESCE(c2p.context, 'n/a') AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent c2p
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
       
             UNION ALL

                SELECT 'Parent-to-Child' AS edge_type,
                       c2p.to_institution_id   AS from_institution_id,
                       c2p.to_object_type      AS from_object_type,
                       c2p.to_object_id        AS from_object_id,
                       c2p.from_institution_id AS to_institution_id,
                       c2p.from_object_type    AS to_object_type,
                       c2p.from_object_id      AS to_object_id,
                       COALESCE(CONCAT(c2p.context, ' (mirror)'), 'n/a') AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent c2p
                 USING (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1;