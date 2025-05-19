 CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ScoresMatrix_GBC_UD AS
                       
                 SELECT e1.institution_id  AS from_institution_id,
                       
                        e1.object_type     AS from_object_type,
                        e1.object_id       AS from_object_id,
                        e2.institution_id  AS to_institution_id,
                        e2.object_type     AS to_object_type,
                        e2.object_id       AS to_object_id,
                       
                        SUM(e1.score*e2.score) AS score, 1 AS to_process
                       
                   FROM graph_registry.Operations_N_Object_T_ScoresExpired s1
                       
             INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_FinalScores e1
                     ON (s1.institution_id, s1.object_type, s1.object_id)
                      = (e1.institution_id, e1.object_type, e1.object_id)
                       
             INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_FinalScores e2 
                     ON e1.concept_id = e2.concept_id
                       
              LEFT JOIN graph_registry.Operations_N_Object_N_Object_T_ScoresExpired s2
                     ON (e1.institution_id, e1.object_type, e1.object_id, e2.institution_id, e2.object_type, e2.object_id)
                      = (s2.from_institution_id, s2.from_object_type, s2.from_object_id, s2.to_institution_id, s2.to_object_type, s2.to_object_id)
                       
             INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                     ON (e1.institution_id, e1.object_type, e2.institution_id, e2.object_type)
                      = (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type)
                       
             INNER JOIN graph_registry.Nodes_N_Object n1
                     ON (e1.institution_id, e1.object_type, e1.object_id)
                      = (n1.institution_id, n1.object_type, n1.object_id)
                       
             INNER JOIN graph_registry.Nodes_N_Object n2
                     ON (e2.institution_id, e2.object_type, e2.object_id)
                      = (n2.institution_id, n2.object_type, n2.object_id)
                       
                  WHERE s1.institution_id = 'EPFL'
                    AND e1.institution_id = 'EPFL'
                    AND e2.institution_id = 'EPFL'
                       
                    AND ((e1.object_type = e2.object_type AND e1.object_id < e2.object_id) OR (e1.object_type != e2.object_type))
                       
                    AND e1.score >= 0.1
                    AND e2.score >= 0.1
                       
                    AND (s1.to_process = 1 OR COALESCE(s2.to_process, 1) = 1)
                    AND tf.flag_type = 'scores'
                    AND tf.to_process = 1
                       
               GROUP BY e1.object_id, e2.object_id
                       
                 HAVING COUNT(DISTINCT e1.concept_id) >= 4
                    AND SUM(e1.score*e2.score) >= 0.1;
