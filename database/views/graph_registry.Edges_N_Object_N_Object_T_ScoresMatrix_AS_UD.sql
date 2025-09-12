CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ScoresMatrix_AS_UD AS
                SELECT from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id,
                       IF(to_object_type='Concept', score, (2/(1 + EXP(-score/(4 * avg_score))) - 1)) AS score, to_process
                  FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
             LEFT JOIN graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE IF(to_object_type='Concept', score, (2/(1 + EXP(-score/(4 * avg_score))) - 1)) >= 0.1
                   AND to_process = 1;