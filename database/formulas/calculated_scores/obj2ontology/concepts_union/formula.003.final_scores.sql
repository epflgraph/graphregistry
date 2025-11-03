-- ==================== Average out all scores and create final table
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
                       (institution_id, object_type, object_id, concept_id, score, to_process)
                 SELECT institution_id, object_type, object_id, concept_id, score, to_process
                   FROM (SELECT institution_id, object_type, object_id, concept_id,
                                (2/(1 + exp(-2 * (COALESCE(score_1,0)+COALESCE(score_2,0)+COALESCE(score_3,0)) )) - 1) AS score,
                                to_process
                           FROM [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix AS d
                          WHERE to_process = 1
                        ) AS new
ON DUPLICATE KEY UPDATE score      = new.score,
                        to_process = new.to_process;
