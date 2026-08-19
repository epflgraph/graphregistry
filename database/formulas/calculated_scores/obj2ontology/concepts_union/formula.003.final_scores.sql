-- ==================== Average out all scores and create final table
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
                       (object_type, object_id, concept_id, score, to_process, deleted)
                 SELECT object_type, object_id, concept_id, score, to_process, deleted
                   FROM (SELECT object_type, object_id, concept_id,
                                (2/(1 + exp(-2 * (COALESCE(score_1,0)+COALESCE(score_2,0)+COALESCE(score_3,0)) )) - 1) AS score,
                                1 AS to_process,
                                0 AS deleted
                           FROM [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix AS d
                          WHERE deleted = 0
                            AND to_process = 1
                        ) AS new
ON DUPLICATE KEY UPDATE score      = new.score,
                        to_process = new.to_process;
