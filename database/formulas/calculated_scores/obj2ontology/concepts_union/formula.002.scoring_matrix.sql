-- ==================== Put all calculation types in columns
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix
                       (institution_id, object_type, object_id, concept_id, score_1, score_2, score_3, to_process)
                 SELECT institution_id, object_type, object_id, concept_id, score_1, score_2, score_3, to_process
                   FROM (SELECT institution_id, object_type, object_id, concept_id,

                      CASE WHEN (object_type = 'Person'      AND calculation_type = 'concept detection on biography')
                             OR (object_type = 'Course'      AND calculation_type = 'manual mapping on course page description')
                             OR (object_type = 'Lecture'     AND calculation_type = 'LLM keyword extraction (bounded)')
                             OR (object_type = 'MOOC'        AND calculation_type = 'slide sum-scores aggregation (bounded)')
                             OR (object_type = 'Person'      AND calculation_type = 'concept detection on biography')
                             OR (object_type = 'Publication' AND calculation_type = 'concept detection on abstract')
                             OR (object_type = 'Startup'     AND calculation_type = 'concept detection on startup website')
                             OR (object_type = 'Unit'        AND calculation_type = 'concept detection on unit website')
                             OR (object_type = 'Widget'      AND calculation_type = 'concept detection on quiz description')
                             OR (object_type = 'Category'    AND calculation_type = 'concept sum-scores aggregation (bounded)')
                           THEN score END AS score_1,

                      CASE WHEN (object_type = 'Person'      AND calculation_type = 'abstract sum-scores aggregation (bounded)')
                             OR (object_type = 'Course'      AND calculation_type = 'slide sum-scores aggregation (bounded)')
                             OR (object_type = 'Lecture'     AND calculation_type = 'slide sum-scores aggregation (bounded)')
                             OR (object_type = 'MOOC'        AND calculation_type = 'people sum-scores aggregation (bounded)')
                             OR (object_type = 'Unit'        AND calculation_type = 'manual mapping on unit website')
                           THEN score END AS score_2,

                      CASE WHEN (object_type = 'Course'      AND calculation_type = 'concept detection on course page description')
                             OR (object_type = 'Unit'        AND calculation_type = 'abstract sum-scores aggregation (bounded)')
                           THEN score END AS score_3,

                                to_process
                           FROM [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores
                          WHERE to_process = 1
                        ) AS new
ON DUPLICATE KEY UPDATE score_1    = new.score_1,
                        score_2    = new.score_2,
                        score_3    = new.score_3,
                        to_process = new.to_process;
