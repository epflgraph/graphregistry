-- ==================== Put all calculation types in columns
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix
                       (object_type, object_id, concept_id, score_1, score_2, score_3, to_process)
                 SELECT object_type, object_id, concept_id, score_1, score_2, score_3, to_process
                   FROM (SELECT object_type, object_id, concept_id,

                      CASE WHEN (object_type = 'Person'      AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Course'      AND calculation_type = 'manual mapping on user input')
                             OR (object_type = 'Lecture'     AND calculation_type = 'LLM keyword extraction (bounded)')
                             OR (object_type = 'MOOC'        AND calculation_type = 'slide sum-scores aggregation (bounded)')
                             OR (object_type = 'Person'      AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Publication' AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Startup'     AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Unit'        AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Widget'      AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Category'    AND calculation_type = 'concept sum-scores aggregation (bounded)')
                             OR (object_type = 'Exercise'    AND calculation_type = 'manual mapping on exercise description')
                             OR (object_type = 'Notebook'    AND calculation_type = 'manual mapping on notebook description')
                           THEN score END AS score_1,

                      CASE WHEN (object_type = 'Person'      AND calculation_type = 'abstract sum-scores aggregation (bounded)')
                             OR (object_type = 'Course'      AND calculation_type = 'average coverage over all lectures (bounded)')
                             OR (object_type = 'Lecture'     AND calculation_type = 'percentage coverage in lecture (bounded)')
                             OR (object_type = 'MOOC'        AND calculation_type = 'people sum-scores aggregation (bounded)')
                             OR (object_type = 'Unit'        AND calculation_type = 'manual mapping on user input')
                           THEN score END AS score_2,

                      CASE WHEN (object_type = 'Course'      AND calculation_type = 'concept detection on user input')
                             OR (object_type = 'Lecture'     AND calculation_type = 'slide sum-scores aggregation (bounded)')
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
