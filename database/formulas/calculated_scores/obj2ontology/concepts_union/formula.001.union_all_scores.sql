-- ==================== Re-create cache table with union of all scores (from custom methods)
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores
                       (object_type, object_id, concept_id, calculation_type, score, to_process, deleted)
                 SELECT object_type, object_id, concept_id, calculation_type, score, to_process, deleted
                   FROM (SELECT s.object_type, s.object_id, s.concept_id, s.calculation_type, s.score,
                                1 AS to_process, 0 AS deleted
                           FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores s
                          WHERE s.deleted = 0
                            AND s.to_process = 1
                            AND (     (s.object_type, s.calculation_type) = ('Course'  , 'average coverage over all lectures (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Lecture' ,     'percentage coverage in lecture (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Lecture' ,             'LLM keyword extraction (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Lecture' ,       'slide sum-scores aggregation (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('MOOC'    ,       'slide sum-scores aggregation (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('MOOC'    ,      'people sum-scores aggregation (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Person'  ,    'abstract sum-scores aggregation (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Unit'    ,    'abstract sum-scores aggregation (bounded)')
                                   OR (s.object_type, s.calculation_type) = ('Category',     'concept sum-scores aggregation (bounded)')
                                )
                            AND s.score >= 0.1
                        ) AS new
ON DUPLICATE KEY UPDATE score      = new.score,
                        to_process = new.to_process;

-- ==================== Re-create cache table with union of all scores (from concept detection)
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores
                       (object_type, object_id, concept_id, calculation_type, score, to_process, deleted)
                 SELECT object_type, object_id, concept_id, calculation_type, score, to_process, deleted
                   FROM (SELECT s.object_type, s.object_id, s.concept_id, CONCAT('concept detection on ', s.text_source) AS calculation_type, s.score,
                                1 AS to_process, 0 AS deleted
                           FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection s
                     INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                          USING (object_type, object_id)
                     INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                          USING (object_type)
                          WHERE s.record_deleted = 0
                            AND se.to_process = 1
                            AND tf.flag_type  = 'scores'
                            AND tf.to_process = 1
                            AND s.score >= 0.1
                        ) AS new
ON DUPLICATE KEY UPDATE score      = new.score,
                        to_process = new.to_process;

-- ==================== Re-create cache table with union of all scores (from manual mapping)
            INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores
                       (object_type, object_id, concept_id, calculation_type, score, to_process, deleted)
                 SELECT object_type, object_id, concept_id, calculation_type, score, to_process, deleted
                   FROM (SELECT s.object_type, s.object_id, s.concept_id, CONCAT('manual mapping on ', s.text_source) AS calculation_type, s.score,
                                1 AS to_process, 0 AS deleted
                           FROM [[registry]].Edges_N_Object_N_Concept_T_ManualMapping s
                     INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                          USING (object_type, object_id)
                     INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                          USING (object_type)
                          WHERE s.record_deleted = 0
                            AND se.to_process = 1
                            AND tf.flag_type  = 'scores'
                            AND tf.to_process = 1
                        ) AS new
ON DUPLICATE KEY UPDATE score      = new.score,
                        to_process = new.to_process;
