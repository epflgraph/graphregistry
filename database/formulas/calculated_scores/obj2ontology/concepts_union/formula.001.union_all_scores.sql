
-- ================= Re-create cache table with union of all scores
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores
                    (institution_id, object_type, object_id, concept_id, calculation_type, score)

              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, s.calculation_type, s.score
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores s
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
                 AND (    (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Course'  ,    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Lecture' ,    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Lecture' ,          'LLM keyword extraction (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'MOOC'    ,    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'MOOC'    ,   'people sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Person'  , 'abstract sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Unit'    , 'abstract sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('Ont' , 'Category',  'concept sum-scores aggregation (bounded)')
                     )
                 AND s.score >= 0.1
                 
           UNION ALL

              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, CONCAT('concept detection on ', s.text_source) AS calculation_type, s.score
                FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection s
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
                 AND s.score >= 0.1
               
           UNION ALL
               
              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, CONCAT('manual mapping on ', s.text_source) AS calculation_type, s.score
                FROM [[registry]].Edges_N_Object_N_Concept_T_ManualMapping s
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;
