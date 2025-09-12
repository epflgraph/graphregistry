
-- =================================== --
-- ===== Refresh Semantic Scores ===== -- 
-- =================================== --

-- ================= Re-create cache table with union of all scores
        REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores
                    (institution_id, object_type, object_id, concept_id, calculation_type, score)

              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, s.calculation_type, s.score
                FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores s
          INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
                 AND (    (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Course' ,    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Lecture',    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Lecture',          'LLM keyword extraction (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'MOOC'   ,    'slide sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'MOOC'   ,   'people sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Person' , 'abstract sum-scores aggregation (bounded)')
                       OR (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Unit'   , 'abstract sum-scores aggregation (bounded)')
                     )
                 AND s.score >= 0.1
                 
           UNION ALL

              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, CONCAT('concept detection on ', s.text_source) AS calculation_type, s.score
                FROM graph_registry.Edges_N_Object_N_Concept_T_ConceptDetection s
          INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
                 AND s.score >= 0.1
               
           UNION ALL
               
              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id, CONCAT('manual mapping on ', s.text_source) AS calculation_type, s.score
                FROM graph_registry.Edges_N_Object_N_Concept_T_ManualMapping s
          INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;
                 

-- ================= Extract all (institution_id, object_type, object_id, concept_id) tuples
DROP TABLE IF EXISTS graph_cache.Edges_N_Object_N_Concept_T_Tuples;
        CREATE TABLE graph_cache.Edges_N_Object_N_Concept_T_Tuples AS
     SELECT DISTINCT institution_id, object_type, object_id, concept_id
                FROM graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores s
          INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;
         
-- ================= Create indices to optimise the next query
         ALTER TABLE graph_cache.Edges_N_Object_N_Concept_T_Tuples
     ADD PRIMARY KEY (institution_id, object_type, object_id, concept_id),
             ADD KEY join_id (object_id, concept_id),
             ADD KEY institution_id (institution_id),
             ADD KEY object_type (object_type),
             ADD KEY object_id (object_id),
             ADD KEY concept_id (concept_id);


-- ================= Put all calculation types in columns [4 min]
        REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_ScoringMatrix
                    (institution_id, object_type, object_id, concept_id, score_1, score_2, score_3)
        
              SELECT t0.institution_id, t0.object_type, t0.object_id, t0.concept_id, --                                                                                             SCORING MATRIX
                     --                                                             Course     Lecture     MOOC        Person     Publication  Startup     Unit                   ← NODE TYPES   ↓ SCORE TYPES
                     COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t11.score, t21.score), t31.score), t41.score), t51.score), t61.score), t71.score), t81.score)   AS score_1, -- Score type 1
                     COALESCE(COALESCE(COALESCE(                                    t12.score, t22.score), t32.score)                                    , t72.score)               AS score_2, -- Score type 2
                                                                                    t13.score                                                                                       AS score_3  -- Score type 3
                
                FROM graph_cache.Edges_N_Object_N_Concept_T_Tuples t0
                
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t11
                  ON (t0.object_id, t0.concept_id) = (t11.object_id, t11.concept_id)
                 AND (t11.institution_id, t11.object_type, t11.calculation_type) = ('EPFL', 'Course', 'manual mapping on course page description')
                
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t12
                  ON (t0.object_id, t0.concept_id) = (t12.object_id, t12.concept_id)
                 AND (t12.institution_id, t12.object_type, t12.calculation_type) = ('EPFL', 'Course', 'slide sum-scores aggregation (bounded)')

           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t13
                  ON (t0.object_id, t0.concept_id) = (t13.object_id, t13.concept_id)
                 AND (t13.institution_id, t13.object_type, t13.calculation_type) = ('EPFL', 'Course', 'concept detection on course page description')

           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t21
                  ON (t0.object_id, t0.concept_id) = (t21.object_id, t21.concept_id)
                 AND (t21.institution_id, t21.object_type, t21.calculation_type) = ('EPFL', 'Lecture', 'LLM keyword extraction (bounded)')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t22
                  ON (t0.object_id, t0.concept_id) = (t22.object_id, t22.concept_id)
                 AND (t22.institution_id, t22.object_type, t22.calculation_type) = ('EPFL', 'Lecture', 'slide sum-scores aggregation (bounded)')
                
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t31
                  ON (t0.object_id, t0.concept_id) = (t31.object_id, t31.concept_id)
                 AND (t31.institution_id, t31.object_type, t31.calculation_type) = ('EPFL', 'MOOC', 'slide sum-scores aggregation (bounded)')

           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t32
                  ON (t0.object_id, t0.concept_id) = (t32.object_id, t32.concept_id)
                 AND (t32.institution_id, t32.object_type, t32.calculation_type) = ('EPFL', 'MOOC', 'people sum-scores aggregation (bounded)')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t41
                  ON (t0.object_id, t0.concept_id) = (t41.object_id, t41.concept_id)
                 AND (t41.institution_id, t41.object_type, t41.calculation_type) = ('EPFL', 'Person', 'abstract sum-scores aggregation (bounded)')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t51
                  ON (t0.object_id, t0.concept_id) = (t51.object_id, t51.concept_id)
                 AND (t51.institution_id, t51.object_type, t51.calculation_type) = ('EPFL', 'Publication', 'concept detection on abstract')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t61
                  ON (t0.object_id, t0.concept_id) = (t61.object_id, t61.concept_id)
                 AND (t61.institution_id, t61.object_type, t61.calculation_type) = ('EPFL', 'Startup', 'concept detection on startup website')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t71
                  ON (t0.object_id, t0.concept_id) = (t71.object_id, t71.concept_id)
                 AND (t71.institution_id, t71.object_type, t71.calculation_type) = ('EPFL', 'Unit', 'manual mapping on unit website')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t72
                  ON (t0.object_id, t0.concept_id) = (t72.object_id, t72.concept_id)
                 AND (t72.institution_id, t72.object_type, t72.calculation_type) = ('EPFL', 'Unit', 'abstract sum-scores aggregation (bounded)')
                 
           LEFT JOIN graph_cache.Edges_N_Object_N_Concept_T_UnionAllScores  t81
                  ON (t0.object_id, t0.concept_id) = (t81.object_id, t81.concept_id)
                 AND (t81.institution_id, t81.object_type, t81.calculation_type) = ('EPFL', 'Widget', 'concept detection on quiz description');


-- ================= Average out all scores and create final table [0 min]
        REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_FinalScores
                    (institution_id, object_type, object_id, concept_id, score)
              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id,
                     (2/(1 + exp(-2 * (COALESCE(s.score_1,0)+COALESCE(s.score_2,0)+COALESCE(s.score_3,0)) )) - 1) AS score
                FROM graph_cache.Edges_N_Object_N_Concept_T_Tuples t
          INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_ScoringMatrix s
               USING (institution_id, object_type, object_id, concept_id);

