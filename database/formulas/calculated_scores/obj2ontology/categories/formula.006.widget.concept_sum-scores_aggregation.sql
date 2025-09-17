
-- #---------------------------------------------------------------------------#
-- # [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores                       #
-- # (institution_id, object_type) = ('EPFL', 'Widget') #
-- #---------------------------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Widget')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
       SELECT t.institution_id AS institution_id, 
              t.object_type    AS object_type,
              t.object_id      AS object_id,
              l.from_id        AS category_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t.score) AS score
         FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection t
   INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
STRAIGHT_JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild  c ON t.concept_id = c.to_id
STRAIGHT_JOIN [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild l ON c.from_id    = l.to_id
        WHERE t.object_type = 'Widget'
          AND tf.to_process = 1
          AND se.to_process = 1
     GROUP BY t.institution_id, t.object_type, t.object_id, l.from_id
       HAVING score >= 1;

-- ======== Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Widget', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Widget')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Widget', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;