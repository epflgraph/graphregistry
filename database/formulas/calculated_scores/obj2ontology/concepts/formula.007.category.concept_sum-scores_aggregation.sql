-- #------------------------------------------------------------#
-- # [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('Ont', 'Category')        #
-- #------------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Category')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
             (institution_id, object_type, object_id, concept_id, calculation_type, score)
       SELECT 'Ont'             AS institution_id,
              'Category'        AS object_type,
              t1.from_id        AS object_id,
              t3.to_id          AS concept_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t3.score)     AS score
         FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild t1
STRAIGHT_JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t2
           ON t1.to_id = t2.from_id
STRAIGHT_JOIN [[ontology]].Edges_N_Concept_N_Concept_T_Symmetric t3
           ON t2.to_id = t3.from_id
   INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
           ON (se.institution_id, se.object_type, se.object_id) = ('Ont', 'Category', t1.from_id)
   INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
           ON (se.institution_id, se.object_type) = (tf.institution_id, tf.object_type)
        WHERE se.to_process = 1
          AND tf.to_process = 1
     GROUP BY t1.from_id, t3.to_id
       HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('Ont', 'Category', 'concept sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'Category')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT s.institution_id, s.object_type, s.object_id, s.concept_id,
             'concept sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
        FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores s      
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON (se.institution_id, se.object_type, se.object_id) = ('Ont', 'Category', s.object_id)
  INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
          ON (se.institution_id, se.object_type) = (tf.institution_id, tf.object_type)
       WHERE (s.institution_id, s.object_type, s.calculation_type) = ('Ont', 'Category', 'concept sum-scores aggregation')
         AND se.to_process = 1
         AND tf.to_process = 1;
