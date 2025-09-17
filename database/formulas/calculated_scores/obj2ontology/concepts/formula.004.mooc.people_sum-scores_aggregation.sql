-- #------------------------------------------------------------#
-- # [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'MOOC')           #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'people sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT t2.institution_id AS institution_id,
             'MOOC'            AS object_type,
             t2.mooc_id        AS object_id, 
             t2.concept_id     AS concept_id,
             'people sum-scores aggregation' AS calculation_type,
             SUM(t2.score)     AS score
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[graph_cache]].Traversal_N_MOOC_N_Publication_N_Concept_T_ConceptDetection t2
          ON (t1.institution_id, t1.object_id)
           = (t2.institution_id, t2.mooc_id)
       WHERE t1.object_type = 'MOOC'
         AND t1.to_process = 1
    GROUP BY t2.institution_id, t2.mooc_id, t2.concept_id
      HAVING score >= 1;

-- ============= Calculate average score for 'people sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'MOOC', 'people sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'people sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'people sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'MOOC'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'MOOC', 'people sum-scores aggregation');
