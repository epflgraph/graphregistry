-- ========= Object type: Person
-- ========= Formula: 'abstract sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
             (object_type, object_id, concept_id, calculation_type, score, to_process, deleted)
      SELECT 'Person'   AS object_type,
             person_id  AS object_id,
             concept_id AS concept_id,
             'abstract sum-scores aggregation' AS calculation_type,
             SUM(score) AS score, 1 AS to_process, 0 AS deleted
         FROM [[traversals]].Person_Publication_Concept__ConceptDetection
        WHERE to_process = 1
          AND deleted = 0
     GROUP BY person_id, concept_id
       HAVING score >= .1;

-- ============= Calculate average score for 'abstract sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
      USING (object_type)
      WHERE (object_type, calculation_type) = ('Person', 'abstract sum-scores aggregation')
        AND score >= .1
        AND deleted = 0
        AND tf.flag_type  = 'scores'
        AND tf.to_process = 1
);

-- ========= Formula: 'abstract sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (object_type, object_id, concept_id, calculation_type, score, to_process, deleted)
      SELECT object_type, object_id, concept_id,
             'abstract sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-t2.score/(4*@avg_score))) - 1) AS score, 1 AS to_process, 0 AS deleted
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
       USING (object_type)
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (object_type, object_id)
       WHERE t1.object_type = 'Person'
         AND t1.to_process  = 1
         AND t1.deleted     = 0
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND (t2.object_type, t2.calculation_type) = ('Person', 'abstract sum-scores aggregation')
         AND t2.deleted = 0
         AND t2.score >= .1;
