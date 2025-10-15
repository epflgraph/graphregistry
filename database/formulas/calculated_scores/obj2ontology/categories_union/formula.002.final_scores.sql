
-- ========= Object type: Person
-- ========= Formula: 'concept sum-scores aggregation'
REPLACE INTO [[[[graph_cache]]]].Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
      SELECT 'EPFL'       AS institution_id,
             'Person'     AS object_type,
             t.person_id  AS object_id,
             l.from_id    AS category_id,
             'concept sum-scores aggregation' AS calculation_type,
             SUM(t.score) AS score

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.institution_id = se.institution_id
         AND tf.object_type = 'Person'
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.object_type = 'Person'
         AND se.to_process  = 1

          -- Join traversal
  INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection t
          ON se.institution_id = t.institution_id
         AND se.object_id = t.person_id

          -- Join ontology
  INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c
          ON t.concept_id = c.to_id
  INNER JOIN [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild l
          ON c.from_id = l.to_id

    GROUP BY t.person_id, l.from_id
      HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores cs
 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
         ON cs.institution_id = tf.institution_id
        AND cs.object_type = 'Person'
        AND cs.calculation_type = 'concept sum-scores aggregation'
        AND tf.object_type = 'Person'
        AND tf.flag_type   = 'scores'
        AND tf.to_process  = 1
 );

-- ========= Formula: 'concept sum-scores aggregation (bounded)'
REPLACE INTO [[[[graph_cache]]]].Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
      SELECT cs.institution_id, cs.object_type, cs.object_id, cs.category_id,
             (2/(1 + EXP(-cs.score/(4*@avg_score))) - 1) AS score

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.institution_id = se.institution_id
         AND tf.object_type = 'Person'
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.object_type = 'Person'
         AND se.to_process  = 1

          -- Join scores table
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores cs
          ON se.institution_id = cs.institution_id
         AND cs.object_type = 'Person'
         AND se.object_id = cs.object_id
         AND cs.calculation_type = 'concept sum-scores aggregation';
