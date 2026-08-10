-- ========= Object type: Person
-- ========= Formula: 'concept sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
             (object_type, object_id, category_id, calculation_type, score, to_process)

      SELECT se.object_type, se.object_id,
             l.from_id AS category_id,
             'concept sum-scores aggregation' AS calculation_type,
             SUM(t.score) AS score, 1 AS to_process

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.object_type = 'Person'
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.object_type = 'Person'
         AND se.to_process  = 1

          -- Join traversal
  INNER JOIN [[traversals]].Person_Publication_Concept__ConceptDetection t
          ON se.object_id = t.person_id
         AND t.deleted = 0

          -- Join ontology
  INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c
          ON t.concept_id = c.to_id
  INNER JOIN [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild l
          ON c.from_id = l.to_id

    GROUP BY t.person_id, l.from_id
      HAVING score >= 1;
