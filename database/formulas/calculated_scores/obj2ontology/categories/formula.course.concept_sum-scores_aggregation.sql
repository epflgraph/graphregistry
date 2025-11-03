-- ========= Object type: Course
-- ========= Formula: 'concept sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)

      SELECT se.institution_id, se.object_type, se.object_id,
             l.from_id AS category_id,
             'concept sum-scores aggregation' AS calculation_type,
             SUM(t.score) AS score

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.institution_id = se.institution_id
         AND tf.object_type = 'Course'
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.object_type = 'Course'
         AND se.to_process  = 1

          -- Join traversal
  INNER JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection t
          ON se.institution_id = t.institution_id
		 AND se.object_type = t.object_type
         AND se.object_id = t.object_id

          -- Join ontology
  INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c
          ON t.concept_id = c.to_id
  INNER JOIN [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild l
          ON c.from_id = l.to_id

    GROUP BY t.object_id, l.from_id
      HAVING score >= .1;
