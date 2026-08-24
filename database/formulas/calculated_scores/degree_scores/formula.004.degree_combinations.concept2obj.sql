                  -- Concept-to-Object edges [checked]
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
                     (from_object_type, from_object_id, to_object_type, degree, log_degree, deleted)
              SELECT 'Concept'          AS from_object_type,
                     e.concept_id       AS from_object_id,
                     e.object_type      AS to_object_type,
                             COUNT(DISTINCT e.object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.object_id)) AS log_degree,
                     0 AS deleted
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (object_type)
               WHERE se.to_process = 1
                 AND se.deleted = 0
                 AND e.deleted = 0
                 AND tf.flag_type = 'scores'
                 AND tf.to_process = 1
            GROUP BY e.concept_id, e.object_type;
