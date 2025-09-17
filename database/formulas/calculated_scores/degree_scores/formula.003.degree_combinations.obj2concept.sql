                  -- Object-to-Concept edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.institution_id   AS from_institution_id,
                     e.object_type      AS from_object_type,
                     e.object_id        AS from_object_id,
                     'Ont'              AS to_institution_id,
                     'Concept'          AS to_object_type,
                             COUNT(DISTINCT e.concept_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.concept_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.institution_id, e.object_type, e.object_id;
