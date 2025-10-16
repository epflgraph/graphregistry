-- ================= Extract all (institution_id, object_type, object_id, concept_id) tuples
      TRUNCATE TABLE [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples;
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples
              SELECT institution_id, object_type, object_id, concept_id
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores s
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.flag_type  = 'scores'
                 AND tf.to_process = 1;
