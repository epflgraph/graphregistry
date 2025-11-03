                  -- Object-to-Category edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
                     (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, degree, log_degree)
              SELECT e.institution_id   AS from_institution_id,
                     e.object_type      AS from_object_type,
                     e.object_id        AS from_object_id,
                     'Ont'              AS to_institution_id,
                     'Category'          AS to_object_type,
                             COUNT(DISTINCT e.category_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.category_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.flag_type  = 'scores'
                 AND tf.to_process = 1
            GROUP BY e.institution_id, e.object_type, e.object_id;
