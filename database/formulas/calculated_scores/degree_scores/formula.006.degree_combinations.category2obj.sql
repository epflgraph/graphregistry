                  -- Category-to-Object edges [checked]
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT 'Ont'              AS from_institution_id,
                     'Category'          AS from_object_type,
                     e.category_id       AS from_object_id,
                     e.institution_id   AS to_institution_id,
                     e.object_type      AS to_object_type,
                             COUNT(DISTINCT e.object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.object_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.category_id, e.institution_id, e.object_type;
