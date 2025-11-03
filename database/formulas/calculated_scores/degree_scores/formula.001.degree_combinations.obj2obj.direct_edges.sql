                  -- Object-to-Object direct edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
                     (from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, degree, log_degree)
              SELECT e.from_institution_id, e.from_object_type, e.from_object_id, e.to_institution_id, e.to_object_type,
                             COUNT(DISTINCT e.to_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.to_object_id)) AS log_degree
                FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (e.from_institution_id, e.from_object_type, e.from_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.flag_type  = 'scores'
                 AND tf.to_process = 1
            GROUP BY e.from_institution_id, e.from_object_type, e.from_object_id, e.to_institution_id, e.to_object_type;
