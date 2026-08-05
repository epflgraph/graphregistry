                  -- Lecture-to-slide edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
                     (from_object_type, from_object_id, to_object_type, degree, log_degree)
              SELECT e.to_object_type, e.to_object_id, e.from_object_type,
                             COUNT(DISTINCT e.from_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.from_object_id)) AS log_degree
                 FROM [[lectures]].Edges_N_Object_N_Object_T_ChildToParent e
           INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                   ON (  se.object_type,   se.object_id)
                    = (e.to_object_type, e.to_object_id)
           INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                USING (object_type)
                WHERE (e.from_object_type, e.to_object_type) = ('Slide', 'Lecture')
                  AND e.record_deleted = 0
                  AND se.to_process = 1
                  AND tf.flag_type  = 'scores'
                  AND tf.to_process = 1
             GROUP BY e.to_object_type, e.to_object_id, e.from_object_type;
