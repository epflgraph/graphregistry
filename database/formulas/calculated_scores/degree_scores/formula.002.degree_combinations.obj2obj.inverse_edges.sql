                  -- Object-to-Object inverse edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.to_institution_id   AS from_institution_id,
                     e.to_object_type      AS from_object_type,
                     e.to_object_id        AS from_object_id,
                     e.from_institution_id AS to_institution_id,
                     e.from_object_type    AS to_object_type,
                             COUNT(DISTINCT e.from_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.from_object_id)) AS log_degree
                FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e 
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (  se.institution_id,   se.object_type,   se.object_id)
                   = (e.to_institution_id, e.to_object_type, e.to_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.to_institution_id, e.to_object_type, e.to_object_id, e.from_institution_id, e.from_object_type;

