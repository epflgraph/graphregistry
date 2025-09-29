-- ================= Calculate max log degrees
DROP TABLE IF EXISTS [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees;
        CREATE TABLE [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees AS
              SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, MAX(log_degree) AS max_log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
            GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type;

-- ================= Add indices to optimise the following query
         ALTER TABLE [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees
     ADD PRIMARY KEY (from_institution_id, from_object_type, to_institution_id, to_object_type),
             ADD KEY from_institution_id (from_institution_id),
             ADD KEY from_object_type    (from_object_type),
             ADD KEY to_institution_id   (to_institution_id),
             ADD KEY to_object_type      (to_object_type);

-- ================= Calculate normalised log scores
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees
              SELECT d.from_institution_id, d.from_object_type, d.from_object_id, d.to_institution_id, d.to_object_type, d.degree, d.log_degree, (d.log_degree / t.max_log_degree) AS norm_log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations d
          INNER JOIN [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees t
               USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (d.from_institution_id, d.from_object_type, d.from_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;
