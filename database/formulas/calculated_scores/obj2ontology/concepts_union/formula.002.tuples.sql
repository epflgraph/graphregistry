-- ================= Extract all (institution_id, object_type, object_id, concept_id) tuples
DROP TABLE IF EXISTS [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples;
        CREATE TABLE [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples AS
     SELECT DISTINCT institution_id, object_type, object_id, concept_id
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores s
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;

-- ================= Create indices to optimise the next query
         ALTER TABLE [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples
     ADD PRIMARY KEY (institution_id, object_type, object_id, concept_id),
             ADD KEY join_id (object_id, concept_id),
             ADD KEY institution_id (institution_id),
             ADD KEY object_type (object_type),
             ADD KEY object_id (object_id),
             ADD KEY concept_id (concept_id);
