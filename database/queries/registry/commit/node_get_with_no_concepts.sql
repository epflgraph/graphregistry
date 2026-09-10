             -- Get all eligible node keys with no active detected concepts
SELECT DISTINCT n.object_type, n.object_id
           FROM [[registry]].Nodes_N_Object n
      LEFT JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection c
             ON n.object_type    = c.object_type
            AND n.object_id      = c.object_id
            AND c.record_deleted = 0
     INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged fc
             ON n.object_type = fc.object_type
            AND n.object_id   = fc.object_id
     INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
             ON n.object_type = tf.object_type
          WHERE n.object_type LIKE '[[object_type]]'
            AND n.object_id   LIKE '[[id_pattern]]'
            AND n.raw_text    IS NOT NULL
            AND n.record_deleted = 0
            AND c.concept_id  IS NULL
            AND tf.to_process = 1
