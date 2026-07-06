             -- Get all eligible node keys with no detected concepts
SELECT DISTINCT institution_id, object_type, object_id
           FROM [[registry]].Nodes_N_Object n
      LEFT JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection c
          USING (institution_id, object_type, object_id)
     INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged fc
          USING (institution_id, object_type, object_id)
     INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
          USING (institution_id, object_type)
          WHERE n.object_type LIKE '[[object_type]]'
            AND n.object_id   LIKE '[[id_pattern]]'
            AND n.raw_text    IS NOT NULL
            AND c.concept_id  IS NULL
            AND fc.to_process = 1
            AND tf.to_process = 1
