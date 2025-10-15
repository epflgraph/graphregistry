    SELECT 'Ont' AS institution_id, 'Concept' AS object_type, from_id AS object_id,
           'n/a' AS field_language, 'node_degree' AS field_name, COUNT(to_id) AS field_value
      FROM [[ontology]].Edges_N_Concept_N_Concept_T_Symmetric

        -- Check type flags
INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
        ON (tf.from_institution_id, tf.from_object_type, tf.to_institution_id, tf.to_object_type, tf.flag_type)
         = ('Ont', 'Concept', 'Ont', 'Concept', 'fields')

     WHERE tf.to_process = 1
  GROUP BY from_id
