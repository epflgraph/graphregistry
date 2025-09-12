  SELECT 'Ont' AS institution_id, 'Concept' AS object_type, from_id,
         'n/a' AS field_language, 'node_degree' AS field_name, COUNT(to_id) AS field_value
    FROM [[ontology]].Edges_N_Concept_N_Concept_T_Symmetric
GROUP BY from_id