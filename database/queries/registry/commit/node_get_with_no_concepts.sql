             -- Get all eligible node keys with no detected concepts
SELECT DISTINCT institution_id, object_type, object_id
           FROM [[registry]].Nodes_N_Object n
      LEFT JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection c
          USING (institution_id, object_type, object_id)
          WHERE n.object_type LIKE '[[object_type]]'
            AND n.object_id LIKE '[[id_pattern]]'
            AND n.raw_text IS NOT NULL
            AND c.concept_id IS NULL;