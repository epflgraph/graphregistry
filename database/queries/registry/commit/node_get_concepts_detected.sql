    -- Get node detected concepts from registry
SELECT concept_id, score
  FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
   AND record_deleted = 0;
