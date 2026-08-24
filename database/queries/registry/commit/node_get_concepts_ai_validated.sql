    -- Get node AI validated concepts from registry
SELECT concept_id, score
  FROM [[registry]].Edges_N_Object_N_Concept_T_LLMPostValidated
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
