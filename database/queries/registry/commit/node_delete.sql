DELETE -- Delete node from basic nodes table
  FROM [[registry]].Nodes_N_Object
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');

DELETE -- Delete node from page profile table
  FROM [[registry]].Data_N_Object_T_PageProfile
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');

DELETE -- Delete node from custom fields table
  FROM [[registry]].Data_N_Object_T_CustomFields
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');

DELETE -- Delete node from concept detection table
  FROM [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
