    -- Delete node from basic nodes table
UPDATE [[registry]].Nodes_N_Object
   SET record_deleted = 1
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]');

    -- Delete node from page profile table
UPDATE [[registry]].Data_N_Object_T_PageProfile
   SET record_deleted = 1
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]');

    -- Delete node from custom fields table
UPDATE [[registry]].Data_N_Object_T_CustomFields
   SET record_deleted = 1
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]');

    -- Delete node from concept detection table
UPDATE [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection
   SET record_deleted = 1
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]');
