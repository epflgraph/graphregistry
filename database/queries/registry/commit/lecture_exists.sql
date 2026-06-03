    -- Evaluate if node exists in registry
SELECT COUNT(*) > 0 AS node_exists
  FROM [[lectures]].Nodes_N_Object
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', 'Lecture', '[[lecture_id]]');
