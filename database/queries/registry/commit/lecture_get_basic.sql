    -- Get basic node data from lectures
SELECT object_title, text_source, raw_text
  FROM [[lectures]].Nodes_N_Object
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]');
