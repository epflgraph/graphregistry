    -- Get basic node data from registry
SELECT object_title, text_source, raw_text
  FROM [[registry]].Nodes_N_Object
 WHERE (institution_id, object_type, object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
   AND record_deleted = 0;
