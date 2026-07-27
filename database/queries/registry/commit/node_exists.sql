    -- Evaluate if node exists in registry
SELECT COUNT(*) > 0 AS node_exists
  FROM [[registry]].Nodes_N_Object
 WHERE (object_type, object_id) = ('[[object_type]]', '[[object_id]]')
   AND record_deleted = 0;
