    -- List non-deleted nodes in registry
SELECT institution_id, object_type, object_id
  FROM [[registry]].Nodes_N_Object
 WHERE object_type = '[[object_type]]'
   AND object_id LIKE '[[id_pattern]]'
   AND record_deleted = 0;
