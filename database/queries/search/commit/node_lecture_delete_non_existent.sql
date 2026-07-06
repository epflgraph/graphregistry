   DELETE a
     FROM [[graphsearch_test]].Data_N_Object_T_PageProfile a
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile b
    USING (institution_id, object_type, object_id)
    WHERE a.object_type = 'Lecture'
      AND b.object_id IS NULL;

   DELETE a
     FROM [[graph_cache_test]].Data_N_Object_T_PageProfile a
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile b
    USING (institution_id, object_type, object_id)
    WHERE a.object_type = 'Lecture'
      AND b.object_id IS NULL;
