   DELETE a
     FROM [[graphsearch_test]].Data_N_Object_T_PageProfile a
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile b
    USING (institution_id, object_type, object_id)
    WHERE a.object_type NOT IN ('CuratedArea', 'Category', 'Concept', 'Lecture', 'Slide', 'Transcript')
      AND b.object_id IS NULL;

   DELETE a
     FROM [[graph_cache_test]].Data_N_Object_T_PageProfile a
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile b
    USING (institution_id, object_type, object_id)
    WHERE a.object_type NOT IN ('CuratedArea', 'Category', 'Concept', 'Lecture', 'Slide', 'Transcript')
      AND b.object_id IS NULL;
