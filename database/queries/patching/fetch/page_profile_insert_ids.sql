       -- Fetch ids of rows to INSERT
   SELECT t.object_type, t.object_id
     FROM        [[graphsearch_test]].Data_N_Object_T_PageProfile t
LEFT JOIN [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
    USING (object_type, object_id)
    WHERE p.object_id IS NULL;
