       -- Fetch ids of rows to DELETE
   SELECT p.object_type, p.object_id
     FROM [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
LEFT JOIN        [[graphsearch_test]].Data_N_Object_T_PageProfile t
    USING (object_type, object_id)
    WHERE t.object_id IS NULL;
