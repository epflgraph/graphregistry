       -- Fetch data rows to be deleted (for rollback)
   SELECT [[data_columns]]
     FROM [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
LEFT JOIN        [[graphsearch_test]].Data_N_Object_T_PageProfile t
    USING (object_type, object_id)
    WHERE t.object_id IS NULL;
