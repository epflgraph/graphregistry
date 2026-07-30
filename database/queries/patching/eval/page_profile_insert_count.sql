       -- Count number of rows to INSERT per object type
   SELECT t.object_type, COUNT(*) AS n
     FROM        [[graphsearch_test]].Data_N_Object_T_PageProfile t
LEFT JOIN [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
    USING (object_type, object_id)
    WHERE p.object_id IS NULL
 GROUP BY t.object_type;
