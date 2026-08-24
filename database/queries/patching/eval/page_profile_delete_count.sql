       -- Count number of rows to DELETE per object type
    SELECT p.object_type, COUNT(*) AS n
      FROM [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
 LEFT JOIN        [[graphsearch_test]].Data_N_Object_T_PageProfile t
       ON p.object_type = t.object_type
      AND p.object_id   = t.object_id
      AND t.deleted     = 0
     WHERE t.object_id IS NULL
  GROUP BY p.object_type;

