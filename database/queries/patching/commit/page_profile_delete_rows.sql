       -- Execute DELETE rows query
    DELETE p
      FROM [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
 LEFT JOIN        [[graphsearch_test]].Data_N_Object_T_PageProfile t
       ON p.object_type = t.object_type
      AND p.object_id   = t.object_id
      AND t.deleted     = 0
     WHERE t.object_id IS NULL;

