       -- Count number of rows to DELETE on docs index table
    SELECT COUNT(*) AS n
      FROM [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
 LEFT JOIN        [[graphsearch_test]].Index_D_[[doc_type]] t
       ON p.doc_type = t.doc_type
      AND p.doc_id   = t.doc_id
      AND t.deleted  = 0
     WHERE t.doc_id IS NULL;

