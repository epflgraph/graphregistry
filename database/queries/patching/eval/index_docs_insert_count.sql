       -- Count number of rows to INSERT on docs index table
   SELECT COUNT(*) AS n
     FROM        [[graphsearch_test]].Index_D_[[doc_type]] t
LEFT JOIN [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
    USING (doc_id)
    WHERE p.doc_id IS NULL;
