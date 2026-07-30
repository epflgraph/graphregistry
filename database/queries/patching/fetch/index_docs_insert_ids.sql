       -- Fetch ids of rows to INSERT
   SELECT t.doc_type, t.doc_id
     FROM        [[graphsearch_test]].Index_D_[[doc_type]] t
LEFT JOIN [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
    USING (doc_type, doc_id)
    WHERE p.doc_id IS NULL;
