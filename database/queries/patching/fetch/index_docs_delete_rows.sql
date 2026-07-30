       -- Fetch data rows to DELETE (for rollback)
   SELECT [[data_columns]]
     FROM [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
LEFT JOIN        [[graphsearch_test]].Index_D_[[doc_type]] t
    USING (doc_id)
    WHERE t.doc_id IS NULL;
