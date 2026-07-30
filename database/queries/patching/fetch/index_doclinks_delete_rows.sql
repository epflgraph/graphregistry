       -- Fetch data rows to DELETE (for rollback)
   SELECT [[data_columns]]
     FROM [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] p
LEFT JOIN        [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] t
    USING (doc_type, doc_id, link_type, link_subtype, link_id)
    WHERE t.doc_id  IS NULL;
