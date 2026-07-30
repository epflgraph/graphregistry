       -- Fetch ids of rows to DELETE on doclinks index table
   SELECT p.doc_id, p.link_id
     FROM [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] p
LEFT JOIN        [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] t
    USING (doc_id, link_id)
    WHERE t.doc_id  IS NULL
       OR t.link_id IS NULL;
