       -- Fetch ids of rows to INSERT
   SELECT t.doc_type, t.doc_id, t.link_type, t.link_subtype, t.link_id
     FROM        [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] t
LEFT JOIN [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] p
    USING (doc_type, doc_id, link_type, link_subtype, link_id)
    WHERE p.doc_id  IS NULL;
