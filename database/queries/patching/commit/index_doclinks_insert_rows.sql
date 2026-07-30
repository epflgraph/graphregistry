         -- Execute INSERT new rows query
INSERT INTO [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]]
            ([[data_columns]])
     SELECT  [[data_columns]]
       FROM        [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] t
  LEFT JOIN [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] p
      USING (doc_id, link_id)
      WHERE p.doc_id  IS NULL
         OR p.link_id IS NULL;
