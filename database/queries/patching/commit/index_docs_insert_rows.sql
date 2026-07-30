         -- Execute INSERT new rows query
INSERT INTO [[graphsearch_prod_mirror]].Index_D_[[doc_type]]
            ([[data_columns]])
     SELECT  [[data_columns]]
       FROM        [[graphsearch_test]].Index_D_[[doc_type]] t
  LEFT JOIN [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
      USING (doc_id)
      WHERE p.doc_id IS NULL;
