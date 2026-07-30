       -- Fetch data rows to be deleted (for rollback)
REPLACE INTO [[graphsearch_prod_mirror]].Index_D_[[doc_type]]
             ([[data_columns]])
      SELECT  [[data_columns]]
        FROM        [[graphsearch_test]].Index_D_[[doc_type]] p
       WHERE to_deploy = 1