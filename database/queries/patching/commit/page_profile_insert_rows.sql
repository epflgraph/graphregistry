         -- Execute INSERT new rows query
INSERT INTO [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile
            ([[data_columns]])
      SELECT  [[data_columns]]
        FROM        [[graphsearch_test]].Data_N_Object_T_PageProfile t
   LEFT JOIN [[graphsearch_prod_mirror]].Data_N_Object_T_PageProfile p
       USING (object_type, object_id)
       WHERE p.object_id IS NULL
         AND t.deleted = 0;

