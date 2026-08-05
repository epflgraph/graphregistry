-- Execute REPLACE rows query (mutually existing rows with different values)
--
-- Placeholder construction (to be done by the Python adapter):
--   [[update_set_clause]]  -> comma-separated assignments: p.<col> = t.<col>
--                             for every payload column, excluding:
--                             - key columns (doc_type, doc_id)
--                             - surrogate/meta columns (row_id, to_process, deleted, last_date_cached, ...)
--   [[changed_condition]]  -> OR-chain of null-safe comparisons:
--                             NOT (p.<col> <=> t.<col>)
--                             using the same column list as [[update_set_clause]].
--
-- The adapter should introspect the target table, subtract the key and excluded
-- columns, and generate both clauses from the remaining columns.
     UPDATE [[graphsearch_prod_mirror]].Index_D_[[doc_type]] p
 INNER JOIN        [[graphsearch_test]].Index_D_[[doc_type]] t
      USING (doc_type, doc_id)
        SET [[update_set_clause]]
      WHERE [[changed_condition]]
        AND t.deleted = 0;

