       -- Execute DELETE rows query
    DELETE p
      FROM [[graphsearch_prod_mirror]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] p
 LEFT JOIN        [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[sem_or_org]][[special_suffix]] t
       ON p.doc_type     = t.doc_type
      AND p.doc_id       = t.doc_id
      AND p.link_type    = t.link_type
      AND p.link_subtype = t.link_subtype
      AND p.link_id      = t.link_id
      AND t.deleted      = 0
     WHERE t.doc_id  IS NULL;
