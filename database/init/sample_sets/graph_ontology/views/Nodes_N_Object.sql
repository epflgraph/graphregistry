CREATE OR REPLACE VIEW [[ontology]].Nodes_N_Object AS
                SELECT object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, record_deleted, row_id
                  FROM [[ontology]].Nodes_N_Concept
             UNION ALL
                SELECT object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, record_deleted, row_id
                  FROM [[ontology]].Nodes_N_Category;
