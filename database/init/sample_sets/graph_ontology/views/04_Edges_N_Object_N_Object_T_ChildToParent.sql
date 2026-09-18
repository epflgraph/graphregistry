CREATE OR REPLACE VIEW [[ontology]].Edges_N_Object_N_Object_T_ChildToParent AS
                SELECT 'Concept' AS from_object_type, t1.to_id AS from_object_id, 'Category' AS to_object_type, t2.from_id AS to_object_id, 'ontology tree' AS context, t1.record_deleted, t1.row_id
                  FROM [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t1
            INNER JOIN [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild t2
                    ON t1.from_id = t2.to_id
             UNION ALL
                SELECT 'Category' AS from_object_type, from_id AS from_object_id, 'Category' AS to_object_type, to_id AS to_object_id, 'ontology tree' AS context, record_deleted, row_id
                  FROM [[ontology]].Edges_N_Category_N_Category_T_ChildToParent;
