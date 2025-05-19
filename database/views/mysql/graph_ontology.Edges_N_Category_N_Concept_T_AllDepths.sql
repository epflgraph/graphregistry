CREATE OR REPLACE VIEW graph_ontology.Edges_N_Category_N_Concept_T_AllDepths AS
                
                SELECT 'Ont' AS institution_id, 'Category' AS object_type, e.from_id AS object_id, e.to_id AS concept_id, n.depth
                  FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild e
            INNER JOIN graph_ontology.Nodes_N_Category n ON e.from_id = n.id
    
             UNION ALL

                SELECT 'Ont' AS institution_id, 'Category' AS object_type, e2.to_id AS object_id, e1.to_id AS concept_id, n.depth
                  FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild e1
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e2 ON e1.from_id = e2.from_id
            INNER JOIN graph_ontology.Nodes_N_Category n ON e2.to_id = n.id

             UNION ALL
     
                SELECT 'Ont' AS institution_id, 'Category' AS object_type, e3.to_id AS object_id, e1.to_id AS concept_id, n.depth
                  FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild e1
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e2 ON e1.from_id = e2.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e3 ON e2.to_id = e3.from_id
            INNER JOIN graph_ontology.Nodes_N_Category n ON e3.to_id = n.id

             UNION ALL
     
                SELECT 'Ont' AS institution_id, 'Category' AS object_type, e4.to_id AS object_id, e1.to_id AS concept_id, n.depth
                  FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild e1
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e2 ON e1.from_id = e2.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e3 ON e2.to_id = e3.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e4 ON e3.to_id = e4.from_id
            INNER JOIN graph_ontology.Nodes_N_Category n ON e4.to_id = n.id

             UNION ALL

                SELECT 'Ont' AS institution_id, 'Category' AS object_type, e5.to_id AS object_id, e1.to_id AS concept_id, n.depth
                  FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild e1
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e2 ON e1.from_id = e2.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e3 ON e2.to_id = e3.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e4 ON e3.to_id = e4.from_id
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent e5 ON e4.to_id = e5.from_id
            INNER JOIN graph_ontology.Nodes_N_Category n ON e5.to_id = n.id;
