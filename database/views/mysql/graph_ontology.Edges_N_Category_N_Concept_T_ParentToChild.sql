CREATE OR REPLACE VIEW graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild AS
                SELECT e1.from_id, e2.to_id
                  FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild e1
            INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild e2 ON e1.to_id = e2.from_id;