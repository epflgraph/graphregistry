CREATE OR REPLACE VIEW Edges_N_Object_N_Object_T_ChildToParent AS
    SELECT 'Ont' AS from_institution_id, 'Concept'  AS from_object_type,   t1.to_id AS from_object_id,
           'Ont' AS   to_institution_id, 'Category' AS   to_object_type, t2.from_id AS   to_object_id,
           'ontology tree' AS context
      FROM Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t1
INNER JOIN Edges_N_Category_N_ConceptsCluster_T_ParentToChild t2
        ON t1.from_id = t2.to_id
 UNION ALL
    SELECT 'Ont' AS from_institution_id, 'Category' AS from_object_type, from_id AS from_object_id,
           'Ont' AS   to_institution_id, 'Category' AS   to_object_type,   to_id AS   to_object_id,
           'ontology tree' AS context
      FROM Edges_N_Category_N_Category_T_ChildToParent;

CREATE OR REPLACE VIEW Nodes_N_Object AS
   SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text
     FROM Nodes_N_Concept
UNION ALL  
   SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text
     FROM Nodes_N_Category;