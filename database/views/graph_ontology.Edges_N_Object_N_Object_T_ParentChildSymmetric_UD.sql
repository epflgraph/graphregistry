CREATE OR REPLACE VIEW graph_ontology.Edges_N_Object_N_Object_T_ParentChildSymmetric_UD AS

                SELECT 'Child-to-Parent' AS edge_type,
                       'Ont' AS from_institution_id, 'Category' AS from_object_type, c2p.from_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Category' AS   to_object_type,   c2p.to_id AS   to_object_id,
                       'n/a' AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent c2p
                    ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                     = ('Ont', 'Category', c2p.from_id, 'Ont', 'Category', c2p.to_id)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
             
             UNION ALL
             
                SELECT 'Parent-to-Child' AS edge_type,
                       'Ont' AS from_institution_id, 'Category' AS from_object_type,   to_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Category' AS   to_object_type, from_id AS   to_object_id,
                       'n/a' AS context, 1 AS to_process
                  FROM graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
            INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent c2p
                    ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                     = ('Ont', 'Category', c2p.to_id, 'Ont', 'Category', c2p.from_id)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
                  
             UNION ALL
                  
                SELECT 'Parent-to-Child' AS edge_type,
                       'Ont' AS from_institution_id, 'Category' AS from_object_type, c.from_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Concept'  AS   to_object_type,   l.to_id AS   to_object_id,
                       'n/a' AS context, 1 AS to_process
                  FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild c
            INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild l
                    ON c.to_id = l.from_id
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
                    ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                     = ('Ont', 'Category', c.from_id, 'Ont', 'Concept', l.to_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1
                    
             UNION ALL

                SELECT 'Child-to-Parent' AS edge_type,
                       'Ont' AS from_institution_id, 'Concept'  AS from_object_type,   l.to_id AS from_object_id,
                       'Ont' AS   to_institution_id, 'Category' AS   to_object_type, c.from_id AS   to_object_id,
                       'n/a' AS context, 1 AS to_process
                  FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild c
            INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild l
                    ON c.to_id = l.from_id
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_FieldsChanged tp
                    ON (tp.from_institution_id, tp.from_object_type, tp.from_object_id, tp.to_institution_id, tp.to_object_type, tp.to_object_id)
                     = ('Ont', 'Category', l.to_id, 'Ont', 'Concept', c.from_id)
            INNER JOIN graph_registry.Operations_N_Object_N_Object_T_TypeFlags tf
                 USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
                 WHERE tp.to_process = 1
                   AND tf.flag_type = 'fields'
                   AND tf.to_process = 1;