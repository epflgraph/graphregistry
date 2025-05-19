-- =================== Graph traversal: Unit-Publication-Concept score edges
CREATE OR REPLACE VIEW graph_registry.GraphTraversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection AS
       SELECT DISTINCT p2u.to_institution_id   AS institution_id,
                       p2u.to_object_id        AS unit_id,
                       a2p.from_object_id      AS publication_id,
                       a2c.concept_id          AS concept_id,
                       a2c.score               AS score
                  FROM graph_registry.Edges_N_Object_N_Object_T_ChildToParent p2u -- Start with: Person-Unit affiliation edges
            INNER JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent a2p -- Link to: Publication-Person authorship edges
                    ON (p2u.from_institution_id, p2u.from_object_id) = (a2p.to_institution_id, a2p.to_object_id) -- [link Person-to-Person]
            INNER JOIN graph_registry.Edges_N_Object_N_Concept_T_ConceptDetection a2c -- Link to: Publication-Concept detection scores
                    ON (a2p.from_institution_id, a2p.from_object_id) = (a2c.institution_id, a2c.object_id) -- [link Publication-to-Publication]
                 WHERE (p2u.from_object_type, p2u.to_object_type) = ('Person', 'Unit') AND p2u.context LIKE 'accreditation%'
                   AND (a2p.from_object_type, a2p.to_object_type, a2p.context) = ('Publication', 'Person', 'authorship')
                   AND (a2c.object_type, a2c.text_source) = ('Publication', 'abstract');