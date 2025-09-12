CREATE OR REPLACE VIEW graph_ontology.Index_D_Concept_L_Concept_T_SEM AS
                SELECT row_number() OVER (ORDER BY doc_id, link_id ASC) AS row_id,
                       doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, semantic_score,
                       1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY semantic_score DESC)) AS row_score,
                                  row_number() OVER (PARTITION BY doc_id ORDER BY semantic_score DESC)  AS row_rank
                  FROM (SELECT 'Ont'       AS doc_institution,
                               'Concept'   AS doc_type,
                               from_id     AS doc_id,
                               'Ont'       AS link_institution,
                               'Concept'   AS link_type,
                               'Semantic'  AS link_subtype,
                               to_id       AS link_id,
                               score       AS semantic_score
                          FROM graph_ontology.Edges_N_Concept_N_Concept_T_Symmetric) t;