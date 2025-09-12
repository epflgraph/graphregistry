CREATE OR REPLACE VIEW graph_ontology.Index_D_Category_L_Concept_T_ORG AS
                SELECT row_number() OVER (ORDER BY doc_id, link_id ASC) AS row_id,
                       doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, degree_score,
                       1/2 + 1/(1+row_number() OVER (PARTITION BY doc_id ORDER BY degree_score DESC)) AS row_score,
                                  row_number() OVER (PARTITION BY doc_id ORDER BY degree_score DESC)  AS row_rank
                  FROM (SELECT 'Ont'              AS doc_institution,
                               'Category'         AS doc_type,
                               t.from_id          AS doc_id,
                               'Ont'              AS link_institution,
                               'Concept'          AS link_type,
                               'Parent-to-Child'  AS link_subtype,
                               t.to_id            AS link_id,
                               IF(tND.avg_norm_log_degree IS NULL, 0.001, tND.avg_norm_log_degree) AS degree_score
                          FROM graph_ontology.Edges_N_Category_N_Concept_T_ParentToChild t
                     LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores tND
                            ON (tND.institution_id, tND.object_type, tND.object_id) = ('Ont', 'Concept', t.to_id)) t;