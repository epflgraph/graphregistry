CREATE OR REPLACE VIEW graph_lectures.Index_D_Lecture_L_Concept_T_ORG AS
                SELECT row_number() OVER (ORDER BY t.object_id, t.concept_id ASC) AS row_id,
                       t.institution_id AS doc_institution, t.object_type AS doc_type, t.object_id AS doc_id,
                       'Ont' AS link_institution, 'Concept' AS link_type, 'Parent-to-Child' AS link_subtype, t.concept_id AS link_id,
                       GROUP_CONCAT(t.detection_timestamp ORDER BY detection_timestamp ASC SEPARATOR ',')  AS timestamps,
                       COALESCE(CAST(AVG(d.avg_norm_log_degree) AS FLOAT), 0.001) AS degree_score,
                       (LOG(SUM(t.detection_score)+1)+1) AS row_score,
                       ROUND(100/(LOG(SUM(t.detection_score)+1)+1)) AS row_rank
                  FROM graph_cache.Edges_N_Lecture_N_Concept_T_Timestamps t
             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores d ON (d.institution_id, d.object_type, d.object_id) = ('Ont', 'Concept', t.concept_id)
              GROUP BY t.institution_id, t.object_type, t.object_id, t.concept_id;