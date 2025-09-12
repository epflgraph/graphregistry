CREATE OR REPLACE VIEW graph_lectures.Index_D_Lecture_L_Concept_T_ORG_Search AS
                SELECT row_number() OVER (ORDER BY e.object_id, e.concept_id ASC) AS row_id,
                       'EPFL' AS doc_institution, 'Lecture' AS doc_type, e.object_id AS doc_id,
                       'Ont' AS link_institution, 'Concept' AS link_type, 'Parent-to-Child' AS link_subtype, e.concept_id AS link_id,
                       p.name_en_value AS name_en, p.name_fr_value AS name_fr, 
                           GROUP_CONCAT(DISTINCT e.detection_timestamp ORDER BY e.detection_timestamp ASC SEPARATOR ',')  AS timestamps,
                       MD5(GROUP_CONCAT(DISTINCT e.detection_timestamp ORDER BY e.detection_timestamp ASC SEPARATOR ',')) AS timestamps_md5,
                       COUNT(e.detection_timestamp) AS n_timestamps,
                       SUM(e.detection_score) AS detection_sum_score,
                       AVG(e.detection_score) AS detection_avg_score,
                       MAX(e.detection_score) AS detection_max_score,
                       COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score,
                       SUM(e.detection_score) AS rank_score,
                       ROUND(100/(1+SUM(e.detection_score))) AS row_rank
                  FROM graph_cache.Edges_N_Lecture_N_Concept_T_Timestamps e
            INNER JOIN graph_ontology.Data_N_Object_T_PageProfile p ON ('Concept', e.concept_id) = (p.object_type, p.object_id)
             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores  d ON ('Concept', e.concept_id) = (d.object_type, d.object_id)
                 WHERE (e.institution_id, e.object_type) = ('EPFL', 'Lecture')
              GROUP BY e.object_id, e.concept_id;
