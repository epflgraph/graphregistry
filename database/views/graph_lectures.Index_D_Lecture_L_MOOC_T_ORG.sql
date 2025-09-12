-- TODO: session_id doen't inidicate the year of the session

CREATE OR REPLACE VIEW graph_lectures.Index_D_Lecture_L_MOOC_T_ORG AS                  
                SELECT row_number() OVER (ORDER BY t1.to_object_id, t1.from_object_id ASC) AS row_id,
                         t1.to_institution_id AS  doc_institution,   t1.to_object_type AS  doc_type,                                      t1.to_object_id AS  doc_id,
                       t1.from_institution_id AS link_institution, t1.from_object_type AS link_type, 'Child-to-Parent' AS link_subtype, t1.from_object_id AS link_id,
                       t3.field_value AS domain, t4.field_value AS language, t5.field_value AS level, t6.field_value AS platform, t7.field_value AS thumbnail_image_url,
                       COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score, 1 AS row_score, 1 AS row_rank
                  FROM graph_lectures.Edges_N_MOOC_N_Lecture_T_ParentToChild t1
             LEFT JOIN graph_registry.Data_N_Object_T_CustomFields t3
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'domain')              = (t3.institution_id, t3.object_type, t3.object_id, t3.field_language, t3.field_name)
             LEFT JOIN graph_registry.Data_N_Object_T_CustomFields t4
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'language')            = (t4.institution_id, t4.object_type, t4.object_id, t4.field_language, t4.field_name)
             LEFT JOIN graph_registry.Data_N_Object_T_CustomFields t5
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'level')               = (t5.institution_id, t5.object_type, t5.object_id, t5.field_language, t5.field_name)
             LEFT JOIN graph_registry.Data_N_Object_T_CustomFields t6
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'platform')            = (t6.institution_id, t6.object_type, t6.object_id, t6.field_language, t6.field_name)
             LEFT JOIN graph_registry.Data_N_Object_T_CustomFields t7
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'thumbnail_image_url') = (t7.institution_id, t7.object_type, t7.object_id, t7.field_language, t7.field_name)
             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores d
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id) = (d.institution_id, d.object_type, d.object_id)
              GROUP BY t1.from_institution_id, t1.from_object_type, t1.from_object_id, t1.to_institution_id, t1.to_object_type, t1.to_object_id;