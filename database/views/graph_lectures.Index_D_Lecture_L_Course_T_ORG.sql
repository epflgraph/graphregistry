CREATE OR REPLACE VIEW graph_lectures.Index_D_Lecture_L_Course_T_ORG AS
                SELECT row_number() OVER (ORDER BY t1.to_object_id, t1.from_object_id ASC) AS row_id,
                         t1.to_institution_id AS  doc_institution,   t1.to_object_type AS  doc_type,                                      t1.to_object_id AS  doc_id,
                       t1.from_institution_id AS link_institution, t1.from_object_type AS link_type, 'Child-to-Parent' AS link_subtype, t1.from_object_id AS link_id,
                       MAX(t1.academic_year) AS academic_year,
                       MAX(t2.field_value) AS latest_academic_year, 
                       MAX(COALESCE(t4.avg_norm_log_degree, 0.001)) AS degree_score,
                       1 AS row_score, 1 AS row_rank
                  FROM graph_lectures.Edges_N_Course_N_Lecture_T_ParentToChild t1
             LEFT JOIN graph_registry.Data_N_Object_T_CalculatedFields t2
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id, 'en', 'latest_academic_year') = (t2.institution_id, t2.object_type, t2.object_id, t2.field_language, t2.field_name)
             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores t4
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id) = (t4.institution_id, t4.object_type, t4.object_id)
              GROUP BY t1.from_institution_id, t1.from_object_type, t1.from_object_id, t1.to_institution_id, t1.to_object_type, t1.to_object_id;