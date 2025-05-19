CREATE OR REPLACE VIEW graph_lectures.Index_D_Course_L_Lecture_T_ORG AS
                SELECT row_number() OVER (ORDER BY t1.from_object_id, t1.to_object_id ASC) AS row_id,
                       t1.from_institution_id AS  doc_institution, t1.from_object_type AS  doc_type,                                    t1.from_object_id AS  doc_id,
                         t1.to_institution_id AS link_institution,   t1.to_object_type AS link_type, 'Parent-to-Child' AS link_subtype,   t1.to_object_id AS link_id,
                       MAX(t1.academic_year) AS academic_year,
                       MAX(t2.video_upload_date)   AS video_upload_date, 
                       MAX(t2.video_modified_date) AS video_modified_date,
                       MIN(t1.sort_number) AS sort_number,
                       MAX(t2.video_stream_url) AS video_stream_url,
                       MAX(t2.video_duration) AS video_duration,
                       MAX(COALESCE(t3.avg_norm_log_degree, 0.001)) AS degree_score,
                       MAX(1/(1+t1.sort_number)) AS row_score,
                       MIN(t1.sort_number) AS row_rank
                  FROM graph_lectures.Edges_N_Course_N_Lecture_T_ParentToChild t1
             LEFT JOIN graph_lectures.Nodes_N_Lecture t2
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id) = (t2.institution_id, t2.object_type, t2.object_id)
             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores t3
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id) = (t3.institution_id, t3.object_type, t3.object_id)
              GROUP BY t1.from_institution_id, t1.from_object_type, t1.from_object_id, t1.to_institution_id, t1.to_object_type, t1.to_object_id;
              