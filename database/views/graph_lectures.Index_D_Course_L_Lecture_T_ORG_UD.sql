CREATE OR REPLACE VIEW graph_lectures.Index_D_Course_L_Lecture_T_ORG_UD AS

                SELECT row_number() OVER (ORDER BY doc_id, link_id ASC) AS row_id, doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id, academic_year, video_upload_date, video_modified_date, sort_number, video_stream_url, video_duration, degree_score, row_score, row_rank
                  
                  FROM (

                SELECT 
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
           FORCE INDEX (from_object_key, to_object_key)

             LEFT JOIN graph_lectures.Nodes_N_Lecture t2
           FORCE INDEX (object_key)
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id)
                     = (   t2.institution_id,    t2.object_type,    t2.object_id)

             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores t3
           FORCE INDEX (object_key)
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id)
                     = (   t3.institution_id,    t3.object_type,    t3.object_id)
                    
            INNER JOIN graph_registry.Operations_N_Object_T_ToProcess t4
           FORCE INDEX (object_key)
                    ON (t1.from_institution_id, t1.from_object_type, t1.from_object_id)
                     = (     t4.institution_id,      t4.object_type,      t4.object_id)

                 WHERE t4.object_type = 'Course'

              GROUP BY t1.from_institution_id,
                       t1.from_object_type,
                       t1.from_object_id,
                       t1.to_institution_id,
                       t1.to_object_type,
                       t1.to_object_id
             
             UNION ALL
              
                SELECT 
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
           FORCE INDEX (from_object_key, to_object_key)

             LEFT JOIN graph_lectures.Nodes_N_Lecture t2
           FORCE INDEX (object_key)
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id)
                     = (   t2.institution_id,    t2.object_type,    t2.object_id)

             LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores t3
           FORCE INDEX (object_key)
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id)
                     = (   t3.institution_id,    t3.object_type,    t3.object_id)
                        
            INNER JOIN graph_lectures.Operations_N_Object_T_ToProcess t5
           FORCE INDEX (object_key)
                    ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id)
                     = (   t5.institution_id,    t5.object_type,    t5.object_id)

                   AND t5.object_type = 'Lecture'

              GROUP BY t1.from_institution_id,
                       t1.from_object_type,
                       t1.from_object_id,
                       t1.to_institution_id,
                       t1.to_object_type,
                       t1.to_object_id

                     ) t;