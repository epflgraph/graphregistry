CREATE OR REPLACE VIEW graph_lectures.Edges_N_Lecture_N_Concept_T_Timestamps AS
       SELECT DISTINCT t1.from_institution_id AS institution_id, t1.from_object_type AS object_type, t1.from_object_id AS object_id, t2.concept_id, MAX(score) AS detection_score, t1.time_hms AS detection_time_hms, t1.`timestamp` AS detection_timestamp
                  FROM graph_lectures.Edges_N_Lecture_N_Slide_T_ParentToChild t1
            INNER JOIN graph_lectures.Edges_N_Slide_N_Concept                 t2 ON (t1.to_institution_id, t1.to_object_type, t1.to_object_id) = (t2.institution_id, t2.object_type, t2.object_id)
              GROUP BY t1.from_institution_id, t1.from_object_type, t1.from_object_id, t2.concept_id, t1.time_hms;