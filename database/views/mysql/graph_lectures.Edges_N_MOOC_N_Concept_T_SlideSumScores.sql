-- =================== MOOC to concept edges (by aggregating slide-concept scores)
CREATE OR REPLACE VIEW graph_lectures.Edges_N_MOOC_N_Concept_T_SlideSumScores AS

	   SELECT DISTINCT t3.from_institution_id AS institution_id,
                       t3.from_object_type    AS object_type,
                       t3.from_object_id      AS object_id,
                       t1.concept_id          AS concept_id,
                       SUM(t1.score)          AS sum_score

                  FROM graph_lectures.Edges_N_Slide_N_Concept t1

            INNER JOIN graph_lectures.Edges_N_Lecture_N_Slide_T_ParentToChild t2
                    ON (   t1.institution_id,    t1.object_type,    t1.object_id)
                     = (t2.to_institution_id, t2.to_object_type, t2.to_object_id)
                       
            INNER JOIN graph_lectures.Edges_N_MOOC_N_Lecture_T_ParentToChild t3
                    ON (t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (  t3.to_institution_id,   t3.to_object_type,   t3.to_object_id)
                       
              GROUP BY t3.from_institution_id,
                       t3.from_object_type,
                       t3.from_object_id,
                       t1.concept_id;