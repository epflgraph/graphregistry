-- =================== Course to concept edges (by aggregating slide-concept scores)
CREATE OR REPLACE VIEW graph_lectures.Edges_N_Course_N_Concept_T_SlideSumScores_UD AS

                SELECT t4.institution_id, t4.object_type, t4.object_id, t1.concept_id,
                       SUM(t1.score) AS sum_score

                  FROM graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t1

            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
                    ON (     t1.institution_id,      t1.object_type,      t1.object_id)
                     = (t2.from_institution_id, t2.from_object_type, t2.from_object_id)

            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t3
                    ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id)
                     = (t3.from_institution_id, t3.from_object_type, t3.from_object_id)

            INNER JOIN graph_registry.Operations_N_Object_T_ScoresExpired t4
                    ON (t3.to_institution_id, t3.to_object_type, t3.to_object_id)
                     = (   t4.institution_id,    t4.object_type,    t4.object_id)

                 WHERE t1.object_type = 'Slide'
                   AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
                   AND (t3.from_object_type, t3.to_object_type) = ('Lecture', 'Course')
                   AND t4.object_type = 'Course'
                   AND t4.to_process = 1
                   
                   AND 1 = (SELECT to_process
                              FROM graph_registry.Operations_N_Object_N_Object_T_TypeFlags
                             WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
                                 = ('EPFL', 'Course', 'Ont', 'Concept', 'scores'))
                       
              GROUP BY t4.institution_id, t4.object_type, t4.object_id, t1.concept_id;