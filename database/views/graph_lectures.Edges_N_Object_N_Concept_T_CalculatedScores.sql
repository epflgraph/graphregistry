CREATE OR REPLACE VIEW graph_lectures.Edges_N_Object_N_Concept_T_CalculatedScores AS

                SELECT institution_id, object_type, object_id, concept_id, 'slide sum-scores aggregation' AS calculation_type, sum_score AS score
                  FROM graph_cache.Edges_N_Course_N_Concept_T_SlideSumScores

             UNION ALL

                SELECT institution_id, object_type, object_id, concept_id, 'slide sum-scores aggregation' AS calculation_type, sum_score AS score
                  FROM graph_cache.Edges_N_MOOC_N_Concept_T_SlideSumScores
                  
             UNION ALL

                SELECT institution_id, object_type, object_id, concept_id, 'slide sum-scores aggregation' AS calculation_type, sum_score AS score
                  FROM graph_cache.Edges_N_Lecture_N_Concept_T_SlideSumScores;