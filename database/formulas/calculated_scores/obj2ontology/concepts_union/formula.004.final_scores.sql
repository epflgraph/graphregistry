
-- ================= Average out all scores and create final table [0 min]
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
                    (institution_id, object_type, object_id, concept_id, score)
              SELECT s.institution_id, s.object_type, s.object_id, s.concept_id,
                     (2/(1 + exp(-2 * (COALESCE(s.score_1,0)+COALESCE(s.score_2,0)+COALESCE(s.score_3,0)) )) - 1) AS score
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_Tuples t
          INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix s
               USING (institution_id, object_type, object_id, concept_id);
