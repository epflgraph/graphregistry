
-- ORIGINAL QUERY:
-- ==================== Average out all scores and create final table
--             INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
--                        (object_type, object_id, concept_id, score, to_process, deleted)
--                  SELECT object_type, object_id, concept_id, score, to_process, deleted
--                    FROM (SELECT object_type, object_id, concept_id,
--                                 (2/(1 + exp(-2 * (COALESCE(score_1,0)+COALESCE(score_2,0)+COALESCE(score_3,0)) )) - 1) AS score,
--                                 1 AS to_process,
--                                 0 AS deleted
--                            FROM [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix AS d
--                           WHERE d.deleted = 0
--                             AND d.to_process = 1
--                         ) AS new
-- ON DUPLICATE KEY UPDATE score      = new.score,
--                         to_process = new.to_process,
--                         deleted    = new.deleted;
-- ALTERNATIVE QUERY(IES):

-- ========================================================================
-- === Average out all scores and insert into / update the final table ====
-- ========================================================================

-- ======== Insert non-existant rows
INSERT INTO [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
            (object_type, object_id, concept_id, score, to_process, deleted)
     SELECT s.object_type, s.object_id, s.concept_id,
            (2/(1 + exp(-2 * (COALESCE(s.score_1,0)+COALESCE(s.score_2,0)+COALESCE(s.score_3,0)) )) - 1),
            1 AS to_process, 0 AS deleted
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix AS s
  LEFT JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores AS f
         ON f.object_type = s.object_type
        AND f.object_id   = s.object_id
        AND f.concept_id  = s.concept_id
      WHERE s.to_process = 1
        AND s.deleted = 0
        AND f.row_id IS NULL;

-- ======== Update existing ones
     UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores AS f
 INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix AS s
         ON f.object_type = s.object_type
        AND f.object_id   = s.object_id
        AND f.concept_id  = s.concept_id
        SET f.score = (2/(1 + exp(-2 * (COALESCE(s.score_1,0)+COALESCE(s.score_2,0)+COALESCE(s.score_3,0)) )) - 1),
            f.to_process = 1,
            f.deleted    = 0
      WHERE s.to_process = 1
        AND s.deleted    = 0;
