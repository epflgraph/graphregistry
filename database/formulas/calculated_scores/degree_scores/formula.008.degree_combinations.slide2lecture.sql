SELECT 1;
		-- 	      -- Lecture-to-concept edges
        -- REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
        --       SELECT t.institution_id   AS from_institution_id,
        --              t.object_type      AS from_object_type,
        --              t.object_id        AS from_object_id,    
		-- 	         'Ont'              AS to_institution_id,
        --              'Concept'          AS to_object_type,
        --                      COUNT(DISTINCT t.concept_id)  AS degree,
        --              LOG(1 + COUNT(DISTINCT t.concept_id)) AS log_degree
        --         FROM [[lectures]].Edges_N_Lecture_N_Concept_T_SlideSumScores_UD t
        --     GROUP BY t.institution_id, t.object_type, t.object_id;

