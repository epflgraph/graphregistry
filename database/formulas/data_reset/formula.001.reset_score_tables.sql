

-- ======= Apply soft-delete to affected objects (on all scores tables)
-- ======= before re-calculating the scores for those objects

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_CalculatedScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_FinalScores e
INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired d
     USING (object_type, object_id)
     WHERE d.to_process = 1
       SET e.deleted = 1;
