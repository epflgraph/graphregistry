-- =================== Cleanup: find objects to be deleted
  DROP TABLE IF EXISTS [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete;
CREATE TEMPORARY TABLE [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete
                       (PRIMARY KEY (object_type, object_id), KEY object_type (object_type), KEY object_id (object_id)) AS
       SELECT DISTINCT object_type, object_id
                  FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
                 WHERE to_process = 1
                   AND deleted = 0;

-- ========= Cleanup: Soft-delete objects marked for delete
      UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores
         SET deleted = 1
       WHERE deleted = 0
         AND (object_type, object_id)
          IN (SELECT object_type, object_id FROM [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete);

-- ========= Formula: 'concept sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores
             (object_type, object_id, category_id, score, to_process, deleted)

      SELECT cs.object_type, cs.object_id, cs.category_id,
             (2/(1 + EXP(-cs.score/(4*av.avg_score))) - 1) AS score,
             1 AS to_process,
             0 AS deleted

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.object_type = se.object_type
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.to_process  = 1
         AND se.deleted     = 0

          -- Join scores table
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores cs
          ON se.object_type = cs.object_type
         AND se.object_id = cs.object_id
         AND cs.calculation_type = 'concept sum-scores aggregation'
         AND cs.deleted = 0

          -- Join score averages
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores_AVG av
          ON cs.object_type = av.object_type;
