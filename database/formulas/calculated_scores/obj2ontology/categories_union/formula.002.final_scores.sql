-- ========= Formula: 'concept sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)

      SELECT cs.institution_id, cs.object_type, cs.object_id, cs.category_id,
             (2/(1 + EXP(-cs.score/(4*av.avg_score))) - 1) AS score

          -- Check type flags
        FROM [[airflow]].Operations_N_Object_T_TypeFlags tf

          -- Check object flags
  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
          ON tf.institution_id = se.institution_id
         AND tf.object_type = se.object_type
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND se.to_process  = 1

          -- Join scores table
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores cs
          ON se.institution_id = cs.institution_id
         AND se.object_type = cs.object_type
         AND se.object_id = cs.object_id
         AND cs.calculation_type = 'concept sum-scores aggregation'

          -- Join score averages
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores_AVG av
          ON cs.institution_id = av.institution_id
         AND cs.object_type = av.object_type;
