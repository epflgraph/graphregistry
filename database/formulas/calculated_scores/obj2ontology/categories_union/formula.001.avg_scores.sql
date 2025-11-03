REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores_AVG
      SELECT institution_id, object_type, COALESCE(AVG(score), 1) AS avg_score
	    FROM [[airflow]].Operations_N_Object_T_TypeFlags tf
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores cs
       USING (institution_id, object_type)
       WHERE tf.flag_type = 'scores'
         AND tf.to_process = 1
         AND cs.calculation_type = 'concept sum-scores aggregation'
    GROUP BY object_type;
