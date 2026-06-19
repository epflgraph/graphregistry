    -- Get video token for a specific lecture
SELECT video_token
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE object_type = 'Lecture'
   AND object_id = '[[lecture_id]]'