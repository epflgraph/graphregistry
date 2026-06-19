    -- Get slide detection task ID for a specific lecture
SELECT slide_detection_task_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE object_type = 'Lecture'
   AND object_id = '[[lecture_id]]'