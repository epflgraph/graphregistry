    -- Get list of lectures for which slides have not yet been detected
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE video_token IS NOT NULL
   AND slide_detection_task_id IS NULL
   AND slides_detected = 0
 LIMIT [[limit]];
