    -- Get list of lectures that have slide detection tasks launched but not yet completed
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE slide_detection_task_id IS NOT NULL
   AND slides_detected = 0
 LIMIT [[limit]];
