    -- Get list of lectures that have video download tasks launched but not yet completed
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE video_download_task_id IS NOT NULL
   AND video_token IS NULL
 LIMIT [[limit]];
