    -- Get lectures that have not been downloaded yet
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE video_download_task_id IS NULL
   AND video_token IS NULL
 LIMIT [[limit]];
