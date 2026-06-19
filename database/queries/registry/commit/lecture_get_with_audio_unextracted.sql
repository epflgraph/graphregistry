    -- Get list of lectures for which audio has not yet been extracted
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE video_token IS NOT NULL
   AND audio_extraction_task_id IS NULL
   AND audio_token IS NULL
 LIMIT [[limit]];
