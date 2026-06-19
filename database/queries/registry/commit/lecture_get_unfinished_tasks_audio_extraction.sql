    -- Get list of lectures that have audio extraction tasks launched but not yet completed
SELECT object_id
  FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens
 WHERE audio_extraction_task_id IS NOT NULL
   AND audio_token IS NULL
 LIMIT [[limit]];
