        -- Get list of slide tokens for a lecture based on the video token
    SELECT s.image_token
      FROM [[airflow]].Operations_N_Lecture_T_ProcessingTokens l
INNER JOIN [[airflow]].Operations_N_Slide_T_ProcessingTokens s
     USING (video_token)
     WHERE l.object_type = 'Lecture'
       AND l.object_id = '[[lecture_id]]'