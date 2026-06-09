    -- Get the file URL for a specific lecture based on its lecture key
SELECT field_value
  FROM [[lectures]].Data_N_Object_T_CustomFields
 WHERE object_type = 'Lecture'
   AND object_id = '[[lecture_id]]'
   AND field_language = 'n/a'
   AND field_name = 'video_stream_url';
