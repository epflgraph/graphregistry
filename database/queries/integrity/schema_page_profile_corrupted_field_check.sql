SELECT object_id, [[check_fields_list]]
  FROM [[schema_name]].Data_N_Object_T_PageProfile
 WHERE object_type = '[[object_type]]'
   AND [[check_fields_conditions]];

-- Example:
-- SELECT object_id, name_en_value, description_short_en_value, description_medium_en_value, description_long_en_value, external_url_en
--   FROM graph_lectures.Data_N_Object_T_PageProfile
--  WHERE object_type = 'Lecture'
--    AND (              name_en_value IS NOT NULL AND (              name_en_is_auto_generated=1 OR               name_en_is_auto_corrected=1))
--    AND ( description_short_en_value IS NOT NULL AND ( description_short_en_is_auto_generated=1 OR  description_short_en_is_auto_corrected=1))
--    AND (description_medium_en_value IS NOT NULL AND (description_medium_en_is_auto_generated=1 OR description_medium_en_is_auto_corrected=1))
--    AND (  description_long_en_value IS NOT NULL AND (  description_long_en_is_auto_generated=1 OR   description_long_en_is_auto_corrected=1))
--    AND external_url_en IS NOT NULL;
