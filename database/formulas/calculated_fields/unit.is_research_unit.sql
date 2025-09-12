SELECT institution_id, object_type, object_id,
       'n/a' AS field_language, 'is_research_unit' AS field_name,
       subtype_en IS NOT NULL AND subtype_en IN ('Laboratory', 'Group', 'Chair', 'Center', 'Institute') AS field_value
  FROM [[registry]].Data_N_Object_T_PageProfile
 WHERE object_type = 'Unit'