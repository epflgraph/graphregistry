    SELECT cp.to_object_id AS lecture_id, cp.from_object_id AS slide_id, cf.field_value AS ocr_text_en, LENGTH(cf.field_value) AS text_length
      FROM [[lectures]].Data_N_Object_T_CustomFields cf
INNER JOIN [[lectures]].Edges_N_Object_N_Object_T_ChildToParent cp
        ON (cf.object_type, cf.object_id) = (cp.from_object_type, cp.from_object_id)
     WHERE (cp.from_object_type, cp.to_object_type, cp.context) = ('Slide', 'Lecture', 'part of')
       AND cf.field_name = 'text_original'
       AND cf.record_deleted = 0
       AND cp.record_deleted = 0
       AND cp.to_object_id = '[[lecture_id]]';
