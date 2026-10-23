-- Get lecture enrichment task data: the OCR content of each slide in the lecture.
-- No concept detection join: keyframes carry only their OCR text; concepts
-- are detected by the LLM from the OCR alone.
SELECT DISTINCT t.from_object_id AS slide_id, c.field_value AS ocr_content
           FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t
     INNER JOIN graph_lectures.Data_N_Object_T_CustomFields c
             ON (c.object_type, c.object_id, c.field_language, c.field_name) = ('Slide', t.from_object_id, 'en', 'text')
            AND c.record_deleted = 0
          WHERE (t.from_object_type, t.to_object_type) = ('Slide', 'Lecture')
            AND t.to_object_id = '[[lecture_id]]'
            AND t.record_deleted = 0;
