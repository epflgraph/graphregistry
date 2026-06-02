             -- Get lecture enrichment task data, including OCR content and detected concepts for each slide in the lecture
SELECT DISTINCT t.from_object_id AS slide_id, c.field_value AS ocr_content,
                GROUP_CONCAT(o.name SEPARATOR '|') AS concepts
           FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t
     INNER JOIN graph_lectures.Data_N_Object_T_CustomFields c
             ON (c.object_type, c.object_id, c.field_language, c.field_name) = ('Slide', t.from_object_id, 'en', 'text')
     INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection d
             ON (d.object_type, d.object_id) = ('Slide', t.from_object_id)
     INNER JOIN graph_ontology.Nodes_N_Concept o
			 ON d.concept_id = o.object_id
          WHERE (t.from_object_type, t.to_object_type) = ('Slide', 'Lecture')
            AND t.to_object_id = '[[lecture_id]]'
	   GROUP BY t.from_object_id, c.field_value;
