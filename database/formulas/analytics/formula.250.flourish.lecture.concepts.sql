   REPLACE INTO graph_analytics.Flourish_Lecture_Concepts
                (course_code, course_name, lecture_id, lecture_name, lecture_url, concept_id, concept_name, concept_times)
SELECT DISTINCT l.course_id AS course_code, p2.name_en_value AS course_name,
                l.lecture_id,
                COALESCE(IF(p1a.name_en_value   IS NOT NULL, p1a.name_en_value,   p1b.name_en_value  ), 'no title') AS lecture_name,
                COALESCE(IF(p1a.external_url_en IS NOT NULL, p1a.external_url_en, p1b.external_url_en), 'no url'  ) AS lecture_url,
                l.concept_id, COALESCE(p3.name, 'untitled') AS concept_name,
                GROUP_CONCAT(DISTINCT LEFT(cf.field_value, 8) ORDER BY cf.field_value ASC SEPARATOR ', ') AS concept_times
           FROM graph_cache.Traversal_N_Course_N_Lecture_N_Slide_N_Concept_T_LLMValidated l
     INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields cf
             ON l.slide_id = cf.from_object_id
            AND (cf.from_object_type, cf.to_object_type, cf.context) = ('Slide', 'Lecture', 'part of')
            AND cf.field_language = 'n/a' AND cf.field_name = 'start_time_hms'
      LEFT JOIN _1_DEV_graph_lectures.Data_N_Object_T_PageProfile p1a
             ON l.lecture_id = p1a.object_id
      LEFT JOIN graph_lectures.Data_N_Object_T_PageProfile p1b
             ON l.lecture_id = p1b.object_id
      LEFT JOIN graph_registry.Data_N_Object_T_PageProfile p2
             ON l.course_id = p2.object_id
      LEFT JOIN graph_ontology.Nodes_N_Concept p3
             ON l.concept_id = p3.object_id
       GROUP BY l.course_id, l.lecture_id, l.concept_id;
     
