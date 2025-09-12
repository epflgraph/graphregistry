CREATE OR REPLACE VIEW graph_lectures.Edges_N_Lecture_N_Concept_T_Timestamps_UD AS

                SELECT t2.from_institution_id    AS institution_id, 
                       t2.from_object_type       AS object_type, 
                       t2.from_object_id         AS object_id, 
                       t3.concept_id             AS concept_id,
                       MAX(t3.score)             AS detection_score, 
                       t4.field_value            AS detection_time_hms, 
                       t5.field_value            AS detection_timestamp
                       
                  FROM graph_airflow.Operations_N_Object_T_FieldsChanged t1

            INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
                    ON (   t1.institution_id,    t1.object_type,    t1.object_id)
                     = (t2.to_institution_id, t2.to_object_type, t2.to_object_id)

            INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t3
                    ON (t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (     t3.institution_id,      t3.object_type,      t3.object_id)
                     
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t4
                    ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id, t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (t4.from_institution_id, t4.from_object_type, t4.from_object_id,  t4.to_institution_id,   t4.to_object_type,   t4.to_object_id)
                     
            INNER JOIN graph_lectures.Data_N_Object_N_Object_T_CustomFields t5
                    ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id, t2.from_institution_id, t2.from_object_type, t2.from_object_id)
                     = (t5.from_institution_id, t5.from_object_type, t5.from_object_id, t5.to_institution_id,   t5.to_object_type,   t5.to_object_id)
            
                 WHERE t1.object_type = 'Lecture'
                   AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
                   AND t3.object_type = 'Slide'
                   AND (t4.from_object_type, t4.to_object_type, t4.field_name) = ('Lecture', 'Slide', 'time_hms')
                   AND (t5.from_object_type, t5.to_object_type, t5.field_name) = ('Lecture', 'Slide', 'timestamp')
                   AND t1.to_process = 1

              GROUP BY t2.from_institution_id,
                       t2.from_object_type, 
                       t2.from_object_id, 
                       t3.concept_id, 
                       t5.field_value;


