
   CREATE TABLE graph_analytics.Flourish_CS_119_Concept_to_Course
                (UNIQUE KEY unique_key (course_code, concept_id)) AS
SELECT DISTINCT root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, course_code, course_name, concept_id, concept_name, is_cs_119_concept, score
           FROM graph_analytics.Flourish_CS_119_Full_Data;
           
               
    ALTER TABLE graph_analytics.Flourish_CS_119_Concept_to_Course
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);
  
   CREATE TABLE graph_analytics.Flourish_CS_119_Concept_to_StudyPlan
                (UNIQUE KEY unique_key (study_plan_id, concept_id)) AS
SELECT DISTINCT root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, concept_id, concept_name, is_cs_119_concept, study_plan_id, study_plan_name, study_plan_level, COUNT(course_code) AS n_courses, SUM(score) AS sum_score, AVG(score) AS avg_score
           FROM graph_analytics.Flourish_CS_119_Full_Data
       GROUP BY study_plan_id, concept_id
       ORDER BY study_plan_id, concept_id;
       
    ALTER TABLE graph_analytics.Flourish_CS_119_Concept_to_StudyPlan
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);
     
     
   CREATE TABLE graph_analytics.Flourish_CS_119_Course_to_StudyPlan
                (UNIQUE KEY unique_key (course_code, study_plan_id)) AS
SELECT DISTINCT course_code, course_name, study_plan_id, study_plan_name, study_plan_level
           FROM graph_analytics.Flourish_CS_119_Full_Data;
           
           
    ALTER TABLE graph_analytics.Flourish_CS_119_Course_to_StudyPlan
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);


CREATE TABLE graph_analytics.Flourish_CS_119_Course_n_lectures (UNIQUE KEY unique_key (course_code)) AS
      SELECT b.to_object_id AS course_code, COUNT(DISTINCT a.object_id) AS n_lectures
        FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated a
  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent b
          ON a.object_id = b.from_object_id
       WHERE a.object_type = 'Lecture'
         AND b.from_object_type = 'Lecture'
         AND b.to_object_type = 'Course'
         AND b.context = 'part of'
    GROUP BY b.to_object_id;

           
    ALTER TABLE graph_analytics.Flourish_CS_119_Course_n_lectures
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);
           
        
   CREATE TABLE graph_analytics.StudyPlan_Levels (
                UNIQUE KEY unique_key (study_plan_id, study_plan_level),
                KEY study_plan_id (study_plan_id),
                KEY study_plan_level (study_plan_level)
                ) AS
SELECT DISTINCT study_plan_id, study_plan_level
           FROM graph_analytics.Flourish_CS_119_Course_to_StudyPlan;
           
    ALTER TABLE graph_analytics.StudyPlan_Levels
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);
           
        