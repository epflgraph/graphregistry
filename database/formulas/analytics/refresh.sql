
-- ========
-- ========
DELETE FROM _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE object_type = 'Lecture'
        AND calculation_type IN ('top concepts ai validated', 'slide count ai validated');


-- ===========
-- ===========
TRUNCATE TABLE graph_analytics._flourish_temp;
TRUNCATE TABLE graph_analytics.Flourish_CS_119_Concept_to_Course;
TRUNCATE TABLE graph_analytics.Flourish_CS_119_Concept_to_StudyPlan;
TRUNCATE TABLE graph_analytics.Flourish_CS_119_Course_n_lectures;
TRUNCATE TABLE graph_analytics.Flourish_CS_119_Course_to_StudyPlan;
TRUNCATE TABLE graph_analytics.Flourish_CS_119_Full_Data;
TRUNCATE TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix;
TRUNCATE TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix_pLevel;
TRUNCATE TABLE graph_analytics.StudyPlan_Levels;


-- ========= Calculate number of slides for each lecture
REPLACE INTO graph_analytics._course_lecture_n_slides
      SELECT course_id, lecture_id, COUNT(DISTINCT slide_id) AS n_slides
        FROM graph_cache.Traversal_N_Course_N_Lecture_N_Slide
    GROUP BY course_id, lecture_id;

-- ========= Calculate number of lectures for each course
REPLACE INTO graph_analytics._course_n_lectures
      SELECT course_id, COUNT(DISTINCT lecture_id) AS n_lectures
        FROM graph_cache.Traversal_N_Course_N_Lecture_N_Slide
    GROUP BY course_id;


-- ============ Object type: Lecture
-- ============ Formula: 'top concepts ai validated'
   REPLACE INTO _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
               (institution_id, object_type, object_id, concept_id, calculation_type, score)
         SELECT institution_id, object_type, object_id, concept_id,
                'top concepts ai validated' AS calculation_type, score
           FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated
          WHERE object_type = 'Lecture';


-- ============ Object type: Lecture
-- ============ Formula: 'slide count ai validated'
   REPLACE INTO _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
                (institution_id, object_type, object_id, concept_id, calculation_type, score)
         SELECT 'EPFL' AS institution_id, 'Lecture' AS object_type, lecture_id AS object_id, concept_id,
                'slide count ai validated' AS calculation_type,
                -- COUNT(DISTINCT slide_id) / MAX(n_slides) AS score
                COUNT(DISTINCT slide_id) / MAX(n_slides) * (1 - EXP(-MAX(n_slides) / 15.0)) AS score
           FROM graph_cache.Traversal_N_Course_N_Lecture_N_Slide_N_Concept_T_LLMValidated t
     INNER JOIN graph_analytics._course_lecture_n_slides
          USING (lecture_id)
       GROUP BY lecture_id, concept_id;


-- ============
-- ============
   REPLACE INTO graph_analytics._flourish_temp
                (course_code, concept_id, score)
         SELECT p.to_object_id AS course_code, c.concept_id, AVG(c.score) AS score
           FROM _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores c
     INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent p
             ON (c.object_type, c.object_id, 'part of') = (p.from_object_type, p.from_object_id, p.context)
          WHERE c.object_type = 'Lecture'
            AND c.calculation_type IN ('slide count ai validated')
       GROUP BY p.to_object_id, c.concept_id;


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_CS_119_Full_Data
				(root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id,
                course_code, course_name, concept_id, concept_name, is_cs_119_concept,
                study_plan_id, study_plan_name, study_plan_level, score)
         SELECT n5.name AS root, n4.name AS category_name_1, n3.name AS category_name_2, n2.name AS category_name_3, n1.name AS category_name_4, a.to_id AS cluster_id,
                q1.course_code, o2.object_title AS course_name, concept_id, o3.name AS concept_name, 0 AS is_cs_119_concept,
                o1.object_id AS study_plan_id,
                IF(q1.course_code LIKE 'CS-119%', CONCAT(o1.object_title, ' [CS-119]'), o1.object_title) AS study_plan_name,
                IF(q1.course_code LIKE 'CS-119%', CONCAT(c1.field_value , ' [CS-119]'), c1.field_value)  AS study_plan_level,
                q1.score
           FROM graph_analytics._flourish_temp q1
     INNER JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent q2 ON ('Course', 'StudyPlan', 'coursebook', q1.course_code) = (q2.from_object_type, q2.to_object_type, q2.context, q2.from_object_id)
     INNER JOIN graph_registry.Data_N_Object_T_CustomFields c1 ON (q2.to_object_type, q2.to_object_id, 'en', 'level') = (c1.object_type, c1.object_id, c1.field_language, c1.field_name)
     INNER JOIN graph_registry.Nodes_N_Object  o1 ON ('StudyPlan', c1.object_id  ) = (o1.object_type, o1.object_id)
     INNER JOIN graph_registry.Nodes_N_Object  o2 ON ('Course'   , q1.course_code) = (o2.object_type, o2.object_id)
     INNER JOIN graph_ontology.Nodes_N_Concept o3 ON ('Concept'  , q1.concept_id ) = (o3.object_type, o3.object_id)
     INNER JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild  b ON b.to_id = q1.concept_id
     INNER JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild a ON b.from_id = a.to_id
     INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent t1 ON t1.from_id = a.from_id
     INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent t2 ON t2.from_id = t1.to_id
     INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent t3 ON t3.from_id = t2.to_id
     INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent t4 ON t4.from_id = t3.to_id
     INNER JOIN graph_ontology.Edges_N_Category_N_Category_T_ChildToParent t5 ON t5.from_id = t4.to_id
     INNER JOIN graph_ontology.Nodes_N_Category n1 ON n1.id = t1.from_id
     INNER JOIN graph_ontology.Nodes_N_Category n2 ON n2.id = t2.from_id
     INNER JOIN graph_ontology.Nodes_N_Category n3 ON n3.id = t3.from_id
     INNER JOIN graph_ontology.Nodes_N_Category n4 ON n4.id = t4.from_id
     INNER JOIN graph_ontology.Nodes_N_Category n5 ON n5.id = t5.from_id
          WHERE o1.object_id LIKE '%2025-2026';


-- ============
-- ============
         UPDATE graph_analytics.Flourish_CS_119_Full_Data
            SET is_cs_119_concept = 1
          WHERE concept_id IN (SELECT concept_id FROM graph_analytics.Flourish_CS_119_Full_Data WHERE course_code LIKE 'CS-119%');


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_CS_119_Concept_to_Course
               (root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, course_code, course_name, concept_id, concept_name, is_cs_119_concept, score)
SELECT DISTINCT root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, course_code, course_name, concept_id, concept_name, is_cs_119_concept, score
           FROM graph_analytics.Flourish_CS_119_Full_Data;


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_CS_119_Concept_to_StudyPlan
               (root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, concept_id, concept_name, is_cs_119_concept, study_plan_id, study_plan_name, study_plan_level, n_courses, sum_score, avg_score)
SELECT DISTINCT root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id, concept_id, concept_name, is_cs_119_concept, study_plan_id, study_plan_name, study_plan_level, COUNT(course_code) AS n_courses, SUM(score) AS sum_score, AVG(score) AS avg_score
           FROM graph_analytics.Flourish_CS_119_Full_Data
       GROUP BY study_plan_id, concept_id
       ORDER BY study_plan_id, concept_id;


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_CS_119_Course_to_StudyPlan
               (course_code, course_name, study_plan_id, study_plan_name, study_plan_level)
SELECT DISTINCT course_code, course_name, study_plan_id, study_plan_name, study_plan_level
           FROM graph_analytics.Flourish_CS_119_Full_Data;


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_CS_119_Course_n_lectures
                (course_code, n_lectures)       
         SELECT course_id, COUNT(DISTINCT lecture_id) AS n_lectures
           FROM graph_cache.Traversal_N_Course_N_Lecture_N_Slide_N_Concept_T_LLMValidated
       GROUP BY course_id;


-- ============
-- ============
   REPLACE INTO graph_analytics.StudyPlan_Levels
               (study_plan_id, study_plan_level)
SELECT DISTINCT study_plan_id, study_plan_level
           FROM graph_analytics.Flourish_CS_119_Course_to_StudyPlan;


-- ===================
-- ===================
  DROP TABLE IF EXISTS graph_analytics.tmp_plan_concepts;
CREATE TEMPORARY TABLE graph_analytics.tmp_plan_concepts AS
       SELECT DISTINCT study_plan_id, study_plan_name, concept_id
                  FROM graph_analytics.Flourish_CS_119_Concept_to_StudyPlan;

-- ===================
-- ===================
           ALTER TABLE graph_analytics.tmp_plan_concepts
       ADD PRIMARY KEY (study_plan_id, concept_id),
             ADD INDEX idx_concept_plan (concept_id, study_plan_id),
             ADD INDEX idx_plan (study_plan_id);


-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_StudyPlan_Jaccard_Matrix
                (from_study_plan_id, from_study_plan_name, to_study_plan_id, to_study_plan_name, intersection_concepts, from_concepts, to_concepts, union_concepts, score)

           WITH plan_sizes AS (
                SELECT study_plan_id, MAX(study_plan_name) AS study_plan_name, COUNT(*) AS n_concepts
                  FROM graph_analytics.tmp_plan_concepts
              GROUP BY study_plan_id),
                
                intersections AS (
                SELECT a.study_plan_id AS from_study_plan_id, b.study_plan_id AS to_study_plan_id, COUNT(*) AS intersection_concepts
                  FROM graph_analytics.tmp_plan_concepts a
                  JOIN graph_analytics.tmp_plan_concepts b ON b.concept_id = a.concept_id
              GROUP BY a.study_plan_id, b.study_plan_id)

        SELECT i.from_study_plan_id, pf.study_plan_name, i.to_study_plan_id, pt.study_plan_name, i.intersection_concepts,
               pf.n_concepts, pt.n_concepts, pf.n_concepts + pt.n_concepts - i.intersection_concepts AS union_concepts,
               i.intersection_concepts / NULLIF(pf.n_concepts + pt.n_concepts - i.intersection_concepts, 0) AS score
          FROM intersections i
          JOIN plan_sizes pf ON pf.study_plan_id = i.from_study_plan_id
          JOIN plan_sizes pt ON pt.study_plan_id = i.to_study_plan_id;
    

-- ============
-- ============
   REPLACE INTO graph_analytics.Flourish_StudyPlan_Jaccard_Matrix_pLevel
               (from_study_plan_id, from_study_plan_name, from_study_plan_name_w_level, from_study_plan_level, to_study_plan_id, to_study_plan_name, to_study_plan_name_w_level, to_study_plan_level, intersection_concepts, from_concepts, to_concepts, union_concepts, score)
         SELECT from_study_plan_id, from_study_plan_name, CONCAT(from_study_plan_name, ' [', c1.study_plan_level, ']') AS from_study_plan_name_w_level, c1.study_plan_level AS from_study_plan_level,
                to_study_plan_id,   to_study_plan_name, CONCAT(  to_study_plan_name, ' [', c2.study_plan_level, ']') AS   to_study_plan_name_w_level, c2.study_plan_level AS   to_study_plan_level,
                intersection_concepts, from_concepts, to_concepts, union_concepts, score
           FROM graph_analytics.Flourish_StudyPlan_Jaccard_Matrix m
     INNER JOIN graph_analytics.StudyPlan_Levels c1
             ON m.from_study_plan_id = c1.study_plan_id
     INNER JOIN graph_analytics.StudyPlan_Levels c2
             ON m.to_study_plan_id = c2.study_plan_id;
