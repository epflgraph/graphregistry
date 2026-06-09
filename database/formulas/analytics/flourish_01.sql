
        --  CREATE TABLE graph_analytics.temp (
		-- 			  KEY course_code (course_code),
		-- 			  KEY concept_id (concept_id),
		-- 			  UNIQUE KEY unique_key (course_code, concept_id)
        --               ) AS
         REPLACE INTO graph_analytics.temp
                      (course_code, concept_id, score)

               SELECT p.to_object_id AS course_code, c.concept_id, AVG(c.score) AS score
				 FROM _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores c
		   INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent p
				   ON (c.object_type, c.object_id, 'part of') = (p.from_object_type, p.from_object_id, p.context)
				WHERE c.object_type = 'Lecture'
                  AND c.calculation_type IN ('slide count ai validated', 'top concepts ai validated')
			 GROUP BY p.to_object_id, c.concept_id;

--  CREATE TABLE graph_analytics.Flourish_v3 AS
 REPLACE INTO graph_analytics.Flourish_v3
              (root, category_name_1, category_name_2, category_name_3, category_name_4, cluster_id,
              course_code, course_name, concept_id, concept_name, is_cs_119_concept,
              study_plan_id, study_plan_name, study_plan_level, score)

       SELECT n5.name AS root, n4.name AS category_name_1, n3.name AS category_name_2, n2.name AS category_name_3, n1.name AS category_name_4, a.to_id AS cluster_id,
              q1.course_code, o2.object_title AS course_name, concept_id, o3.name AS concept_name, 0 AS is_cs_119_concept,
              o1.object_id AS study_plan_id,
              IF(q1.course_code LIKE 'CS-119%', CONCAT(o1.object_title, ' [CS-119]'), o1.object_title) AS study_plan_name,
              IF(q1.course_code LIKE 'CS-119%', CONCAT(c1.field_value , ' [CS-119]'), c1.field_value)  AS study_plan_level,
              q1.score
         FROM graph_analytics.temp q1
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


       UPDATE graph_analytics.Flourish_v3
          SET is_cs_119_concept = 1
        WHERE concept_id IN (SELECT concept_id FROM graph_analytics.Flourish_v3 WHERE course_code LIKE 'CS-119%');
