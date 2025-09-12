
-- ================================= --
-- ===== Refresh Degree Scores ===== -- 
-- ================================= --

                  -- Object-to-Object direct edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.from_institution_id, e.from_object_type, e.from_object_id, e.to_institution_id, e.to_object_type,
                             COUNT(DISTINCT e.to_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.to_object_id)) AS log_degree
                FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (e.from_institution_id, e.from_object_type, e.from_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.from_institution_id, e.from_object_type, e.from_object_id, e.to_institution_id, e.to_object_type;

                  -- Object-to-Object inverse edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.to_institution_id   AS from_institution_id,
                     e.to_object_type      AS from_object_type,
                     e.to_object_id        AS from_object_id,
                     e.from_institution_id AS to_institution_id,
                     e.from_object_type    AS to_object_type,
                             COUNT(DISTINCT e.from_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.from_object_id)) AS log_degree
                FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e 
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (  se.institution_id,   se.object_type,   se.object_id)
                   = (e.to_institution_id, e.to_object_type, e.to_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.to_institution_id, e.to_object_type, e.to_object_id, e.from_institution_id, e.from_object_type;




                  -- Object-to-Concept edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.institution_id   AS from_institution_id,
                     e.object_type      AS from_object_type,
                     e.object_id        AS from_object_id,
                     'Ont'              AS to_institution_id,
                     'Concept'          AS to_object_type,
                             COUNT(DISTINCT e.concept_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.concept_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.institution_id, e.object_type, e.object_id;

                  -- Concept-to-Object edges [checked]
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT 'Ont'              AS from_institution_id,
                     'Concept'          AS from_object_type,
                     e.concept_id       AS from_object_id,
                     e.institution_id   AS to_institution_id,
                     e.object_type      AS to_object_type,
                             COUNT(DISTINCT e.object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.object_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.concept_id, e.institution_id, e.object_type;








                  -- Object-to-Category edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.institution_id   AS from_institution_id,
                     e.object_type      AS from_object_type,
                     e.object_id        AS from_object_id,
                     'Ont'              AS to_institution_id,
                     'Category'          AS to_object_type,
                             COUNT(DISTINCT e.category_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.category_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.institution_id, e.object_type, e.object_id;

                  -- Category-to-Object edges [checked]
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT 'Ont'              AS from_institution_id,
                     'Category'          AS from_object_type,
                     e.category_id       AS from_object_id,
                     e.institution_id   AS to_institution_id,
                     e.object_type      AS to_object_type,
                             COUNT(DISTINCT e.object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.object_id)) AS log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
               USING (institution_id, object_type, object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.category_id, e.institution_id, e.object_type;





                  -- Lecture-to-slide edges
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
              SELECT e.to_institution_id, e.to_object_type, e.to_object_id, e.from_institution_id, e.from_object_type,
                             COUNT(DISTINCT e.from_object_id)  AS degree,
                     LOG(1 + COUNT(DISTINCT e.from_object_id)) AS log_degree
                FROM [[lectures]].Edges_N_Object_N_Object_T_ChildToParent e
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (  se.institution_id,   se.object_type,   se.object_id)
                   = (e.to_institution_id, e.to_object_type, e.to_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE (e.from_object_type, e.to_object_type) = ('Slide', 'Lecture')
                 AND se.to_process = 1
                 AND tf.to_process = 1
            GROUP BY e.to_institution_id, e.to_object_type, e.to_object_id, e.from_institution_id, e.from_object_type;

		-- 	      -- Lecture-to-concept edges
        -- REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
        --       SELECT t.institution_id   AS from_institution_id,
        --              t.object_type      AS from_object_type,
        --              t.object_id        AS from_object_id,    
		-- 	         'Ont'              AS to_institution_id,
        --              'Concept'          AS to_object_type,
        --                      COUNT(DISTINCT t.concept_id)  AS degree,
        --              LOG(1 + COUNT(DISTINCT t.concept_id)) AS log_degree
        --         FROM [[lectures]].Edges_N_Lecture_N_Concept_T_SlideSumScores_UD t
        --     GROUP BY t.institution_id, t.object_type, t.object_id;




-- ================= Calculate max log degrees
DROP TABLE IF EXISTS [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees;
        CREATE TABLE [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees AS
              SELECT from_institution_id, from_object_type, to_institution_id, to_object_type, MAX(log_degree) AS max_log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
            GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type;

-- ================= Add indices to optimise the following query
         ALTER TABLE [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees
     ADD PRIMARY KEY (from_institution_id, from_object_type, to_institution_id, to_object_type),
             ADD KEY from_institution_id (from_institution_id),
             ADD KEY from_object_type    (from_object_type),
             ADD KEY to_institution_id   (to_institution_id),
             ADD KEY to_object_type      (to_object_type);



-- ================= Calculate normalised log scores
        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees
              SELECT d.from_institution_id, d.from_object_type, d.from_object_id, d.to_institution_id, d.to_object_type, d.degree, d.log_degree, (d.log_degree / t.max_log_degree) AS norm_log_degree
                FROM [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations d
          INNER JOIN [[graph_cache]].Edges_N_Object_N_Object_T_MaxLogDegrees t
               USING (from_institution_id, from_object_type, to_institution_id, to_object_type)
          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (d.from_institution_id, d.from_object_type, d.from_object_id)
          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
               WHERE se.to_process = 1
                 AND tf.to_process = 1;


-- ================= Calculate final degree scores
        REPLACE INTO [[graph_cache]].Nodes_N_Object_T_DegreeScores
                    (institution_id, object_type, object_id, avg_degree, avg_log_degree, avg_norm_log_degree, to_process)

              SELECT d.from_institution_id  AS institution_id,
                     d.from_object_type     AS object_type,
                     d.from_object_id       AS object_id,
                     AVG(d.degree)          AS avg_degree,
                     AVG(d.log_degree)      AS avg_log_degree,
                     AVG(d.norm_log_degree) AS avg_norm_log_degree,
                     1                      AS to_process
                     
                FROM [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees d

          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (d.from_institution_id, d.from_object_type, d.from_object_id)

          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)
          
               WHERE (d.from_object_type, d.to_object_type) IN
                     (
                       ('Category', 'Course'),
                       ('Category', 'Lecture'),
                       ('Category', 'Person'),
                       ('Category', 'Publication'),
                       ('Concept', 'Course'),
                       ('Concept', 'Lecture'),
                       ('Concept', 'Publication'),
                       ('Course', 'Concept'),
                       ('Course', 'Lecture'),
                       ('Lecture', 'Concept'),
                       ('Lecture', 'Slide'),
                       ('MOOC', 'Concept'),
                       ('MOOC', 'Lecture'),
                       ('Person', 'Publication'),
                       ('Publication', 'Concept'),
                       ('Startup', 'Concept'),
                       ('Unit', 'Concept'),
                       ('Unit', 'Person'),
                       ('Widget', 'Concept')
                     )

                 AND se.to_process = 1
                 AND tf.to_process = 1

            GROUP BY d.from_institution_id, d.from_object_type, d.from_object_id;



            