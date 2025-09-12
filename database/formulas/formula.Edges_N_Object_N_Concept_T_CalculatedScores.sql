
-- #====================================================#
-- #                                                    #
-- #    Edges_N_Object_N_Concept_T_CalculatedScores     #
-- #                                                    #
-- #====================================================#

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'Course')         #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'Course')
-- ========= Formula: 'slide sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)

      SELECT t4.institution_id, t4.object_type, t4.object_id, t1.concept_id,
             'slide sum-scores aggregation' AS calculation_type,
             SUM(t1.score) AS score

        FROM graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t1

  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
          ON (     t1.institution_id,      t1.object_type,      t1.object_id)
           = (t2.from_institution_id, t2.from_object_type, t2.from_object_id)

  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t3
          ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id)
           = (t3.from_institution_id, t3.from_object_type, t3.from_object_id)

  INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired t4
          ON (t3.to_institution_id, t3.to_object_type, t3.to_object_id)
           = (   t4.institution_id,    t4.object_type,    t4.object_id)

       WHERE t1.object_type = 'Slide'
         AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
         AND (t3.from_object_type, t3.to_object_type) = ('Lecture', 'Course')
         AND t4.object_type = 'Course'
         AND t4.to_process = 1

         AND 1 = (SELECT to_process
        FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
       WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
           = ('EPFL', 'Course', 'Ont', 'Concept', 'scores'))

    GROUP BY t4.institution_id, t4.object_type, t4.object_id, t1.concept_id;

-- ============= Calculate average score for 'slide sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Course', 'slide sum-scores aggregation')
        AND score>=1);

-- ========= (institution_id, object_type) = ('EPFL', 'Course')
-- ========= Formula: 'slide sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'slide sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Course'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'Course', 'slide sum-scores aggregation')
         AND t2.score >= 1;

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'MOOC')           #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'slide sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)

      SELECT t4.institution_id, t4.object_type, t4.object_id, t1.concept_id,
             'slide sum-scores aggregation' AS calculation_type,
             SUM(t1.score) AS score

        FROM graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t1

  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
          ON (     t1.institution_id,      t1.object_type,      t1.object_id)
           = (t2.from_institution_id, t2.from_object_type, t2.from_object_id)

  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t3
          ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id)
           = (t3.from_institution_id, t3.from_object_type, t3.from_object_id)

  INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired t4
          ON (t3.to_institution_id, t3.to_object_type, t3.to_object_id)
           = (   t4.institution_id,    t4.object_type,    t4.object_id)

       WHERE t1.object_type = 'Slide'
         AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
         AND (t3.from_object_type, t3.to_object_type) = ('Lecture', 'MOOC')
         AND t4.object_type = 'MOOC'
         AND t4.to_process = 1
     
         AND 1 = (SELECT to_process
                    FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                   WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
                       = ('EPFL', 'MOOC', 'Ont', 'Concept', 'scores'))
          
    GROUP BY t4.institution_id, t4.object_type, t4.object_id, t1.concept_id;

-- ============= Calculate average score for 'slide sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'MOOC', 'slide sum-scores aggregation')
        AND score>=1);

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'slide sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'slide sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'MOOC'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'MOOC', 'slide sum-scores aggregation')
         AND t2.score >= 1;

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'Lecture')        #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'Lecture')
-- ========= Formula: 'slide sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)

      SELECT t3.institution_id, t3.object_type, t3.object_id, t1.concept_id,
             'slide sum-scores aggregation' AS calculation_type,
             SUM(t1.score) AS score

        FROM graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection t1

  INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
          ON (     t1.institution_id,      t1.object_type,      t1.object_id)
           = (t2.from_institution_id, t2.from_object_type, t2.from_object_id)

  INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired t3
          ON (t2.to_institution_id, t2.to_object_type, t2.to_object_id)
           = (   t3.institution_id,    t3.object_type,    t3.object_id)

       WHERE t1.object_type = 'Slide'
         AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
         AND t3.object_type = 'Lecture'
         AND t3.to_process = 1
     
         AND 1 = (SELECT to_process
                    FROM graph_airflow.Operations_N_Object_N_Object_T_TypeFlags
                   WHERE (from_institution_id, from_object_type, to_institution_id, to_object_type, flag_type)
                       = ('EPFL', 'Lecture', 'Ont', 'Concept', 'scores'))
          
    GROUP BY t3.institution_id, t3.object_type, t3.object_id, t1.concept_id;

-- ============= Calculate average score for 'slide sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Lecture', 'slide sum-scores aggregation')
        AND score>=1);

-- ========= (institution_id, object_type) = ('EPFL', 'Lecture')
-- ========= Formula: 'slide sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'slide sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Lecture'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'Lecture', 'slide sum-scores aggregation')
         AND t2.score >= 1;

-- ========= (institution_id, object_type) = ('EPFL', 'Lecture')
-- ========= Formula: 'LLM keyword extraction (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'LLM keyword extraction (bounded)' AS calculation_type,
             LEAST(score, 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_LLMTags t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Lecture'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type) = ('EPFL', 'Lecture');


     -- SELECT 'EPFL'      AS institution_id,
     --        'Lecture'   AS object_type,
     --        lecture_id  AS object_id,
     --        concept_id  AS concept_id,
     --        'LLM keyword extraction (bounded)' AS calculation_type,
     --        LEAST(score, 1) AS score
     --   FROM data_augmentation.Lectures_Concepts;

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'MOOC')           #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'people sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT t2.institution_id AS institution_id,
             'MOOC'            AS object_type,
             t2.mooc_id        AS object_id, 
             t2.concept_id     AS concept_id,
             'people sum-scores aggregation' AS calculation_type,
             SUM(t2.score)     AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Traversal_N_MOOC_N_Publication_N_Concept_T_ConceptDetection t2
          ON (t1.institution_id, t1.object_id)
           = (t2.institution_id, t2.mooc_id)
       WHERE t1.object_type = 'MOOC'
         AND t1.to_process = 1
    GROUP BY t2.mooc_id, t2.concept_id
      HAVING score >= 1;

-- ============= Calculate average score for 'people sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'MOOC', 'people sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'MOOC')
-- ========= Formula: 'people sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'people sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'MOOC'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'MOOC', 'people sum-scores aggregation');

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'Person')         #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'Person')
-- ========= Formula: 'abstract sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT t2.institution_id AS institution_id,
             'Person'       AS object_type,
             t2.person_id      AS object_id,
             t2.concept_id     AS concept_id,
             'abstract sum-scores aggregation' AS calculation_type,
             SUM(t2.score)     AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection t2
          ON (t1.institution_id, t1.object_id)
           = (t2.institution_id, t2.person_id)
       WHERE t1.object_type = 'Person'
         AND t1.to_process = 1
    GROUP BY t2.institution_id, t2.person_id, t2.concept_id
      HAVING score >= 1;

-- ============= Calculate average score for 'abstract sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Person', 'abstract sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'Person')
-- ========= Formula: 'abstract sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'abstract sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Person'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'Person', 'abstract sum-scores aggregation');

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'Unit')           #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'Unit')
-- ========= Formula: 'abstract sum-scores aggregation'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT t2.institution_id AS institution_id,
             'Unit'            AS object_type,
             t2.unit_id        AS object_id,
             t2.concept_id     AS concept_id,
             'abstract sum-scores aggregation' AS calculation_type,
             SUM(t2.score)     AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection t2
          ON (t1.institution_id, t1.object_id)
           = (t2.institution_id, t2.unit_id)
       WHERE t1.object_type = 'Unit'
         AND t1.to_process = 1
    GROUP BY t2.institution_id, t2.unit_id, t2.concept_id
      HAVING score >= 1;

-- ============= Calculate average score for 'abstract sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores e
 INNER JOIN graph_cache.Data_N_Object_T_CalculatedFields d1 ON (d1.institution_id, d1.object_type, d1.object_id, d1.field_language, d1.field_name, d1.field_value) = ('EPFL', 'Unit', e.object_id, 'n/a', 'is_active_unit'  , 1)
 INNER JOIN graph_cache.Data_N_Object_T_CalculatedFields d2 ON (d2.institution_id, d2.object_type, d2.object_id, d2.field_language, d2.field_name, d2.field_value) = ('EPFL', 'Unit', e.object_id, 'n/a', 'is_research_unit', 1)
        AND (e.institution_id, e.object_type, e.calculation_type) = ('EPFL', 'Unit', 'abstract sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'Unit')
-- ========= Formula: 'abstract sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'abstract sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM graph_airflow.Operations_N_Object_T_ScoresExpired t1
  INNER JOIN graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Unit'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'Unit', 'abstract sum-scores aggregation');

-- #------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('Ont', 'Category')        #
-- #------------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Category')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
             (institution_id, object_type, object_id, concept_id, calculation_type, score)
       SELECT 'Ont'             AS institution_id,
              'Category'        AS object_type,
              t1.from_id        AS object_id,
              t3.to_id          AS concept_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t3.score)     AS score
         FROM graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild t1
STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t2
           ON t1.to_id = t2.from_id
STRAIGHT_JOIN graph_ontology.Edges_N_Concept_N_Concept_T_Symmetric t3
           ON t2.to_id = t3.from_id
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
           ON (se.institution_id, se.object_type, se.object_id) = ('Ont', 'Category', t1.from_id)
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
           ON (se.institution_id, se.object_type) = (tf.institution_id, tf.object_type)
        WHERE se.to_process = 1
          AND tf.to_process = 1
     GROUP BY t1.from_id, t3.to_id
       HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('Ont', 'Category', 'concept sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'Category')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
REPLACE INTO graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT s.institution_id, s.object_type, s.object_id, s.concept_id,
             'concept sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
        FROM graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores s      
  INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
          ON (se.institution_id, se.object_type, se.object_id) = ('Ont', 'Category', s.object_id)
  INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
          ON (se.institution_id, se.object_type) = (tf.institution_id, tf.object_type)        
       WHERE (s.institution_id, s.object_type, s.calculation_type) = ('Ont', 'Category', 'concept sum-scores aggregation')
         AND se.to_process = 1
         AND tf.to_process = 1;


