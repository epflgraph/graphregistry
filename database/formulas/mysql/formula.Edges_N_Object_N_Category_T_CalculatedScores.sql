
-- #================================================#
-- #                                                #
-- #    Edges_N_Object_N_Category_T_FinalScores     #
-- #                                                #
-- #================================================#

-- #-----------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Category_T_FinalScores # 
-- # (institution_id, object_type) = ('EPFL', 'Person')  #
-- #-----------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Person')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
       SELECT 'EPFL'       AS institution_id, 
              'Person'     AS object_type,
              t.person_id  AS object_id,
              l.from_id    AS category_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t.score) AS score
         FROM graph_cache.Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection t
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
           ON (tf.institution_id, tf.object_type) = ('EPFL', 'Person')
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
           ON (se.institution_id, se.object_type, se.object_id) = ('EPFL', 'Person', t.person_id)
STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c
           ON t.concept_id = c.to_id
STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l
           ON c.from_id = l.to_id
        WHERE tf.to_process = 1
          AND se.to_process = 1
     GROUP BY t.person_id, l.from_id
       HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Person', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Person')
-- ========== Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Person', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;

-- #----------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Category_T_FinalScores      # 
-- # (institution_id, object_type) = ('EPFL', 'Publication')  #
-- #----------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Publication')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
       SELECT 'EPFL'           AS institution_id, 
              'Publication'    AS object_type,
              t.publication_id AS object_id,
              l.from_id        AS category_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t.score) AS score
         FROM graph_cache.Traversal_N_Publication_N_Concept_T_ConceptDetection t
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
           ON (tf.institution_id, tf.object_type) = ('EPFL', 'Person')
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
           ON (se.institution_id, se.object_type, se.object_id) = ('EPFL', 'Publication', t.publication_id)
STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c 
           ON t.concept_id = c.to_id
STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l 
           ON c.from_id = l.to_id
        WHERE tf.to_process = 1
          AND se.to_process = 1
     GROUP BY t.publication_id, l.from_id
       HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Publication', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Publication')
-- ========== Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Publication', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;

-- #------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Category_T_FinalScores  #
-- # (institution_id, object_type) = ('EPFL', 'Unit')     #
-- #------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', 'Unit')
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
       SELECT 'EPFL'      AS institution_id, 
              'Unit'      AS object_type,
              t.unit_id   AS object_id,
              l.from_id   AS category_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t.score) AS score
         FROM graph_cache.Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection t
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags tf
           ON (tf.institution_id, tf.object_type) = ('EPFL', 'Person')
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se
           ON (se.institution_id, se.object_type, se.object_id) = ('EPFL', 'Unit', t.unit_id)
STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild c 
           ON t.concept_id = c.to_id
STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l 
           ON c.from_id = l.to_id
        WHERE tf.to_process = 1
          AND se.to_process = 1
     GROUP BY t.unit_id, l.from_id
       HAVING score >= 1;

-- ============= Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Unit', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Unit')
-- ========== Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Unit', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;

-- #---------------------------------------------------------------------------#
-- # graph_cache.Edges_N_Object_N_Category_T_FinalScores                       #
-- # (institution_id, object_type) = ('EPFL', ('Course', 'Startup', 'Widget')) #
-- #---------------------------------------------------------------------------#

-- ========== (institution_id, object_type) = ('EPFL', ('Course', 'Startup', 'Widget'))
-- ========== Formula: 'concept sum-scores aggregation'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
             (institution_id, object_type, object_id, category_id, calculation_type, score)
       SELECT t.institution_id AS institution_id, 
              t.object_type    AS object_type,
              t.object_id      AS object_id,
              l.from_id        AS category_id,
              'concept sum-scores aggregation' AS calculation_type,
              SUM(t.score) AS score
         FROM graph_registry.Edges_N_Object_N_Concept_T_ConceptDetection t
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
STRAIGHT_JOIN graph_ontology.Edges_N_ConceptsCluster_N_Concept_T_ParentToChild  c ON t.concept_id = c.to_id
STRAIGHT_JOIN graph_ontology.Edges_N_Category_N_ConceptsCluster_T_ParentToChild l ON c.from_id    = l.to_id
        WHERE t.object_type IN ('Course', 'Startup', 'Widget')
          AND tf.to_process = 1
          AND se.to_process = 1
     GROUP BY t.object_id, l.from_id
       HAVING score >= 1;

-- ======== Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Course', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Course')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Course', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;

-- ======== Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Startup', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Startup')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Startup', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;

-- ======== Calculate average score for 'concept sum-scores aggregation'
SET @avg_score = (
     SELECT AVG(score)
       FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores
      WHERE (institution_id, object_type, calculation_type) = ('EPFL', 'Widget', 'concept sum-scores aggregation'));

-- ========== (institution_id, object_type) = ('EPFL', 'Widget')
-- ========= Formula: 'concept sum-scores aggregation (bounded)'
 REPLACE INTO graph_cache.Edges_N_Object_N_Category_T_FinalScores
             (institution_id, object_type, object_id, category_id, score)
       SELECT s.institution_id, s.object_type, s.object_id, s.category_id,
              (2/(1 + EXP(-s.score/(4*@avg_score))) - 1) AS score
         FROM graph_cache.Edges_N_Object_N_Category_T_CalculatedScores s
   INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags     tf USING (institution_id, object_type)
   INNER JOIN graph_airflow.Operations_N_Object_T_ScoresExpired se USING (institution_id, object_type, object_id)
        WHERE (s.institution_id, s.object_type, s.calculation_type) = ('EPFL', 'Widget', 'concept sum-scores aggregation')
          AND se.to_process = 1
          AND tf.to_process = 1;