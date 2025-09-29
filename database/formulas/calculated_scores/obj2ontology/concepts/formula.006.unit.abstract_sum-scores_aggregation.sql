-- #------------------------------------------------------------#
-- # [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores    #
-- # (institution_id, object_type) = ('EPFL', 'Unit')           #
-- #------------------------------------------------------------#

-- ========= (institution_id, object_type) = ('EPFL', 'Unit')
-- ========= Formula: 'abstract sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT t2.institution_id AS institution_id,
             'Unit'            AS object_type,
             t2.unit_id        AS object_id,
             t2.concept_id     AS concept_id,
             'abstract sum-scores aggregation' AS calculation_type,
             SUM(t2.score)     AS score
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[graph_cache]].Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection t2
          ON (t1.institution_id, t1.object_id)
           = (t2.institution_id, t2.unit_id)
       WHERE t1.object_type = 'Unit'
         AND t1.to_process = 1
    GROUP BY t2.institution_id, t2.unit_id, t2.concept_id
      HAVING score >= 1;

-- ============= Calculate average score for 'abstract sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores e
 INNER JOIN [[graph_cache]].Data_N_Object_T_CalculatedFields d1 ON (d1.institution_id, d1.object_type, d1.object_id, d1.field_language, d1.field_name, d1.field_value) = ('EPFL', 'Unit', e.object_id, 'n/a', 'is_active_unit'  , 1)
 INNER JOIN [[graph_cache]].Data_N_Object_T_CalculatedFields d2 ON (d2.institution_id, d2.object_type, d2.object_id, d2.field_language, d2.field_name, d2.field_value) = ('EPFL', 'Unit', e.object_id, 'n/a', 'is_research_unit', 1)
        AND (e.institution_id, e.object_type, e.calculation_type) = ('EPFL', 'Unit', 'abstract sum-scores aggregation'));

-- ========= (institution_id, object_type) = ('EPFL', 'Unit')
-- ========= Formula: 'abstract sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'abstract sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-score/(4*@avg_score))) - 1) AS score
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Unit'
         AND t1.to_process = 1
         AND (t2.institution_id, t2.object_type, t2.calculation_type) = ('EPFL', 'Unit', 'abstract sum-scores aggregation');
