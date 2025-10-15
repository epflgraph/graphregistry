
-- ========= Object type: Course
-- ========= Formula: 'slide sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)

      SELECT t4.institution_id, t4.object_type, t4.object_id, t1.concept_id,
             'slide sum-scores aggregation' AS calculation_type,
             SUM(t1.score) AS score

        FROM [[lectures]].Edges_N_Object_N_Concept_T_ConceptDetection t1

  INNER JOIN [[lectures]].Edges_N_Object_N_Object_T_ChildToParent t2
          ON (     t1.institution_id,      t1.object_type,      t1.object_id)
           = (t2.from_institution_id, t2.from_object_type, t2.from_object_id)

  INNER JOIN [[lectures]].Edges_N_Object_N_Object_T_ChildToParent t3
          ON (  t2.to_institution_id,   t2.to_object_type,   t2.to_object_id)
           = (t3.from_institution_id, t3.from_object_type, t3.from_object_id)

  INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired t4
          ON (t3.to_institution_id, t3.to_object_type, t3.to_object_id)
           = (   t4.institution_id,    t4.object_type,    t4.object_id)
           
  INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
          ON (t4.institution_id, t4.object_type)
           = (tf.institution_id, tf.object_type)

       WHERE t1.object_type = 'Slide'
         AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
         AND (t3.from_object_type, t3.to_object_type) = ('Lecture', 'Course')
         AND t4.object_type = 'Course'
         AND t4.to_process  = 1
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1

    GROUP BY t4.institution_id, t4.object_type, t4.object_id, t1.concept_id;

-- ======== Calculate average score for 'slide sum-scores aggregation'
SET @avg_score = (
     SELECT COALESCE(AVG(score), 1)
       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
      USING (institution_id, object_type)
      WHERE (object_type, calculation_type) = ('Course', 'slide sum-scores aggregation')
        AND score >= 1
        AND tf.flag_type  = 'scores'
        AND tf.to_process = 1
);
        
-- ========= Formula: 'slide sum-scores aggregation (bounded)'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'slide sum-scores aggregation (bounded)' AS calculation_type,
             (2/(1 + EXP(-t2.score/(4*@avg_score))) - 1) AS score
        FROM [[airflow]].Operations_N_Object_T_ScoresExpired t1
  INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
       USING (institution_id, object_type)
  INNER JOIN [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t2
       USING (institution_id, object_type, object_id)
       WHERE t1.object_type = 'Course'
         AND t1.to_process  = 1
         AND tf.flag_type   = 'scores'
         AND tf.to_process  = 1
         AND (t2.object_type, t2.calculation_type) = ('Course', 'slide sum-scores aggregation')
         AND t2.score >= 1;
