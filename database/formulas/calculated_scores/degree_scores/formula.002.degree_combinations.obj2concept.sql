-- ============================================================================
-- Formula: degree_combinations.obj2concept
-- Purpose: Object-to-Concept and Concept-to-Object degree combinations over
--          the concept FinalScores, aggregated into one row per
--          (from_object, to_object_type): SUM of the object-side degree
--          (distinct linked concepts) and the concept-side degree (distinct
--          linked objects). Exactly one writer per triple, so the REPLACE plus
--          the table's unique key converge under recomputation. Both branches
--          are scoped on the flagged object, as in the two formulas this
--          replaces.
-- ============================================================================

        REPLACE INTO [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations
                     (from_object_type, from_object_id, to_object_type, degree, log_degree, deleted)
              SELECT u.from_object_type,
                     u.from_object_id,
                     u.to_object_type,
                     SUM(u.directional_degree)          AS degree,
                     LOG(1 + SUM(u.directional_degree)) AS log_degree,
                     0                                  AS deleted
                FROM (
                     -- Object side: distinct linked concepts per object.
                     SELECT e.object_type      AS from_object_type,
                            e.object_id        AS from_object_id,
                            'Concept'          AS to_object_type,
                            COUNT(DISTINCT e.concept_id) AS directional_degree
                       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
                 INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                      USING (object_type, object_id)
                 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                      USING (object_type)
                      WHERE se.to_process = 1
                        AND se.deleted = 0
                        AND e.deleted = 0
                        AND tf.flag_type = 'scores'
                        AND tf.to_process = 1
                   GROUP BY e.object_type, e.object_id

                      UNION ALL

                     -- Concept side: distinct linked objects per concept.
                     SELECT 'Concept'                   AS from_object_type,
                            e.concept_id                AS from_object_id,
                            e.object_type               AS to_object_type,
                            COUNT(DISTINCT e.object_id) AS directional_degree
                       FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
                 INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                      USING (object_type, object_id)
                 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                      USING (object_type)
                      WHERE se.to_process = 1
                        AND se.deleted = 0
                        AND e.deleted = 0
                        AND tf.flag_type = 'scores'
                        AND tf.to_process = 1
                   GROUP BY e.concept_id, e.object_type
                    ) u
            GROUP BY u.from_object_type, u.from_object_id, u.to_object_type;
