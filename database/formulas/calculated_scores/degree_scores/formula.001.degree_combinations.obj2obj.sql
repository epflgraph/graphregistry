-- ============================================================================
-- Formula: degree_combinations.obj2obj
-- Purpose: Object-to-object degree combinations over the registry ChildToParent
--          edges, aggregating BOTH directions into one row per
--          (from_object, to_object_type): SUM of the direct degree (distinct
--          linked objects as from) and the inverse degree (distinct linked
--          objects as to). Exactly one writer per triple, so the REPLACE plus
--          the table's unique key converge under recomputation instead of
--          appending a directional copy per run.
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
                     -- Direct: distinct linked objects per from-object.
                     SELECT e.from_object_type,
                            e.from_object_id,
                            e.to_object_type,
                            COUNT(DISTINCT e.to_object_id) AS directional_degree
                       FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e
                 INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                         ON (    se.object_type,     se.object_id)
                          = (e.from_object_type, e.from_object_id)
                 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                      USING (object_type)
                      WHERE e.record_deleted = 0
                        AND se.to_process = 1
                        AND se.deleted = 0
                        AND tf.flag_type  = 'scores'
                        AND tf.to_process = 1
                   GROUP BY e.from_object_type, e.from_object_id, e.to_object_type

                      UNION ALL

                     -- Inverse: distinct linked objects per to-object.
                     SELECT e.to_object_type,
                            e.to_object_id,
                            e.from_object_type,
                            COUNT(DISTINCT e.from_object_id) AS directional_degree
                       FROM [[registry]].Edges_N_Object_N_Object_T_ChildToParent e
                 INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                         ON (    se.object_type,     se.object_id)
                          = (e.to_object_type, e.to_object_id)
                 INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
                      USING (object_type)
                      WHERE e.record_deleted = 0
                        AND se.to_process = 1
                        AND se.deleted = 0
                        AND tf.flag_type  = 'scores'
                        AND tf.to_process = 1
                   GROUP BY e.to_object_type, e.to_object_id, e.from_object_type
                    ) u
            GROUP BY u.from_object_type, u.from_object_id, u.to_object_type;
