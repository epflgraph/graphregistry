
-- =========================== Start with collecting the course IDs that should be reset/deleted
DROP TEMPORARY TABLE IF EXISTS [[graph_cache]]._objects_to_delete;
        CREATE TEMPORARY TABLE [[graph_cache]]._objects_to_delete (
                               object_type      VARCHAR(32)  COLLATE utf8mb4_unicode_ci NOT NULL,
                               object_id        VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                               calculation_type VARCHAR(64)  COLLATE utf8mb4_unicode_ci NOT NULL,
                               PRIMARY KEY (object_type, object_id, calculation_type),
                               KEY object_type (object_type),
                               KEY object_id (object_id),
                               KEY calculation_type (calculation_type));
                   INSERT INTO [[graph_cache]]._objects_to_delete (object_type, object_id, calculation_type)
               SELECT DISTINCT 'Course', course_id, 'average coverage over all lectures (bounded)'
                          FROM graph_traversals.Course_Concept__CoverageScore
                         WHERE to_process = 1
                           AND deleted = 0;

                           -- TODO: this should be the airflow scores tab;le

-- ======= Apply soft-delete to respective courses (on all scores tables)
-- ======= before re-calculating the scores for those courses

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id, calculation_type)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id, calculation_type)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id, calculation_type)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_CalculatedScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id, calculation_type)
       SET e.deleted = 1;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_FinalScores e
INNER JOIN [[graph_cache]]._objects_to_delete d
     USING (object_type, object_id)
       SET e.deleted = 1;

-- ============ Object type: Course
-- ============ Formula: 'average lecture coverage (bounded)'
   REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
                (object_type, object_id, concept_id, calculation_type, score, to_process)
SELECT DISTINCT 'Course' AS object_type, course_id AS object_id, concept_id,
                'average coverage over all lectures (bounded)' AS calculation_type,
                score, to_process
           FROM [[traversals]].Course_Concept__CoverageScore
          WHERE to_process = 1
            AND deleted = 0
            AND score >= 0.1;
