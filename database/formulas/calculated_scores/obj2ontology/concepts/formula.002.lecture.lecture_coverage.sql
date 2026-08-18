-- ============ Object type: Lecture
-- ============ Formula: 'lecture coverage (bounded)'
   REPLACE INTO [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores
                (object_type, object_id, concept_id, calculation_type, score, to_process)
SELECT DISTINCT 'Lecture' AS object_type, lecture_id AS object_id, concept_id,
                'percentage coverage in lecture (bounded)' AS calculation_type,
                score, 1 AS to_process
            FROM [[traversals]].Course_Lecture_Concept__CoverageScore
           WHERE to_process = 1
             AND deleted = 0
             AND score >= 0.1;
