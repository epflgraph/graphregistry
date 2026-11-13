-- ========= Object type: All types
-- ========= Formula: 'concept sum-scores aggregation'

-- =================== Cleanup: find objects to be deleted
  DROP TABLE IF EXISTS [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete;
CREATE TEMPORARY TABLE [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete
                       (PRIMARY KEY (object_type, object_id), KEY object_type (object_type), KEY object_id (object_id)) AS
       SELECT DISTINCT object_type, object_id
                  FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores
                 WHERE to_process = 1
                   AND deleted = 0;

-- ========= Cleanup: Soft-delete objects marked for delete
      UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
         SET deleted = 1
       WHERE deleted = 0
         AND calculation_type = 'concept sum-scores aggregation'
         AND (object_type, object_id)
          IN (SELECT object_type, object_id FROM [[graph_cache]]._TEMP_Concept_FinalScores_objs_to_delete);

-- ========= Update calculated scores: Object-to-Category
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
             (object_type, object_id, category_id, calculation_type, score, to_process, deleted)

      SELECT fs.object_type   AS object_type,
             fs.object_id     AS object_id,
             tr.category_4_id AS category_id,
             'concept sum-scores aggregation' AS calculation_type,
             SUM(fs.score) AS score,
             1 AS to_process,
             0 AS deleted

          -- Select from pre-calculated object to concept scores
        FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores fs

          -- Join category tree from ontology
  INNER JOIN [[traversals]].Category_Cluster_Concept__FullOntology tr
          ON fs.concept_id = tr.concept_id
         AND fs.to_process = 1
         AND fs.deleted = 0

          -- Aggregate by category level 4
    GROUP BY fs.object_type, fs.object_id, tr.category_4_id;
