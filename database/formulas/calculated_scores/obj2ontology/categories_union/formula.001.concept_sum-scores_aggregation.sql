-- ========= Object type: All types
-- ========= Formula: 'concept sum-scores aggregation'
REPLACE INTO [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores
             (object_type, object_id, category_id, calculation_type, score, to_process)

      SELECT fs.object_type   AS object_type,
             fs.object_id     AS object_id,
             tr.category_4_id AS category_id,
             'concept sum-scores aggregation' AS calculation_type,
             SUM(fs.score) AS score,
             fs.to_process

          -- Select from pre-calculated object to concept scores
        FROM [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores fs

          -- Join category tree from ontology
  INNER JOIN [[traversals]].Category_Cluster_Concept__FullOntology tr
          ON fs.concept_id = tr.concept_id
         AND fs.to_process = 1

          -- Aggregate by category level 4
    GROUP BY fs.object_type, fs.object_id, tr.category_4_id;
