-- ============================================================================
-- Formula: downstream_deleted_flags
-- Purpose: Propagate record_deleted flags from graph_registry and graph_lectures
--          into the corresponding deleted flags in graph_airflow and graph_cache.
-- Note: graphsearch_test and elasticsearch_cache do not have delete flags.
--       graph_ontology is intentionally not included here; add it if needed.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_T_FieldsChanged
-- ----------------------------------------------------------------------------
   UPDATE [[airflow]].Operations_N_Object_T_FieldsChanged t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_N_Object_T_FieldsChanged
-- ----------------------------------------------------------------------------
   UPDATE [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_T_ScoresExpired
-- ----------------------------------------------------------------------------
   UPDATE [[airflow]].Operations_N_Object_T_ScoresExpired t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-level data tables
-- ----------------------------------------------------------------------------
   UPDATE [[graph_cache]].Data_N_Object_T_PageProfile t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Data_N_Object_T_AllFields t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Data_N_Object_T_CalculatedFields t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-object symmetric data tables
-- ----------------------------------------------------------------------------
   UPDATE [[graph_cache]].Data_N_Object_N_Object_T_AllFieldsSymmetric t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Data_N_Object_N_Object_T_CalculatedFields t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-ontology edge tables (object endpoint only)
-- ----------------------------------------------------------------------------
   UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_CalculatedScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_FinalScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_cache: Node degree scores
-- ----------------------------------------------------------------------------
   UPDATE [[graph_cache]].Nodes_N_Object_T_DegreeScores t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rp
       ON (t.object_type, t.object_id) = (rp.object_type, rp.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lp
       ON (t.object_type, t.object_id) = (lp.object_type, lp.object_id)
      SET t.deleted = COALESCE(rp.record_deleted, lp.record_deleted, t.deleted)
    WHERE rp.object_id IS NOT NULL
       OR lp.object_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-object edge tables
-- ----------------------------------------------------------------------------
   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ParentChildSymmetric t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;

   UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC t
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpf
       ON (t.from_object_type, t.from_object_id) = (rpf.object_type, rpf.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpf
       ON (t.from_object_type, t.from_object_id) = (lpf.object_type, lpf.object_id)
LEFT JOIN [[registry]].Data_N_Object_T_PageProfile rpt
       ON (t.to_object_type,   t.to_object_id)   = (rpt.object_type, rpt.object_id)
LEFT JOIN [[lectures]].Data_N_Object_T_PageProfile lpt
       ON (t.to_object_type,   t.to_object_id)   = (lpt.object_type, lpt.object_id)
      SET t.deleted = GREATEST(
           COALESCE(rpf.record_deleted, lpf.record_deleted, 0),
           COALESCE(rpt.record_deleted, lpt.record_deleted, 0)
         )
    WHERE rpf.object_id IS NOT NULL
       OR lpf.object_id IS NOT NULL
       OR rpt.object_id IS NOT NULL
       OR lpt.object_id IS NOT NULL;
