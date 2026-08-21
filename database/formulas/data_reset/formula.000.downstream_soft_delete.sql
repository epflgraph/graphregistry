-- ============================================================================
-- Formula: downstream_deleted_flags
-- Purpose: Propagate record_deleted flags from graph_registry and graph_lectures
--          into the corresponding deleted flags in graph_airflow and graph_cache.
-- Note: graphsearch_test and elasticsearch_cache do not have delete flags.
--       graph_ontology is intentionally not included here; add it if needed.
--
-- Performance note: each target table is updated with two separate INNER JOIN
-- queries, one against graph_registry and one against graph_lectures. Edge tables
-- are updated per source schema for both endpoints. Cross-schema edges (one
-- endpoint in registry, the other in lectures) are not covered by this version.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_T_FieldsChanged
-- ----------------------------------------------------------------------------
    UPDATE [[airflow]].Operations_N_Object_T_FieldsChanged t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[airflow]].Operations_N_Object_T_FieldsChanged t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_N_Object_T_FieldsChanged
-- ----------------------------------------------------------------------------
    UPDATE [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

-- ----------------------------------------------------------------------------
-- graph_airflow: Operations_N_Object_T_ScoresExpired
-- ----------------------------------------------------------------------------
    UPDATE [[airflow]].Operations_N_Object_T_ScoresExpired t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[airflow]].Operations_N_Object_T_ScoresExpired t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-level data tables
-- ----------------------------------------------------------------------------
    UPDATE [[graph_cache]].Data_N_Object_T_PageProfile t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Data_N_Object_T_PageProfile t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Data_N_Object_T_AllFields t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Data_N_Object_T_AllFields t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Data_N_Object_T_CalculatedFields t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Data_N_Object_T_CalculatedFields t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-object symmetric data tables
-- ----------------------------------------------------------------------------
    UPDATE [[graph_cache]].Data_N_Object_N_Object_T_AllFieldsSymmetric t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Data_N_Object_N_Object_T_AllFieldsSymmetric t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Data_N_Object_N_Object_T_CalculatedFields t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Data_N_Object_N_Object_T_CalculatedFields t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

-- ----------------------------------------------------------------------------
-- graph_cache: Object-ontology edge tables (object endpoint only)
-- ----------------------------------------------------------------------------
    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_CalculatedScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_FinalScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_ScoringMatrix t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Concept_T_UnionAllScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_CalculatedScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Category_T_FinalScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_CalculatedScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_CalculatedScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_FinalScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_CuratedArea_T_FinalScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

-- ----------------------------------------------------------------------------
-- graph_cache: Node degree scores
-- ----------------------------------------------------------------------------
    UPDATE [[graph_cache]].Nodes_N_Object_T_DegreeScores t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Nodes_N_Object_T_DegreeScores t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.object_type, t.object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

-- ----------------------------------------------------------------------------
-- graph_cache: Object-object edge tables
-- ----------------------------------------------------------------------------
-- Note: DegreeCombinations and NormLogDegrees do not have a to_object_id column;
--       they aggregate by (from_object_*, to_object_type). Only the from endpoint
--       is propagated here.
    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.from_object_type, t.from_object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_DegreeCombinations t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.from_object_type, t.from_object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile s
        ON (t.from_object_type, t.from_object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile s
        ON (t.from_object_type, t.from_object_id) = (s.object_type, s.object_id)
       SET t.deleted = s.record_deleted;

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ParentChildSymmetric t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ParentChildSymmetric t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC t
INNER JOIN [[registry]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[registry]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);

    UPDATE [[graph_cache]].Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC t
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile sf
        ON (t.from_object_type, t.from_object_id) = (sf.object_type, sf.object_id)
INNER JOIN [[lectures]].Data_N_Object_T_PageProfile st
        ON (t.to_object_type,   t.to_object_id)   = (st.object_type, st.object_id)
       SET t.deleted = GREATEST(sf.record_deleted, st.record_deleted);
