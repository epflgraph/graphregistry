-- ================================================
-- Graph traversal: Publication-Concept score edges
-- ================================================

-- ======================= Graph traversal: Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection (
                           institution_id VARCHAR(255) NOT NULL,
                           publication_id VARCHAR(255) NOT NULL,
                           concept_id     INT UNSIGNED NOT NULL,
                           score          FLOAT NOT NULL,
                           PRIMARY KEY (institution_id, publication_id, concept_id),
                           KEY institution_id (institution_id),
                           KEY publication_id (publication_id),
                           KEY concept_id (concept_id));

-- ============= Graph traversal: Publication-Concept score edges (INSERT)
    REPLACE INTO [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection
          SELECT a2c.institution_id AS institution_id,
                 a2c.object_id      AS publication_id,
                 a2c.concept_id     AS concept_id,
                 a2c.score          AS score
            FROM [[airflow]].Operations_N_Object_T_FieldsChanged tp
      INNER JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection a2c
           USING (institution_id, object_type, object_id)
           WHERE a2c.object_type = 'Publication'
             AND a2c.text_source = 'abstract'
             AND a2c.score >= 0.1
             AND tp.to_process = 1;
