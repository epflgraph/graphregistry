-- =======================================================
-- Graph traversal: Person-Publication-Concept score edges
-- =======================================================

-- ======================= Graph traversal: Person-Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection (
                           institution_id VARCHAR(255) NOT NULL,
                           person_id      VARCHAR(255) NOT NULL,
                           publication_id VARCHAR(255) NOT NULL,
                           concept_id     INT UNSIGNED NOT NULL,
                           score          FLOAT NOT NULL,
                           PRIMARY KEY (institution_id, person_id, publication_id, concept_id),
                           KEY institution_id (institution_id),
                           KEY person_id (person_id),
                           KEY publication_id (publication_id),
                           KEY concept_id (concept_id));

-- ============= Graph traversal: Person-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[graph_cache]].Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection
          SELECT p2a.institution_id, p2a.person_id, p2a.publication_id, a2c.concept_id, a2c.score
              -- Start with: (Person, Publication) tuples to process
            FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp
              -- Link to: Person-Publication authorship edges
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship p2a
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id) = ('Publication', publication_id, 'Person', person_id)
              -- Link to: Publication-Concept detection scores
      INNER JOIN [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection a2c
              ON p2a.institution_id = a2c.institution_id AND p2a.publication_id = a2c.publication_id
             AND tp.to_process = 1;
