-- =======================================================
-- Graph traversal: Person-Publication-Concept score edges
-- =======================================================

-- ======================= Graph traversal: Person-Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Person_Publication_Concept__ConceptDetection (
                            person_id          VARCHAR(255) NOT NULL,
                            publication_id     VARCHAR(255) NOT NULL,
                            concept_id         VARCHAR(255) NOT NULL,
                            score              FLOAT NOT NULL,
                            idx_publication_id CHAR(2) DEFAULT NULL,
                            to_process         TINYINT(4) NOT NULL DEFAULT 0,
                            deleted            TINYINT(4) NOT NULL DEFAULT 0,
                            row_id             BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (person_id, publication_id, concept_id),
                            KEY person_id (person_id),
                            KEY publication_id (publication_id),
                            KEY concept_id (concept_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted),
                            KEY idx_publication_id (idx_publication_id));

-- ============= Cleanup: Reset to_process flags
          UPDATE [[traversals]].Person_Publication_Concept__ConceptDetection
             SET to_process = 0
           WHERE to_process = 1;

-- ============= TODO: set deleted flags for relevant edges

-- ============= Graph traversal: Person-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[traversals]].Person_Publication_Concept__ConceptDetection
                 (person_id, publication_id, concept_id, score, idx_publication_id, to_process)

          SELECT p2a.person_id               AS person_id,
                 p2a.publication_id          AS publication_id,
                 a2c.concept_id              AS concept_id,
                 a2c.score                   AS score,
                 LEFT(p2a.publication_id, 2) AS idx_publication_id,
                 1 AS to_process

              -- Start with: (Person, Publication) tuples to process
            FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

              -- Check type flags
      INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
              ON (tp.from_object_type, tp.to_institution_id, tp.to_object_type)
               = (tf.from_object_type, tf.to_institution_id, tf.to_object_type)

              -- Link to: Person-Publication authorship edges
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship p2a
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id)
               = ('Publication', publication_id, 'Person', person_id)

              -- Link to: Publication-Concept detection scores
      INNER JOIN [[traversals]].Publication_Concept__ConceptDetection a2c
           USING (publication_id)

           WHERE tp.to_process = 1
             AND tf.to_process = 1;
