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

-- ===============================================================================
-- ============= Graph traversal: Person-Publication-Concept score edges (REPLACE)
-- ===============================================================================

    REPLACE INTO [[traversals]].Person_Publication_Concept__ConceptDetection
                 (person_id, publication_id, concept_id, score, idx_publication_id, to_process)

          SELECT t1.person_id          AS person_id,
                 t1.publication_id     AS publication_id,
                 t2.concept_id         AS concept_id,
                 t2.score              AS score,
                 t1.idx_publication_id AS idx_publication_id,
                 1 AS to_process

              -- Link to: Person-Publication authorship edges
            FROM [[traversals]].Person_Publication__Authorship t1

              -- Link to: Publication-Concept detection scores
      INNER JOIN [[traversals]].Publication_Concept__ConceptDetection t2
           USING (publication_id)

           WHERE t1.to_process = 1;
