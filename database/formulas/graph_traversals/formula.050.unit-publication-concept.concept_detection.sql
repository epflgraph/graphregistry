-- =====================================================
-- Graph traversal: Unit-Publication-Concept score edges
-- =====================================================

-- ======================= Graph traversal: Unit-Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Unit_Publication_Concept__ConceptDetection (
                            unit_id VARCHAR(255) NOT NULL,
                            publication_id VARCHAR(255) NOT NULL,
                            concept_id VARCHAR(255) NOT NULL,
                            score FLOAT NOT NULL,
                            idx_publication_id CHAR(2) NOT NULL,
                            to_process TINYINT(3) unsigned NOT NULL DEFAULT 0,
                            deleted TINYINT(3) unsigned NOT NULL DEFAULT 0,
                            row_id BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (unit_id, publication_id, concept_id),
                            KEY unit_id (unit_id),
                            KEY publication_id (publication_id),
                            KEY concept_id (concept_id),
                            KEY idx_publication_id (idx_publication_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted));

-- ============= Cleanup: Reset to_process flags
          UPDATE [[traversals]].Unit_Publication_Concept__ConceptDetection
             SET to_process = 0
           WHERE to_process = 1;

-- ============= Soft-delete filtering for source traversal rows

-- ============= Graph traversal: Unit-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[traversals]].Unit_Publication_Concept__ConceptDetection
                 (unit_id, publication_id, concept_id, score, idx_publication_id, to_process)

          SELECT u2p.unit_id                 AS unit_id,
                 p2a.publication_id          AS publication_id,
                 a2c.concept_id              AS concept_id,
                 a2c.score                   AS score,
                 LEFT(p2a.publication_id, 2) AS idx_publication_id,
                 1 AS to_process

              -- Start with: Unit-Person affiliation edges
            FROM [[traversals]].Unit_Person__Affiliation u2p

              -- Link to: Person-Publication authorship edges
      INNER JOIN [[traversals]].Person_Publication__Authorship p2a
           ON u2p.person_id = p2a.person_id
          AND p2a.deleted = 0

              -- Link to: Publication-Concept detection scores
      INNER JOIN [[traversals]].Publication_Concept__ConceptDetection a2c
           ON p2a.publication_id = a2c.publication_id
          AND a2c.deleted = 0

           WHERE u2p.to_process = 1
             AND u2p.deleted = 0;
