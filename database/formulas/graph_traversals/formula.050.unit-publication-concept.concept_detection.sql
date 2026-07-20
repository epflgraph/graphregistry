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

-- ============= TODO: set deleted flags for relevant edges

-- ============= Graph traversal: Unit-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[traversals]].Unit_Publication_Concept__ConceptDetection
                 (unit_id, publication_id, concept_id, score, idx_publication_id, to_process)

          SELECT u2p.unit_id                 AS unit_id,
                 p2a.publication_id          AS publication_id,
                 a2c.concept_id              AS concept_id,
                 a2c.score                   AS score,
                 LEFT(p2a.publication_id, 2) AS idx_publication_id,
                 1 AS to_process

              -- Start with: (Unit, Person) tuples to process
            FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

              -- Link to: Unit-Person affiliation edges
      INNER JOIN [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation u2p
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id)
               = ('Person', person_id, 'Unit', unit_id)

              -- Check type flags
      INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
              ON tf.object_type = 'Unit'

              -- Link to: Person-Publication authorship edges
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship p2a
           USING (person_id)

              -- Link to: Publication-Concept detection scores
      INNER JOIN [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection a2c
           USING (publication_id)

           WHERE tp.to_process = 1
             AND tf.to_process = 1;
