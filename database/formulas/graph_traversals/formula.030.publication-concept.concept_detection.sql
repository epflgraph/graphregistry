-- ================================================
-- Graph traversal: Publication-Concept score edges
-- ================================================

-- ======================= Graph traversal: Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Publication_Concept__ConceptDetection (
                            publication_id     VARCHAR(255) NOT NULL,
                            concept_id         VARCHAR(255) NOT NULL,
                            score              FLOAT NOT NULL,
                            idx_publication_id CHAR(2) DEFAULT NULL,
                            to_process         TINYINT(4) NOT NULL DEFAULT 0,
                            deleted            TINYINT(4) NOT NULL DEFAULT 0,
                            row_id             BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (publication_id,concept_id),
                            KEY publication_id (publication_id),
                            KEY concept_id (concept_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted),
                            KEY idx_publication_id (idx_publication_id));

-- ============= Cleanup: Reset to_process flags
          UPDATE [[traversals]].Publication_Concept__ConceptDetection
             SET to_process = 0
           WHERE to_process = 1;

-- ============= Soft-delete filtering for source registry rows

-- ============= Graph traversal: Publication-Concept score edges (INSERT)
    REPLACE INTO [[traversals]].Publication_Concept__ConceptDetection
                 (publication_id, concept_id, score, idx_publication_id, to_process)

          SELECT a2c.object_id          AS publication_id,
                 a2c.concept_id         AS concept_id,
                 a2c.score              AS score,
                 LEFT(a2c.object_id, 2) AS idx_publication_id,
                 1 AS to_process

            FROM [[airflow]].Operations_N_Object_T_FieldsChanged tp

      INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
           USING (object_type)

      INNER JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection a2c
              ON (tp.object_type, tp.object_id) = (a2c.object_type, a2c.object_id)
             AND a2c.record_deleted = 0

           WHERE a2c.object_type = 'Publication'
             AND a2c.score >= 0.1
             AND tp.to_process = 1
             AND tf.to_process = 1;
