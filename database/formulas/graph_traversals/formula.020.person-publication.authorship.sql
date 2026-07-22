-- ====================================================
-- Graph traversal: Person-Publication authorship edges
-- ====================================================

-- ======================= Graph traversal: Person-Publication authorship edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Person_Publication__Authorship (
                            person_id          VARCHAR(255) NOT NULL,
                            publication_id     VARCHAR(255) NOT NULL,
                            idx_publication_id CHAR(2) DEFAULT NULL,
                            to_process         TINYINT(4) NOT NULL DEFAULT 0,
                            deleted            TINYINT(4) NOT NULL DEFAULT 0,
                            row_id             BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (person_id, publication_id),
                            KEY person_id (person_id),
                            KEY publication_id (publication_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted));

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Unit_Person__Affiliation
            SET to_process = 0
          WHERE to_process = 1;

-- ============ TODO: set deleted flags for relevant edges

-- ============ Graph traversal: Person-Publication authorship edges (REPLACE)
   REPLACE INTO [[traversals]].Person_Publication__Authorship
                (person_id, publication_id, idx_publication_id, to_process)

         SELECT a2p.to_object_id            AS person_id,
                a2p.from_object_id          AS publication_id,
                LEFT(a2p.from_object_id, 2) AS idx_publication_id,
                1 AS to_process

           FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

     INNER JOIN [[airflow]].Operations_N_Object_N_Object_T_TypeFlags tf
             ON (tp.from_object_type, tp.to_object_type)
              = (tf.from_object_type, tf.to_object_type)

     INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent a2p
             ON ( tp.from_object_type,  tp.from_object_id,  tp.to_object_type,  tp.to_object_id)
              = (a2p.from_object_type, a2p.from_object_id, a2p.to_object_type, a2p.to_object_id)

          WHERE a2p.from_object_type = 'Publication'
            AND a2p.to_object_type   = 'Person'
            AND a2p.context          = 'authorship'

            AND tp.to_process = 1
            AND tf.to_process = 1;
