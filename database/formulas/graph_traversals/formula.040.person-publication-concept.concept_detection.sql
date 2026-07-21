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

    -- ========= Iteration: '00'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '00';


    -- ========= Iteration: '01'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '01';


    -- ========= Iteration: '02'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '02';


    -- ========= Iteration: '03'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '03';


    -- ========= Iteration: '04'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '04';


    -- ========= Iteration: '05'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '05';


    -- ========= Iteration: '06'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '06';


    -- ========= Iteration: '07'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '07';


    -- ========= Iteration: '08'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '08';


    -- ========= Iteration: '09'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '09';


    -- ========= Iteration: '0a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0a';


    -- ========= Iteration: '0b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0b';


    -- ========= Iteration: '0c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0c';


    -- ========= Iteration: '0d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0d';


    -- ========= Iteration: '0e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0e';


    -- ========= Iteration: '0f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '0f';


    -- ========= Iteration: '10'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '10';


    -- ========= Iteration: '11'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '11';


    -- ========= Iteration: '12'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '12';


    -- ========= Iteration: '13'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '13';


    -- ========= Iteration: '14'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '14';


    -- ========= Iteration: '15'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '15';


    -- ========= Iteration: '16'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '16';


    -- ========= Iteration: '17'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '17';


    -- ========= Iteration: '18'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '18';


    -- ========= Iteration: '19'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '19';


    -- ========= Iteration: '1a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1a';


    -- ========= Iteration: '1b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1b';


    -- ========= Iteration: '1c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1c';


    -- ========= Iteration: '1d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1d';


    -- ========= Iteration: '1e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1e';


    -- ========= Iteration: '1f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '1f';


    -- ========= Iteration: '20'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '20';


    -- ========= Iteration: '21'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '21';


    -- ========= Iteration: '22'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '22';


    -- ========= Iteration: '23'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '23';


    -- ========= Iteration: '24'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '24';


    -- ========= Iteration: '25'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '25';


    -- ========= Iteration: '26'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '26';


    -- ========= Iteration: '27'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '27';


    -- ========= Iteration: '28'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '28';


    -- ========= Iteration: '29'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '29';


    -- ========= Iteration: '2a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2a';


    -- ========= Iteration: '2b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2b';


    -- ========= Iteration: '2c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2c';


    -- ========= Iteration: '2d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2d';


    -- ========= Iteration: '2e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2e';


    -- ========= Iteration: '2f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '2f';


    -- ========= Iteration: '30'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '30';


    -- ========= Iteration: '31'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '31';


    -- ========= Iteration: '32'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '32';


    -- ========= Iteration: '33'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '33';


    -- ========= Iteration: '34'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '34';


    -- ========= Iteration: '35'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '35';


    -- ========= Iteration: '36'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '36';


    -- ========= Iteration: '37'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '37';


    -- ========= Iteration: '38'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '38';


    -- ========= Iteration: '39'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '39';


    -- ========= Iteration: '3a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3a';


    -- ========= Iteration: '3b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3b';


    -- ========= Iteration: '3c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3c';


    -- ========= Iteration: '3d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3d';


    -- ========= Iteration: '3e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3e';


    -- ========= Iteration: '3f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '3f';


    -- ========= Iteration: '40'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '40';


    -- ========= Iteration: '41'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '41';


    -- ========= Iteration: '42'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '42';


    -- ========= Iteration: '43'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '43';


    -- ========= Iteration: '44'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '44';


    -- ========= Iteration: '45'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '45';


    -- ========= Iteration: '46'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '46';


    -- ========= Iteration: '47'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '47';


    -- ========= Iteration: '48'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '48';


    -- ========= Iteration: '49'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '49';


    -- ========= Iteration: '4a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4a';


    -- ========= Iteration: '4b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4b';


    -- ========= Iteration: '4c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4c';


    -- ========= Iteration: '4d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4d';


    -- ========= Iteration: '4e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4e';


    -- ========= Iteration: '4f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '4f';


    -- ========= Iteration: '50'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '50';


    -- ========= Iteration: '51'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '51';


    -- ========= Iteration: '52'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '52';


    -- ========= Iteration: '53'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '53';


    -- ========= Iteration: '54'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '54';


    -- ========= Iteration: '55'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '55';


    -- ========= Iteration: '56'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '56';


    -- ========= Iteration: '57'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '57';


    -- ========= Iteration: '58'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '58';


    -- ========= Iteration: '59'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '59';


    -- ========= Iteration: '5a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5a';


    -- ========= Iteration: '5b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5b';


    -- ========= Iteration: '5c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5c';


    -- ========= Iteration: '5d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5d';


    -- ========= Iteration: '5e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5e';


    -- ========= Iteration: '5f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '5f';


    -- ========= Iteration: '60'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '60';


    -- ========= Iteration: '61'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '61';


    -- ========= Iteration: '62'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '62';


    -- ========= Iteration: '63'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '63';


    -- ========= Iteration: '64'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '64';


    -- ========= Iteration: '65'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '65';


    -- ========= Iteration: '66'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '66';


    -- ========= Iteration: '67'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '67';


    -- ========= Iteration: '68'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '68';


    -- ========= Iteration: '69'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '69';


    -- ========= Iteration: '6a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6a';


    -- ========= Iteration: '6b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6b';


    -- ========= Iteration: '6c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6c';


    -- ========= Iteration: '6d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6d';


    -- ========= Iteration: '6e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6e';


    -- ========= Iteration: '6f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '6f';


    -- ========= Iteration: '70'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '70';


    -- ========= Iteration: '71'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '71';


    -- ========= Iteration: '72'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '72';


    -- ========= Iteration: '73'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '73';


    -- ========= Iteration: '74'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '74';


    -- ========= Iteration: '75'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '75';


    -- ========= Iteration: '76'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '76';


    -- ========= Iteration: '77'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '77';


    -- ========= Iteration: '78'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '78';


    -- ========= Iteration: '79'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '79';


    -- ========= Iteration: '7a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7a';


    -- ========= Iteration: '7b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7b';


    -- ========= Iteration: '7c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7c';


    -- ========= Iteration: '7d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7d';


    -- ========= Iteration: '7e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7e';


    -- ========= Iteration: '7f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '7f';


    -- ========= Iteration: '80'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '80';


    -- ========= Iteration: '81'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '81';


    -- ========= Iteration: '82'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '82';


    -- ========= Iteration: '83'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '83';


    -- ========= Iteration: '84'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '84';


    -- ========= Iteration: '85'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '85';


    -- ========= Iteration: '86'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '86';


    -- ========= Iteration: '87'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '87';


    -- ========= Iteration: '88'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '88';


    -- ========= Iteration: '89'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '89';


    -- ========= Iteration: '8a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8a';


    -- ========= Iteration: '8b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8b';


    -- ========= Iteration: '8c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8c';


    -- ========= Iteration: '8d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8d';


    -- ========= Iteration: '8e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8e';


    -- ========= Iteration: '8f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '8f';


    -- ========= Iteration: '90'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '90';


    -- ========= Iteration: '91'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '91';


    -- ========= Iteration: '92'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '92';


    -- ========= Iteration: '93'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '93';


    -- ========= Iteration: '94'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '94';


    -- ========= Iteration: '95'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '95';


    -- ========= Iteration: '96'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '96';


    -- ========= Iteration: '97'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '97';


    -- ========= Iteration: '98'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '98';


    -- ========= Iteration: '99'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '99';


    -- ========= Iteration: '9a'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9a';


    -- ========= Iteration: '9b'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9b';


    -- ========= Iteration: '9c'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9c';


    -- ========= Iteration: '9d'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9d';


    -- ========= Iteration: '9e'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9e';


    -- ========= Iteration: '9f'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = '9f';


    -- ========= Iteration: 'a0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a0';


    -- ========= Iteration: 'a1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a1';


    -- ========= Iteration: 'a2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a2';


    -- ========= Iteration: 'a3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a3';


    -- ========= Iteration: 'a4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a4';


    -- ========= Iteration: 'a5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a5';


    -- ========= Iteration: 'a6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a6';


    -- ========= Iteration: 'a7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a7';


    -- ========= Iteration: 'a8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a8';


    -- ========= Iteration: 'a9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'a9';


    -- ========= Iteration: 'aa'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'aa';


    -- ========= Iteration: 'ab'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ab';


    -- ========= Iteration: 'ac'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ac';


    -- ========= Iteration: 'ad'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ad';


    -- ========= Iteration: 'ae'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ae';


    -- ========= Iteration: 'af'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'af';


    -- ========= Iteration: 'b0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b0';


    -- ========= Iteration: 'b1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b1';


    -- ========= Iteration: 'b2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b2';


    -- ========= Iteration: 'b3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b3';


    -- ========= Iteration: 'b4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b4';


    -- ========= Iteration: 'b5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b5';


    -- ========= Iteration: 'b6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b6';


    -- ========= Iteration: 'b7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b7';


    -- ========= Iteration: 'b8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b8';


    -- ========= Iteration: 'b9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'b9';


    -- ========= Iteration: 'ba'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ba';


    -- ========= Iteration: 'bb'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'bb';


    -- ========= Iteration: 'bc'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'bc';


    -- ========= Iteration: 'bd'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'bd';


    -- ========= Iteration: 'be'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'be';


    -- ========= Iteration: 'bf'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'bf';


    -- ========= Iteration: 'c0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c0';


    -- ========= Iteration: 'c1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c1';


    -- ========= Iteration: 'c2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c2';


    -- ========= Iteration: 'c3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c3';


    -- ========= Iteration: 'c4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c4';


    -- ========= Iteration: 'c5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c5';


    -- ========= Iteration: 'c6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c6';


    -- ========= Iteration: 'c7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c7';


    -- ========= Iteration: 'c8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c8';


    -- ========= Iteration: 'c9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'c9';


    -- ========= Iteration: 'ca'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ca';


    -- ========= Iteration: 'cb'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'cb';


    -- ========= Iteration: 'cc'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'cc';


    -- ========= Iteration: 'cd'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'cd';


    -- ========= Iteration: 'ce'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ce';


    -- ========= Iteration: 'cf'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'cf';


    -- ========= Iteration: 'd0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd0';


    -- ========= Iteration: 'd1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd1';


    -- ========= Iteration: 'd2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd2';


    -- ========= Iteration: 'd3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd3';


    -- ========= Iteration: 'd4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd4';


    -- ========= Iteration: 'd5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd5';


    -- ========= Iteration: 'd6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd6';


    -- ========= Iteration: 'd7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd7';


    -- ========= Iteration: 'd8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd8';


    -- ========= Iteration: 'd9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'd9';


    -- ========= Iteration: 'da'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'da';


    -- ========= Iteration: 'db'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'db';


    -- ========= Iteration: 'dc'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'dc';


    -- ========= Iteration: 'dd'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'dd';


    -- ========= Iteration: 'de'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'de';


    -- ========= Iteration: 'df'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'df';


    -- ========= Iteration: 'e0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e0';


    -- ========= Iteration: 'e1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e1';


    -- ========= Iteration: 'e2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e2';


    -- ========= Iteration: 'e3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e3';


    -- ========= Iteration: 'e4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e4';


    -- ========= Iteration: 'e5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e5';


    -- ========= Iteration: 'e6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e6';


    -- ========= Iteration: 'e7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e7';


    -- ========= Iteration: 'e8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e8';


    -- ========= Iteration: 'e9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'e9';


    -- ========= Iteration: 'ea'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ea';


    -- ========= Iteration: 'eb'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'eb';


    -- ========= Iteration: 'ec'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ec';


    -- ========= Iteration: 'ed'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ed';


    -- ========= Iteration: 'ee'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ee';


    -- ========= Iteration: 'ef'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ef';


    -- ========= Iteration: 'f0'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f0';


    -- ========= Iteration: 'f1'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f1';


    -- ========= Iteration: 'f2'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f2';


    -- ========= Iteration: 'f3'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f3';


    -- ========= Iteration: 'f4'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f4';


    -- ========= Iteration: 'f5'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f5';


    -- ========= Iteration: 'f6'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f6';


    -- ========= Iteration: 'f7'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f7';


    -- ========= Iteration: 'f8'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f8';


    -- ========= Iteration: 'f9'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'f9';


    -- ========= Iteration: 'fa'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'fa';


    -- ========= Iteration: 'fb'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'fb';


    -- ========= Iteration: 'fc'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'fc';


    -- ========= Iteration: 'fd'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'fd';


    -- ========= Iteration: 'fe'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'fe';


    -- ========= Iteration: 'ff'
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

           WHERE t1.to_process = 1
             AND t1.idx_publication_id = 'ff';
