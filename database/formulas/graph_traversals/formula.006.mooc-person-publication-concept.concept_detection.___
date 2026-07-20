-- ============================================================
-- Graph traversal: MOOC-Person-Publication-Concept score edges
-- ============================================================

-- ======================= Graph traversal: MOOC-Person-Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_MOOC_N_Person_N_Publication_N_Concept_T_ConceptDet (
                           institution_id VARCHAR(6)   NOT NULL,
                           mooc_id        VARCHAR(32)  NOT NULL,
                           person_id      INT UNSIGNED NOT NULL,
                           publication_id VARCHAR(128) NOT NULL,
                           concept_id     INT UNSIGNED NOT NULL,
                           score          FLOAT        NOT NULL,
                           PRIMARY KEY (institution_id, mooc_id, person_id, publication_id, concept_id),
                           KEY institution_id (institution_id),
                           KEY mooc_id (mooc_id),
                           KEY person_id (person_id),
                           KEY publication_id (publication_id),
                           KEY concept_id (concept_id));

-- ============= Graph traversal: MOOC-Person-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[graph_cache]].Traversal_N_MOOC_N_Person_N_Publication_N_Concept_T_ConceptDet
          SELECT t1.from_institution_id AS institution_id,
                 t1.from_object_id      AS mooc_id,
                 t1.to_object_id        AS person_id,
                 t2.publication_id      AS publication_id,
                 t2.concept_id          AS concept_id,
                 t2.score               AS score

              -- Start with: (Person, Publication) tuples to process
            FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

              -- Link to: Person-Publication-Concept detection scores
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection t2
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id)
               = ('Publication', t2.publication_id, 'Person', t2.person_id)

              -- Link to: MOOC-Person affiliation edges
      INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent t1 
              ON (t1.from_institution_id, t1.from_object_type, t1.to_institution_id, t1.to_object_type, t1.to_object_id, t1.context)
               = (t2.institution_id, 'MOOC', t2.institution_id, 'Person', t2.person_id, 'teacher')

              -- Check type flags
      INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
              ON (tf.institution_id, tf.object_type)
               = (t2.institution_id, 'MOOC')

           WHERE tp.to_process = 1
             AND tf.to_process = 1;
