-- =====================================================
-- Graph traversal: Unit-Publication-Concept score edges
-- =====================================================

-- ======================= Graph traversal: Unit-Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection (
                           institution_id VARCHAR(255) NOT NULL,
                           unit_id        VARCHAR(255) NOT NULL,
                           publication_id VARCHAR(255) NOT NULL,
                           concept_id     INT UNSIGNED NOT NULL,
                           score          FLOAT NOT NULL,
                           PRIMARY KEY (institution_id, unit_id, publication_id, concept_id),
                           KEY institution_id (institution_id),
                           KEY unit_id (unit_id),
                           KEY publication_id (publication_id),
                           KEY concept_id (concept_id));

-- ============= Graph traversal: Unit-Publication-Concept score edges (REPLACE)
    REPLACE INTO [[graph_cache]].Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection
          SELECT u2p.institution_id, u2p.unit_id, p2a.publication_id, a2c.concept_id, a2c.score

              -- Start with: (Unit, Person) tuples to process
            FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

              -- Link to: Unit-Person affiliation edges
      INNER JOIN [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation u2p
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id)
               = ('Person', person_id, 'Unit', unit_id)

              -- Check type flags
      INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
              ON (tf.institution_id, tf.object_type)
               = (u2p.institution_id, 'Unit')

              -- Link to: Person-Publication authorship edges
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship p2a
              ON u2p.institution_id = p2a.institution_id
             AND u2p.person_id = p2a.person_id

              -- Link to: Publication-Concept detection scores
      INNER JOIN [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection a2c
              ON p2a.institution_id = a2c.institution_id
             AND p2a.publication_id = a2c.publication_id

           WHERE tp.to_process = 1
             AND tf.to_process = 1;
