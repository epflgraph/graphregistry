
-- #=====================================================#
-- #                                                     #
-- #    Calculate and cache relevant Graph Traversals    #
-- #                                                     #
-- #=====================================================#


-- ==============================================
-- Graph traversal: Unit-Person affiliation edges
-- ==============================================

-- ======================= Graph traversal: Unit-Person affiliation edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation (
                           institution_id VARCHAR(255) NOT NULL,
                           unit_id        VARCHAR(255) NOT NULL,
                           person_id      VARCHAR(255) NOT NULL,
                           position_group VARCHAR(255) NOT NULL,
                           PRIMARY KEY (institution_id, unit_id, person_id),
                           KEY institution_id (institution_id),
                           KEY unit_id (unit_id),
                           KEY person_id (person_id),
                           KEY position_group (position_group));

-- ============ Graph traversal: Unit-Person affiliation edges (REPLACE)
   REPLACE INTO [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation
         SELECT p2u.to_institution_id   AS institution_id,
                p2u.to_object_id        AS unit_id,
                p2u.from_object_id      AS person_id,
                f2.from_field_value     AS position_group
                
           FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

     INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent p2u
             ON ( tp.from_institution_id,  tp.from_object_type,  tp.from_object_id,    tp.to_institution_id,    tp.to_object_type,    tp.to_object_id)
              = (p2u.from_institution_id, p2u.from_object_type, p2u.from_object_id,   p2u.to_institution_id,   p2u.to_object_type,   p2u.to_object_id)
     
     INNER JOIN [[registry]].Data_N_Object_N_Object_T_CustomFields cf
             ON ( cf.from_institution_id,  cf.from_object_type,  cf.from_object_id,    cf.to_institution_id,    cf.to_object_type,    cf.to_object_id)
              = (p2u.from_institution_id, p2u.from_object_type, p2u.from_object_id,   p2u.to_institution_id,   p2u.to_object_type,   p2u.to_object_id)

     INNER JOIN [[registry]].Mapping_N_Field_N_Field f1 ON cf.field_value    = f1.from_field_value
     INNER JOIN [[registry]].Mapping_N_Field_N_Field f2 ON f1.to_field_value = f2.from_field_value
          WHERE (f1.from_object_type, f1.from_field_name, f1.to_object_type, f1.to_field_name) = ('Person', 'position_name' , 'Unit', 'position_group')
            AND (f2.from_object_type, f2.from_field_name, f2.to_object_type, f2.to_field_name) = ('Person', 'position_group', 'Unit', 'position_rank')
            
            AND (p2u.from_object_type, p2u.to_object_type) = ('Person', 'Unit')
            AND cf.field_name = 'current_position_name'
            AND f2.from_field_value IN ('Professor', 'Director', 'Researcher')

            AND tp.to_process = 1;

-- ====================================================
-- Graph traversal: Person-Publication authorship edges
-- ====================================================

-- ======================= Graph traversal: Person-Publication authorship edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship (
                           institution_id VARCHAR(255) NOT NULL,
                           person_id      VARCHAR(255) NOT NULL,
                           publication_id VARCHAR(255) NOT NULL,
                           PRIMARY KEY (institution_id, person_id, publication_id),
                           KEY institution_id (institution_id),
                           KEY person_id (person_id),
                           KEY publication_id (publication_id));

-- ============ Graph traversal: Person-Publication authorship edges (REPLACE)
   REPLACE INTO [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship
         SELECT a2p.to_institution_id   AS institution_id,
                a2p.to_object_id        AS person_id,
                a2p.from_object_id      AS publication_id

           FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

     INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent a2p
             ON ( tp.from_institution_id,  tp.from_object_type,  tp.from_object_id,    tp.to_institution_id,    tp.to_object_type,    tp.to_object_id)
              = (a2p.from_institution_id, a2p.from_object_type, a2p.from_object_id,   a2p.to_institution_id,   a2p.to_object_type,   a2p.to_object_id)

          WHERE a2p.from_object_type = 'Publication'
            AND a2p.to_object_type   = 'Person'
            AND a2p.context          = 'authorship'
            
            AND tp.to_process = 1;

-- ================================================
-- Graph traversal: Publication-Concept score edges
-- ================================================

-- ======================= Graph traversal: Publication-Concept score edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection (
                           institution_id VARCHAR(255) NOT NULL,
                           publication_id VARCHAR(255) NOT NULL,
                           concept_id     INT UNSIGNED NOT NULL,
                           score          FLOAT NOT NULL,
                           PRIMARY KEY (institution_id, publication_id, concept_id),
                           KEY institution_id (institution_id),
                           KEY publication_id (publication_id),
                           KEY concept_id (concept_id));

-- ============= Graph traversal: Publication-Concept score edges (INSERT)
    REPLACE INTO [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection
          SELECT a2c.institution_id AS institution_id,
                 a2c.object_id      AS publication_id,
                 a2c.concept_id     AS concept_id,
                 a2c.score          AS score
            FROM [[airflow]].Operations_N_Object_T_FieldsChanged tp
      INNER JOIN [[registry]].Edges_N_Object_N_Concept_T_ConceptDetection a2c
           USING (institution_id, object_type, object_id)
           WHERE a2c.object_type = 'Publication'
             AND a2c.text_source = 'abstract'
             AND a2c.score >= 0.1
             AND tp.to_process = 1;

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
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id) = ('Person', person_id, 'Unit', unit_id)
              -- Link to: Person-Publication authorship edges
      INNER JOIN [[graph_cache]].Traversal_N_Person_N_Publication_T_Authorship p2a
              ON u2p.institution_id = p2a.institution_id AND u2p.person_id = p2a.person_id
              -- Link to: Publication-Concept detection scores
      INNER JOIN [[graph_cache]].Traversal_N_Publication_N_Concept_T_ConceptDetection a2c
              ON p2a.institution_id = a2c.institution_id AND p2a.publication_id = a2c.publication_id
             AND tp.to_process = 1;

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
              ON (tp.from_object_type, tp.from_object_id, tp.to_object_type, tp.to_object_id) = ('Publication', t2.publication_id, 'Person', t2.person_id)
              -- Link to: MOOC-Person affiliation edges
      INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent t1 
              ON (t1.from_institution_id, t1.from_object_type, t1.to_institution_id, t1.to_object_type, t1.to_object_id, t1.context)
               = ('EPFL', 'MOOC', 'EPFL', 'Person', t2.person_id, 'teacher')
             AND tp.to_process = 1;


