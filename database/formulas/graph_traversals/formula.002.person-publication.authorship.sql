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
