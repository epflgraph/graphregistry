-- ========================================================
-- Graph traversal: Category-Cluster-Concept ontology edges
-- ========================================================

-- ========== Graph traversal: Category-Cluster-Concept ontology edges (CREATE TABLE)
 CREATE TABLE
IF NOT EXISTS [[traversals]].Category_Cluster_Concept__FullOntology (
              root_id  VARCHAR(255),
              root_name       VARCHAR(255),
              category_1_id   VARCHAR(255),
              category_1_name VARCHAR(255),
              category_2_id   VARCHAR(255),
              category_2_name VARCHAR(255),
              category_3_id   VARCHAR(255),
              category_3_name VARCHAR(255),
              category_4_id   VARCHAR(255),
              category_4_name VARCHAR(255),
              cluster_id      VARCHAR(255),
              concept_id      VARCHAR(255),
              concept_name    VARCHAR(255),
              to_process      TINYINT(1) UNSIGNED DEFAULT 0,
              deleted         TINYINT(1) UNSIGNED DEFAULT 0,
              row_id          BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
              PRIMARY KEY (row_id),
              UNIQUE KEY unique_key (root_id, category_1_id, category_2_id, category_3_id, category_4_id, cluster_id, concept_id) USING HASH,
              KEY root_id         (root_id),
              KEY root_name       (root_name),
              KEY category_1_id   (category_1_id),
              KEY category_1_name (category_1_name),
              KEY category_2_id   (category_2_id),
              KEY category_2_name (category_2_name),
              KEY category_3_id   (category_3_id),
              KEY category_3_name (category_3_name),
              KEY category_4_id   (category_4_id),
              KEY category_4_name (category_4_name),
              KEY cluster_id      (cluster_id),
              KEY concept_id      (concept_id),
              KEY concept_name    (concept_name),
              KEY to_process      (to_process),
              KEY deleted         (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Category_Cluster_Concept__FullOntology
            SET to_process = 0
          WHERE to_process = 1;

-- ============ Soft-delete filtering for source registry rows

-- ============ Graph traversal: Category-Cluster-Concept ontology edges (REPLACE)
   REPLACE INTO [[traversals]].Category_Cluster_Concept__FullOntology
                 (root_id, root_name, category_1_id, category_1_name, category_2_id, category_2_name, category_3_id, category_3_name, category_4_id, category_4_name, cluster_id, concept_id, concept_name, to_process, deleted)
SELECT DISTINCT n5.id   AS       root_id, n5.name AS       root_name,
                n4.id   AS category_1_id, n4.name AS category_1_name,
                n3.id   AS category_2_id, n3.name AS category_2_name,
                n2.id   AS category_3_id, n2.name AS category_3_name,
                n1.id   AS category_4_id, n1.name AS category_4_name,
                a.to_id AS    cluster_id,
                d.id    AS    concept_id,  d.name AS    concept_name,
                fc.to_process AS to_process,
                0 AS deleted
           FROM [[ontology]].Edges_N_Category_N_ConceptsCluster_T_ParentToChild a
     INNER JOIN [[ontology]].Edges_N_ConceptsCluster_N_Concept_T_ParentToChild b ON (a.to_id = b.from_id)
     INNER JOIN [[ontology]].Nodes_N_Concept d ON (b.to_id = d.id)
     INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t1 ON ( a.from_id = t1.from_id)
     INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t2 ON (t2.from_id = t1.to_id)
     INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t3 ON (t3.from_id = t2.to_id)
     INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t4 ON (t4.from_id = t3.to_id)
     INNER JOIN [[ontology]].Edges_N_Category_N_Category_T_ChildToParent t5 ON (t5.from_id = t4.to_id)
     INNER JOIN [[ontology]].Nodes_N_Category n1 ON (t1.from_id = n1.id)
     INNER JOIN [[ontology]].Nodes_N_Category n2 ON (t2.from_id = n2.id)
     INNER JOIN [[ontology]].Nodes_N_Category n3 ON (t3.from_id = n3.id)
     INNER JOIN [[ontology]].Nodes_N_Category n4 ON (t4.from_id = n4.id)
     INNER JOIN [[ontology]].Nodes_N_Category n5 ON (t5.from_id = n5.id)
     INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged fc
             ON d.id = fc.object_id
            AND fc.object_type = 'Concept'
            AND fc.to_process = 1
            AND fc.deleted = 0;
