
CREATE TABLE IF NOT EXISTS Data_N_Object_N_Object_T_CustomFields (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') COLLATE utf8mb4_unicode_ci NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci NOT NULL,
  from_object_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') COLLATE utf8mb4_unicode_ci NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci NOT NULL,
  to_object_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  context enum('accreditation','affiliation','authorship','coursebook','teacher','founder') COLLATE utf8mb4_unicode_ci NOT NULL,
  field_language enum('en','fr','de','it','n/a') COLLATE utf8mb4_unicode_ci NOT NULL,
  field_name varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  field_value text COLLATE utf8mb4_unicode_ci NOT NULL,
  record_created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  record_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY unique_key (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id,field_language,field_name,context),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY edge_key (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_CustomFields (
  institution_id varchar(6) COLLATE utf8mb4_unicode_ci NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci NOT NULL,
  object_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  field_language enum('en','fr','de','it','n/a') COLLATE utf8mb4_unicode_ci NOT NULL,
  field_name varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  field_value text COLLATE utf8mb4_unicode_ci NOT NULL,
  record_created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  record_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (institution_id,object_type,object_id,field_language,field_name),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_key (institution_id,object_type,object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_Embeddings (
  institution_id varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'Ont',
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'Concept',
  object_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  embedding text COLLATE utf8mb4_unicode_ci,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY object_key (institution_id,object_type,object_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_PageProfile (
  institution_id varchar(6) COLLATE utf8mb4_unicode_ci NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci NOT NULL,
  object_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  numeric_id_en int unsigned DEFAULT NULL,
  numeric_id_fr int unsigned DEFAULT NULL,
  numeric_id_de int unsigned DEFAULT NULL,
  numeric_id_it int unsigned DEFAULT NULL,
  short_code varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  subtype_en varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  subtype_fr varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  subtype_de varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  subtype_it varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  name_en_is_auto_generated tinyint DEFAULT NULL,
  name_en_is_auto_corrected tinyint DEFAULT NULL,
  name_en_is_auto_translated tinyint DEFAULT NULL,
  name_en_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  name_en_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  name_fr_is_auto_generated tinyint DEFAULT NULL,
  name_fr_is_auto_corrected tinyint DEFAULT NULL,
  name_fr_is_auto_translated tinyint DEFAULT NULL,
  name_fr_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  name_fr_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  name_de_is_auto_generated tinyint DEFAULT NULL,
  name_de_is_auto_corrected tinyint DEFAULT NULL,
  name_de_is_auto_translated tinyint DEFAULT NULL,
  name_de_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  name_de_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  name_it_is_auto_generated tinyint DEFAULT NULL,
  name_it_is_auto_corrected tinyint DEFAULT NULL,
  name_it_is_auto_translated tinyint DEFAULT NULL,
  name_it_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  name_it_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_short_en_is_auto_generated tinyint DEFAULT NULL,
  description_short_en_is_auto_corrected tinyint DEFAULT NULL,
  description_short_en_is_auto_translated tinyint DEFAULT NULL,
  description_short_en_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_short_en_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_short_fr_is_auto_generated tinyint DEFAULT NULL,
  description_short_fr_is_auto_corrected tinyint DEFAULT NULL,
  description_short_fr_is_auto_translated tinyint DEFAULT NULL,
  description_short_fr_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_short_fr_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_short_de_is_auto_generated tinyint DEFAULT NULL,
  description_short_de_is_auto_corrected tinyint DEFAULT NULL,
  description_short_de_is_auto_translated tinyint DEFAULT NULL,
  description_short_de_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_short_de_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_short_it_is_auto_generated tinyint DEFAULT NULL,
  description_short_it_is_auto_corrected tinyint DEFAULT NULL,
  description_short_it_is_auto_translated tinyint DEFAULT NULL,
  description_short_it_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_short_it_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_medium_en_is_auto_generated tinyint DEFAULT NULL,
  description_medium_en_is_auto_corrected tinyint DEFAULT NULL,
  description_medium_en_is_auto_translated tinyint DEFAULT NULL,
  description_medium_en_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_medium_en_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_medium_fr_is_auto_generated tinyint DEFAULT NULL,
  description_medium_fr_is_auto_corrected tinyint DEFAULT NULL,
  description_medium_fr_is_auto_translated tinyint DEFAULT NULL,
  description_medium_fr_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_medium_fr_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_medium_de_is_auto_generated tinyint DEFAULT NULL,
  description_medium_de_is_auto_corrected tinyint DEFAULT NULL,
  description_medium_de_is_auto_translated tinyint DEFAULT NULL,
  description_medium_de_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_medium_de_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_medium_it_is_auto_generated tinyint DEFAULT NULL,
  description_medium_it_is_auto_corrected tinyint DEFAULT NULL,
  description_medium_it_is_auto_translated tinyint DEFAULT NULL,
  description_medium_it_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_medium_it_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_long_en_is_auto_generated tinyint DEFAULT NULL,
  description_long_en_is_auto_corrected tinyint DEFAULT NULL,
  description_long_en_is_auto_translated tinyint DEFAULT NULL,
  description_long_en_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_long_en_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_long_fr_is_auto_generated tinyint DEFAULT NULL,
  description_long_fr_is_auto_corrected tinyint DEFAULT NULL,
  description_long_fr_is_auto_translated tinyint DEFAULT NULL,
  description_long_fr_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_long_fr_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_long_de_is_auto_generated tinyint DEFAULT NULL,
  description_long_de_is_auto_corrected tinyint DEFAULT NULL,
  description_long_de_is_auto_translated tinyint DEFAULT NULL,
  description_long_de_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_long_de_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  description_long_it_is_auto_generated tinyint DEFAULT NULL,
  description_long_it_is_auto_corrected tinyint DEFAULT NULL,
  description_long_it_is_auto_translated tinyint DEFAULT NULL,
  description_long_it_translated_from char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description_long_it_value mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  external_key_en varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  external_key_fr varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  external_key_de varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  external_key_it varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  external_url_en text COLLATE utf8mb4_unicode_ci,
  external_url_fr text COLLATE utf8mb4_unicode_ci,
  external_url_de text COLLATE utf8mb4_unicode_ci,
  external_url_it text COLLATE utf8mb4_unicode_ci,
  is_visible tinyint NOT NULL DEFAULT '0',
  record_created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  record_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (institution_id,object_type,object_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY object_key (institution_id,object_type,object_id),
  KEY subtype_en (subtype_en),
  KEY subtype_fr (subtype_fr),
  KEY subtype_de (subtype_de),
  KEY subtype_it (subtype_it),
  KEY short_code (short_code),
  KEY numeric_id_fr (numeric_id_fr),
  KEY numeric_id_de (numeric_id_de),
  KEY numeric_id_it (numeric_id_it),
  KEY numeric_id_en (numeric_id_en),
  KEY is_visible (is_visible)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Category_N_Category_T_ChildToParent (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Category_N_Concept_T_AnchorPage (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Category_N_ConceptsCluster_T_ParentToChild (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Concept_N_Concept_T_Directed (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Concept_N_Concept_T_Embeddings (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  score float DEFAULT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Concept_N_Concept_T_Symmetric (
  id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  score float DEFAULT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Concept_N_Concept_T_Undirected (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  score float NOT NULL,
  normalised_score float DEFAULT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_ConceptsCluster_N_Concept_T_ParentToChild (
  from_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  to_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  UNIQUE KEY unique_key (from_id,to_id),
  KEY from_id (from_id),
  KEY to_id (to_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Nodes_N_Category (
  institution_id varchar(6) COLLATE utf8mb4_unicode_ci NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci NOT NULL,
  object_id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  name varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  depth int unsigned NOT NULL,
  reference_page_id int unsigned NOT NULL,
  reference_page_key varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  reference_page_url varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY row_id (row_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY id (id),
  KEY name (name),
  KEY depth (depth),
  KEY anchor_page_id (reference_page_id),
  KEY anchor_page_key (reference_page_key)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Nodes_N_Concept (
  institution_id varchar(6) COLLATE utf8mb4_unicode_ci DEFAULT 'Ont',
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') COLLATE utf8mb4_unicode_ci DEFAULT 'Concept',
  object_id varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  id varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  name varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  is_ontology_category tinyint(1) NOT NULL,
  is_ontology_concept tinyint(1) NOT NULL,
  is_ontology_neighbour tinyint(1) NOT NULL,
  is_noise tinyint(1) NOT NULL DEFAULT '0',
  is_unused tinyint(1) NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY id (id),
  KEY name (name),
  KEY is_ontology_category (is_ontology_category),
  KEY is_ontology_concept (is_ontology_concept),
  KEY is_ontology_neighbour (is_ontology_neighbour),
  KEY is_noise (is_noise),
  KEY is_unused (is_unused)  
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Category_N_OAlexTopic_T_Semantic (
  category_id varchar(255) NOT NULL,
  category_name varchar(255) NOT NULL,
  topic_id varchar(255) NOT NULL,
  topic_name varchar(255) NOT NULL,
  embedding_score float NOT NULL,
  wikipedia_score float NOT NULL,
  score float NOT NULL,
  row_id int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (category_id,topic_id),
  UNIQUE KEY row_id (row_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE OR REPLACE VIEW Edges_N_Object_N_Object_T_ChildToParent AS
    SELECT 'Ont' AS from_institution_id, 'Concept'  AS from_object_type,   t1.to_id AS from_object_id,
           'Ont' AS   to_institution_id, 'Category' AS   to_object_type, t2.from_id AS   to_object_id,
           'ontology tree' AS context, t1.row_id
      FROM Edges_N_ConceptsCluster_N_Concept_T_ParentToChild t1
INNER JOIN Edges_N_Category_N_ConceptsCluster_T_ParentToChild t2
        ON t1.from_id = t2.to_id
 UNION ALL
    SELECT 'Ont' AS from_institution_id, 'Category' AS from_object_type, from_id AS from_object_id,
           'Ont' AS   to_institution_id, 'Category' AS   to_object_type,   to_id AS   to_object_id,
           'ontology tree' AS context, row_id
      FROM Edges_N_Category_N_Category_T_ChildToParent;

CREATE OR REPLACE VIEW Nodes_N_Object AS
   SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, row_id
     FROM Nodes_N_Concept
UNION ALL  
   SELECT institution_id, object_type, object_id, name AS object_title, NULL AS text_source, NULL AS raw_text, row_id
     FROM Nodes_N_Category;