CREATE TABLE IF NOT EXISTS Data_N_Object_N_Object_T_AllFieldsSymmetric (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value text NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id,context,field_language,field_name),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_N_Object_T_CalculatedFields (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value text NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_AllFields (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value longtext NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (object_type,object_id,field_language,field_name),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_type_and_id (object_type,object_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_CalculatedFields (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value text NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_type_and_id (object_type,object_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_PageProfile (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  numeric_id_en int(10) unsigned DEFAULT NULL,
  numeric_id_fr int(10) unsigned DEFAULT NULL,
  numeric_id_de int(10) unsigned DEFAULT NULL,
  numeric_id_it int(10) unsigned DEFAULT NULL,
  short_code varchar(255) DEFAULT NULL,
  subtype_en varchar(255) DEFAULT NULL,
  subtype_fr varchar(255) DEFAULT NULL,
  subtype_de varchar(255) DEFAULT NULL,
  subtype_it varchar(255) DEFAULT NULL,
  name_en_is_auto_generated tinyint(4) DEFAULT NULL,
  name_en_is_auto_corrected tinyint(4) DEFAULT NULL,
  name_en_is_auto_translated tinyint(4) DEFAULT NULL,
  name_en_translated_from char(6) DEFAULT NULL,
  name_en_value mediumtext DEFAULT NULL,
  name_fr_is_auto_generated tinyint(4) DEFAULT NULL,
  name_fr_is_auto_corrected tinyint(4) DEFAULT NULL,
  name_fr_is_auto_translated tinyint(4) DEFAULT NULL,
  name_fr_translated_from char(6) DEFAULT NULL,
  name_fr_value mediumtext DEFAULT NULL,
  name_de_is_auto_generated tinyint(4) DEFAULT NULL,
  name_de_is_auto_corrected tinyint(4) DEFAULT NULL,
  name_de_is_auto_translated tinyint(4) DEFAULT NULL,
  name_de_translated_from char(6) DEFAULT NULL,
  name_de_value mediumtext DEFAULT NULL,
  name_it_is_auto_generated tinyint(4) DEFAULT NULL,
  name_it_is_auto_corrected tinyint(4) DEFAULT NULL,
  name_it_is_auto_translated tinyint(4) DEFAULT NULL,
  name_it_translated_from char(6) DEFAULT NULL,
  name_it_value mediumtext DEFAULT NULL,
  description_short_en_is_auto_generated tinyint(4) DEFAULT NULL,
  description_short_en_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_short_en_is_auto_translated tinyint(4) DEFAULT NULL,
  description_short_en_translated_from char(6) DEFAULT NULL,
  description_short_en_value mediumtext DEFAULT NULL,
  description_short_fr_is_auto_generated tinyint(4) DEFAULT NULL,
  description_short_fr_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_short_fr_is_auto_translated tinyint(4) DEFAULT NULL,
  description_short_fr_translated_from char(6) DEFAULT NULL,
  description_short_fr_value mediumtext DEFAULT NULL,
  description_short_de_is_auto_generated tinyint(4) DEFAULT NULL,
  description_short_de_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_short_de_is_auto_translated tinyint(4) DEFAULT NULL,
  description_short_de_translated_from char(6) DEFAULT NULL,
  description_short_de_value mediumtext DEFAULT NULL,
  description_short_it_is_auto_generated tinyint(4) DEFAULT NULL,
  description_short_it_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_short_it_is_auto_translated tinyint(4) DEFAULT NULL,
  description_short_it_translated_from char(6) DEFAULT NULL,
  description_short_it_value mediumtext DEFAULT NULL,
  description_medium_en_is_auto_generated tinyint(4) DEFAULT NULL,
  description_medium_en_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_medium_en_is_auto_translated tinyint(4) DEFAULT NULL,
  description_medium_en_translated_from char(6) DEFAULT NULL,
  description_medium_en_value mediumtext DEFAULT NULL,
  description_medium_fr_is_auto_generated tinyint(4) DEFAULT NULL,
  description_medium_fr_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_medium_fr_is_auto_translated tinyint(4) DEFAULT NULL,
  description_medium_fr_translated_from char(6) DEFAULT NULL,
  description_medium_fr_value mediumtext DEFAULT NULL,
  description_medium_de_is_auto_generated tinyint(4) DEFAULT NULL,
  description_medium_de_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_medium_de_is_auto_translated tinyint(4) DEFAULT NULL,
  description_medium_de_translated_from char(6) DEFAULT NULL,
  description_medium_de_value mediumtext DEFAULT NULL,
  description_medium_it_is_auto_generated tinyint(4) DEFAULT NULL,
  description_medium_it_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_medium_it_is_auto_translated tinyint(4) DEFAULT NULL,
  description_medium_it_translated_from char(6) DEFAULT NULL,
  description_medium_it_value mediumtext DEFAULT NULL,
  description_long_en_is_auto_generated tinyint(4) DEFAULT NULL,
  description_long_en_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_long_en_is_auto_translated tinyint(4) DEFAULT NULL,
  description_long_en_translated_from char(6) DEFAULT NULL,
  description_long_en_value mediumtext DEFAULT NULL,
  description_long_fr_is_auto_generated tinyint(4) DEFAULT NULL,
  description_long_fr_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_long_fr_is_auto_translated tinyint(4) DEFAULT NULL,
  description_long_fr_translated_from char(6) DEFAULT NULL,
  description_long_fr_value mediumtext DEFAULT NULL,
  description_long_de_is_auto_generated tinyint(4) DEFAULT NULL,
  description_long_de_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_long_de_is_auto_translated tinyint(4) DEFAULT NULL,
  description_long_de_translated_from char(6) DEFAULT NULL,
  description_long_de_value mediumtext DEFAULT NULL,
  description_long_it_is_auto_generated tinyint(4) DEFAULT NULL,
  description_long_it_is_auto_corrected tinyint(4) DEFAULT NULL,
  description_long_it_is_auto_translated tinyint(4) DEFAULT NULL,
  description_long_it_translated_from char(6) DEFAULT NULL,
  description_long_it_value mediumtext DEFAULT NULL,
  external_key_en varchar(255) DEFAULT NULL,
  external_key_fr varchar(255) DEFAULT NULL,
  external_key_de varchar(255) DEFAULT NULL,
  external_key_it varchar(255) DEFAULT NULL,
  external_url_en text DEFAULT NULL,
  external_url_fr text DEFAULT NULL,
  external_url_de text DEFAULT NULL,
  external_url_it text DEFAULT NULL,
  is_visible tinyint(4) NOT NULL DEFAULT 1,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY is_visible (is_visible),
  KEY has_expired (to_process),
  KEY deleted (deleted),
  KEY object_type_process_id (object_type,to_process,object_id),
  KEY object_type_id_process (object_type,object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_CalculatedScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  category_id varchar(255) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  UNIQUE KEY unique_key (object_type,object_id,category_id,calculation_type) USING HASH,
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY category_id (category_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process),
  KEY object_type_and_id (object_type,object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_CalculatedScores_AVG (
  object_type varchar(32) NOT NULL,
  avg_score float NOT NULL,
  PRIMARY KEY (object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_FinalScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  category_id varchar(255) NOT NULL,
  score float DEFAULT NULL,
  to_process tinyint(4) DEFAULT 0,
  UNIQUE KEY unique_key (object_type,object_id,category_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY category_id (category_id),
  KEY to_process (to_process),
  KEY idx_concept_type_proc (category_id,object_type,to_process),
  KEY idx_concept_type_proc_score (category_id,object_type,to_process,score),
  KEY idx_proc_type_score_category (to_process,object_type,score,category_id),
  KEY object_type_and_id (object_type,object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_CalculatedScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(10) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  UNIQUE KEY unique_key (object_type,object_id,concept_id,calculation_type),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process),
  KEY object_type_and_id (object_type,object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_FinalScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(10) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id,object_type),
  UNIQUE KEY unique_key (object_type,object_id,concept_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY idx_concept_type_proc (concept_id,object_type,to_process),
  KEY idx_concept_type_proc_score (concept_id,object_type,to_process,score),
  KEY idx_proc_type_score_concept (to_process,object_type,score,concept_id),
  KEY object_type_and_id (object_type,object_id),
  KEY object_type_process_concept (object_type,to_process,concept_id),
  KEY object_type_concept_process (object_type,concept_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
 PARTITION BY LIST  COLUMNS(object_type)
(PARTITION p_category VALUES IN ('Category') ENGINE = InnoDB,
 PARTITION p_course VALUES IN ('Course') ENGINE = InnoDB,
 PARTITION p_exercise VALUES IN ('Exercise') ENGINE = InnoDB,
 PARTITION p_lecture VALUES IN ('Lecture') ENGINE = InnoDB,
 PARTITION p_mooc VALUES IN ('MOOC') ENGINE = InnoDB,
 PARTITION p_notebook VALUES IN ('Notebook') ENGINE = InnoDB,
 PARTITION p_person VALUES IN ('Person') ENGINE = InnoDB,
 PARTITION p_publication VALUES IN ('Publication') ENGINE = InnoDB,
 PARTITION p_startup VALUES IN ('Startup') ENGINE = InnoDB,
 PARTITION p_unit VALUES IN ('Unit') ENGINE = InnoDB,
 PARTITION p_widget VALUES IN ('Widget') ENGINE = InnoDB,
 PARTITION p_default DEFAULT ENGINE = InnoDB);

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_ScoringMatrix (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL DEFAULT '',
  concept_id varchar(10) NOT NULL,
  score_1 float DEFAULT NULL,
  score_2 float DEFAULT NULL,
  score_3 float DEFAULT 0,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (object_type,object_id,concept_id),
  KEY join_id (object_id,concept_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY object_type_and_id (object_type,object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_UnionAllScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(10) NOT NULL,
  calculation_type varchar(64) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (object_type,object_id,concept_id,calculation_type),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process),
  KEY object_type_and_id (object_type,object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_CuratedArea_T_CalculatedScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  curated_area_id varchar(255) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  UNIQUE KEY object_type_and_id (object_type,object_id),
  UNIQUE KEY unique_key (object_type,object_id,curated_area_id,calculation_type) USING HASH,
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY curated_area_id (curated_area_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_CuratedArea_T_FinalScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  curated_area_id varchar(255) NOT NULL,
  score float DEFAULT NULL,
  to_process tinyint(4) DEFAULT 0,
  UNIQUE KEY object_type_and_id (object_type,object_id),
  UNIQUE KEY unique_key (object_type,object_id,curated_area_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY curated_area_id (curated_area_id),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_DegreeCombinations (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL DEFAULT '',
  to_object_type varchar(32) NOT NULL,
  degree bigint(20) NOT NULL DEFAULT 0,
  log_degree double DEFAULT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_MaxLogDegrees (
  from_object_type varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  to_object_type varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  max_log_degree double DEFAULT NULL,
  PRIMARY KEY (from_object_type,to_object_type),
  KEY from_object_type (from_object_type),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_NormLogDegrees (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL DEFAULT '',
  to_object_type varchar(32) NOT NULL,
  degree bigint(20) NOT NULL DEFAULT 0,
  log_degree double DEFAULT NULL,
  norm_log_degree double DEFAULT NULL,
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ParentChildSymmetric (
  edge_type varchar(16) NOT NULL,
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id,context),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY context (context),
  KEY to_process (to_process),
  KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY object_type_process_id (from_object_type,to_object_type,to_process,from_object_id,to_object_id),
  KEY object_type_id_process (from_object_type,to_object_type,from_object_id,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_AVG (
  from_object_type varchar(32) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  avg_score float NOT NULL,
  n_rows int(10) unsigned DEFAULT NULL,
  KEY from_object_type (from_object_type),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS (
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC (
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id,from_object_type,to_object_type),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
 PARTITION BY LIST  COLUMNS(from_object_type,to_object_type)
(PARTITION p_category_category VALUES IN (('Category','Category')) ENGINE = InnoDB,
 PARTITION p_category_concept VALUES IN (('Category','Concept')) ENGINE = InnoDB,
 PARTITION p_category_course VALUES IN (('Category','Course')) ENGINE = InnoDB,
 PARTITION p_category_exercise VALUES IN (('Category','Exercise')) ENGINE = InnoDB,
 PARTITION p_category_lecture VALUES IN (('Category','Lecture')) ENGINE = InnoDB,
 PARTITION p_category_mooc VALUES IN (('Category','MOOC')) ENGINE = InnoDB,
 PARTITION p_category_notebook VALUES IN (('Category','Notebook')) ENGINE = InnoDB,
 PARTITION p_category_person VALUES IN (('Category','Person')) ENGINE = InnoDB,
 PARTITION p_category_publication VALUES IN (('Category','Publication')) ENGINE = InnoDB,
 PARTITION p_category_specialisation VALUES IN (('Category','Specialisation')) ENGINE = InnoDB,
 PARTITION p_category_startup VALUES IN (('Category','Startup')) ENGINE = InnoDB,
 PARTITION p_category_studyplan VALUES IN (('Category','StudyPlan')) ENGINE = InnoDB,
 PARTITION p_category_unit VALUES IN (('Category','Unit')) ENGINE = InnoDB,
 PARTITION p_category_widget VALUES IN (('Category','Widget')) ENGINE = InnoDB,
 PARTITION p_concept_category VALUES IN (('Concept','Category')) ENGINE = InnoDB,
 PARTITION p_concept_concept VALUES IN (('Concept','Concept')) ENGINE = InnoDB,
 PARTITION p_concept_course VALUES IN (('Concept','Course')) ENGINE = InnoDB,
 PARTITION p_concept_exercise VALUES IN (('Concept','Exercise')) ENGINE = InnoDB,
 PARTITION p_concept_lecture VALUES IN (('Concept','Lecture')) ENGINE = InnoDB,
 PARTITION p_concept_mooc VALUES IN (('Concept','MOOC')) ENGINE = InnoDB,
 PARTITION p_concept_notebook VALUES IN (('Concept','Notebook')) ENGINE = InnoDB,
 PARTITION p_concept_person VALUES IN (('Concept','Person')) ENGINE = InnoDB,
 PARTITION p_concept_publication VALUES IN (('Concept','Publication')) ENGINE = InnoDB,
 PARTITION p_concept_specialisation VALUES IN (('Concept','Specialisation')) ENGINE = InnoDB,
 PARTITION p_concept_startup VALUES IN (('Concept','Startup')) ENGINE = InnoDB,
 PARTITION p_concept_studyplan VALUES IN (('Concept','StudyPlan')) ENGINE = InnoDB,
 PARTITION p_concept_unit VALUES IN (('Concept','Unit')) ENGINE = InnoDB,
 PARTITION p_concept_widget VALUES IN (('Concept','Widget')) ENGINE = InnoDB,
 PARTITION p_course_category VALUES IN (('Course','Category')) ENGINE = InnoDB,
 PARTITION p_exercise_category VALUES IN (('Exercise','Category')) ENGINE = InnoDB,
 PARTITION p_lecture_category VALUES IN (('Lecture','Category')) ENGINE = InnoDB,
 PARTITION p_mooc_category VALUES IN (('MOOC','Category')) ENGINE = InnoDB,
 PARTITION p_notebook_category VALUES IN (('Notebook','Category')) ENGINE = InnoDB,
 PARTITION p_person_category VALUES IN (('Person','Category')) ENGINE = InnoDB,
 PARTITION p_publication_category VALUES IN (('Publication','Category')) ENGINE = InnoDB,
 PARTITION p_specialisation_category VALUES IN (('Specialisation','Category')) ENGINE = InnoDB,
 PARTITION p_startup_category VALUES IN (('Startup','Category')) ENGINE = InnoDB,
 PARTITION p_studyplan_category VALUES IN (('StudyPlan','Category')) ENGINE = InnoDB,
 PARTITION p_unit_category VALUES IN (('Unit','Category')) ENGINE = InnoDB,
 PARTITION p_widget_category VALUES IN (('Widget','Category')) ENGINE = InnoDB,
 PARTITION p_course_concept VALUES IN (('Course','Concept')) ENGINE = InnoDB,
 PARTITION p_exercise_concept VALUES IN (('Exercise','Concept')) ENGINE = InnoDB,
 PARTITION p_lecture_concept VALUES IN (('Lecture','Concept')) ENGINE = InnoDB,
 PARTITION p_mooc_concept VALUES IN (('MOOC','Concept')) ENGINE = InnoDB,
 PARTITION p_notebook_concept VALUES IN (('Notebook','Concept')) ENGINE = InnoDB,
 PARTITION p_person_concept VALUES IN (('Person','Concept')) ENGINE = InnoDB,
 PARTITION p_publication_concept VALUES IN (('Publication','Concept')) ENGINE = InnoDB,
 PARTITION p_specialisation_concept VALUES IN (('Specialisation','Concept')) ENGINE = InnoDB,
 PARTITION p_startup_concept VALUES IN (('Startup','Concept')) ENGINE = InnoDB,
 PARTITION p_studyplan_concept VALUES IN (('StudyPlan','Concept')) ENGINE = InnoDB,
 PARTITION p_unit_concept VALUES IN (('Unit','Concept')) ENGINE = InnoDB,
 PARTITION p_widget_concept VALUES IN (('Widget','Concept')) ENGINE = InnoDB,
 PARTITION p_default DEFAULT ENGINE = InnoDB);

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC (
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS (
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC (
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Nodes_N_Object_T_DegreeScores (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL DEFAULT '',
  avg_degree float NOT NULL,
  avg_log_degree float NOT NULL,
  avg_norm_log_degree float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_Checksums (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id,context),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_ChecksumsCustomFields (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id,context),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_ChecksumsObject (
  from_object_type varchar(32) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(32) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(32) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY object_type_and_id (from_object_type,from_object_id,to_object_type,to_object_id),
  UNIQUE KEY unique_key (from_object_type,from_object_id,to_object_type,to_object_id,context),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_Checksums (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsCustomFields (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsObject (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsPageProfile (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  UNIQUE KEY object_type_and_id (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_LargestConnectedGraph (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY uid (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_NoLooseEnds (
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY uid (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
