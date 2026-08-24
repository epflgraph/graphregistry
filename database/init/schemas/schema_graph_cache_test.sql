CREATE TABLE IF NOT EXISTS Data_N_Object_N_Object_T_AllFieldsSymmetric (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value text NOT NULL,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY uid (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id,field_language,field_name),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY field_language (field_language),
  KEY field_name (field_name)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_N_Object_T_CalculatedFields (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(255) NOT NULL,
  field_value text NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY edge_key (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  KEY uid (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id,field_language,field_name)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_AllFields (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(64) NOT NULL,
  field_value longtext NOT NULL,
  row_id int(11) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,field_language,field_name),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY field_language (field_language),
  KEY field_name (field_name)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_CalculatedFields (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  field_language enum('en','fr','de','it','n/a') NOT NULL,
  field_name varchar(255) NOT NULL,
  field_value text NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,field_language,field_name),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY field_language (field_language),
  KEY field_name (field_name),
  KEY object_key (institution_id,object_type,object_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Data_N_Object_T_PageProfile (
  institution_id varchar(8) NOT NULL,
  object_type varchar(16) NOT NULL,
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
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (institution_id,object_type,object_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY uid (institution_id,object_type,object_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY is_visible (is_visible),
  KEY has_expired (to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Course_N_Concept_T_SlideSumScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  sum_score double DEFAULT NULL,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Course_N_Lecture_T_ParentToChild (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  academic_year varchar(45) DEFAULT NULL,
  channel_type enum('category','playlist') DEFAULT NULL,
  old_sort_number int(10) unsigned DEFAULT NULL,
  sort_number bigint(20) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  UNIQUE KEY uid (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Lecture_N_Concept_T_SlideSumScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  sum_score double DEFAULT NULL,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Lecture_N_Concept_T_Timestamps (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  detection_score float DEFAULT NULL,
  detection_time_hms time(3) NOT NULL,
  detection_timestamp smallint(5) unsigned NOT NULL,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id,detection_time_hms),
  UNIQUE KEY uid (institution_id,object_type,object_id,concept_id,detection_time_hms),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY detection_time_hms (detection_time_hms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Lecture_N_Video_T_ParentToChild_Enriched (
  lecture_id varchar(255) NOT NULL,
  video_id varchar(255) NOT NULL,
  n_slides int(10) unsigned DEFAULT NULL,
  is_restricted tinyint(4) DEFAULT NULL,
  subtype enum('classroom','slides') DEFAULT NULL,
  PRIMARY KEY (lecture_id,video_id),
  KEY lecture_id (lecture_id),
  KEY video_id (video_id),
  KEY is_restricted (is_restricted),
  KEY subtype (subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_MOOC_N_Concept_T_SlideSumScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  sum_score double DEFAULT NULL,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_MOOC_N_Lecture_T_ParentToChild (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  sort_number int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  UNIQUE KEY uid (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_CalculatedScores_AVG (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  avg_score float NOT NULL,
  PRIMARY KEY (institution_id,object_type),
  KEY institution_id (institution_id),
  KEY object_type (object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_CalculatedScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  category_id varchar(255) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,category_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY category_id (category_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Category_T_FinalScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  category_id varchar(255) NOT NULL,
  score float DEFAULT NULL,
  to_process tinyint(4) DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,category_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY category_id (category_id),
  KEY to_process (to_process),
  KEY idx_concept_type_proc (category_id,object_type,to_process),
  KEY idx_concept_type_proc_score (category_id,object_type,to_process,score),
  KEY idx_proc_type_score_category (to_process,object_type,score,category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_CalculatedScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id),
  UNIQUE KEY uid (institution_id,object_type,object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_FinalScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY unique_key (institution_id,object_type,object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY idx_concept_type_proc (concept_id,object_type,to_process),
  KEY idx_concept_type_proc_score (concept_id,object_type,to_process,score),
  KEY idx_proc_type_score_concept (to_process,object_type,score,concept_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_ScoringMatrix (
  institution_id varchar(6) NOT NULL DEFAULT '',
  object_type varchar(17) NOT NULL DEFAULT '',
  object_id varchar(255) NOT NULL DEFAULT '',
  concept_id bigint(20) NOT NULL DEFAULT 0,
  score_1 float DEFAULT NULL,
  score_2 float DEFAULT NULL,
  score_3 float DEFAULT 0,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY institution_id_2 (institution_id,object_type,object_id,concept_id),
  KEY join_id (object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_Tuples (
  institution_id varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  object_type varchar(17) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  object_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  concept_id bigint(20) NOT NULL DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,concept_id),
  KEY join_id (object_id,concept_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Concept_T_UnionAllScores (
  institution_id varchar(6) NOT NULL,
  object_type varchar(32) NOT NULL,
  object_id varchar(255) NOT NULL,
  concept_id bigint(20) NOT NULL,
  calculation_type varchar(64) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY obj_cpt_cal_tp_key (institution_id,object_type,object_id,concept_id,calculation_type,to_process),
  UNIQUE KEY obj_cpt_cal_key (institution_id,object_type,object_id,concept_id,calculation_type),
  KEY obj_cpt_key (institution_id,object_type,object_id,concept_id),
  KEY object_key (institution_id,object_type,object_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY concept_id (concept_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_CuratedArea_T_CalculatedScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  curated_area_id varchar(255) NOT NULL,
  calculation_type varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,curated_area_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY curated_area_id (curated_area_id),
  KEY calculation_type (calculation_type),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_CuratedArea_T_FinalScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Specialisation','Startup','Strategic area','StudyPlan','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL,
  curated_area_id varchar(255) NOT NULL,
  score float DEFAULT NULL,
  to_process tinyint(4) DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id,curated_area_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY curated_area_id (curated_area_id),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_DegreeCombinations (
  from_institution_id varchar(5) NOT NULL DEFAULT '',
  from_object_type varchar(17) NOT NULL DEFAULT '',
  from_object_id varchar(255) NOT NULL DEFAULT '',
  to_institution_id varchar(5) NOT NULL DEFAULT '',
  to_object_type varchar(17) NOT NULL DEFAULT '',
  degree bigint(20) NOT NULL DEFAULT 0,
  log_degree double DEFAULT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY unique_key (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_MaxLogDegrees (
  from_institution_id varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  from_object_type varchar(17) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  to_institution_id varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  to_object_type varchar(17) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  max_log_degree double DEFAULT NULL,
  PRIMARY KEY (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_NormLogDegrees (
  from_institution_id varchar(5) NOT NULL DEFAULT '',
  from_object_type varchar(17) NOT NULL DEFAULT '',
  from_object_id varchar(255) NOT NULL DEFAULT '',
  to_institution_id varchar(5) NOT NULL DEFAULT '',
  to_object_type varchar(17) NOT NULL DEFAULT '',
  degree bigint(20) NOT NULL DEFAULT 0,
  log_degree double DEFAULT NULL,
  norm_log_degree double DEFAULT NULL,
  PRIMARY KEY (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ParentChildSymmetric (
  edge_type varchar(16) NOT NULL,
  from_institution_id varchar(8) NOT NULL,
  from_object_type varchar(16) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id varchar(8) NOT NULL,
  to_object_type varchar(16) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  context varchar(64) NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id,context),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY uid (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id,context),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY context (context),
  KEY to_process (to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_AVG (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  avg_score float NOT NULL,
  n_rows int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Edges_N_Object_N_Object_T_ScoresMatrix_Research_GBC (
  from_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  from_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  to_object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  to_object_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY unique_key (from_institution_id,from_object_type,from_object_id,to_institution_id,to_object_type,to_object_id),
  KEY idx_gb_types_proc (from_object_type,to_object_type,to_process),
  KEY idx_gb_types_proc_score (from_object_type,to_object_type,to_process,score),
  KEY from_institution_id (from_institution_id),
  KEY from_object_type (from_object_type),
  KEY from_object_id (from_object_id),
  KEY to_institution_id (to_institution_id),
  KEY to_object_type (to_object_type),
  KEY to_object_id (to_object_id),
  KEY to_process (to_process),
  KEY idx_fi_fot_ti_tot (from_institution_id,from_object_type,to_institution_id,to_object_type),
  KEY idx_fi_fot_foid (from_institution_id,from_object_type,from_object_id),
  KEY idx_fi_fot (from_institution_id,from_object_type),
  KEY idx_ti_tot_toid (to_institution_id,to_object_type,to_object_id),
  KEY idx_ti_tot (to_institution_id,to_object_type),
  KEY idx_fot_foid_tot_toid (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY idx_fi_fot_ti_tot_tp (from_institution_id,from_object_type,to_institution_id,to_object_type,to_process),
  KEY idx_fi_fot_foid_tp (from_institution_id,from_object_type,from_object_id,to_process),
  KEY idx_fi_fot_tp (from_institution_id,from_object_type,to_process),
  KEY idx_ti_tot_toid_tp (to_institution_id,to_object_type,to_object_id,to_process),
  KEY idx_ti_tot_tp (to_institution_id,to_object_type,to_process),
  KEY idx_fot_foid_tot_toid_tp (from_object_type,from_object_id,to_object_type,to_object_id,to_process)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Nodes_N_Object_T_DegreeScores (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  object_type enum('Category','Chart','Concept','Course','Curated area','Dashboard','Exercise','External person','Hardware','Historical figure','Lecture','Learning module','MOOC','News','Notebook','Person','Publication','Slide','Specialisation','Startup','Strategic area','StudyPlan','Transcript','Unit','Widget') NOT NULL,
  object_id varchar(255) NOT NULL DEFAULT '',
  avg_degree float NOT NULL,
  avg_log_degree float NOT NULL,
  avg_norm_log_degree float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (institution_id,object_type,object_id),
  KEY institution_id (institution_id),
  KEY object_type (object_type),
  KEY object_id (object_id),
  KEY to_process (to_process)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_Checksums (
  from_object_type varchar(16) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(16) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_ChecksumsCustomFields (
  from_object_type varchar(16) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(16) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_N_Object_T_ChecksumsObject (
  from_object_type varchar(16) NOT NULL,
  from_object_id varchar(255) NOT NULL,
  to_object_type varchar(16) NOT NULL,
  to_object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (from_object_type,from_object_id,to_object_type,to_object_id),
  KEY object_type (from_object_type),
  KEY object_id (from_object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_Checksums (
  object_type varchar(16) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsCustomFields (
  object_type varchar(16) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsObject (
  object_type varchar(16) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Operations_N_Object_T_ChecksumsPageProfile (
  object_type varchar(16) NOT NULL,
  object_id varchar(255) NOT NULL,
  checksum_val varchar(32) DEFAULT NULL,
  PRIMARY KEY (object_type,object_id),
  KEY object_type (object_type),
  KEY object_id (object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Concept_N_Concept_T_Depth2 (
  from_concept_id varchar(255) NOT NULL,
  to_concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  row_id int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (from_concept_id,to_concept_id),
  UNIQUE KEY row_id (row_id),
  KEY from_concept_id (from_concept_id),
  KEY to_concept_id (to_concept_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_MOOC_N_Person_N_Publication_N_Concept_T_ConceptDet (
  institution_id varchar(6) NOT NULL,
  mooc_id varchar(32) NOT NULL,
  person_id int(10) unsigned NOT NULL,
  publication_id varchar(128) NOT NULL,
  concept_id int(10) unsigned NOT NULL,
  score float NOT NULL,
  PRIMARY KEY (institution_id,mooc_id,person_id,publication_id,concept_id),
  KEY institution_id (institution_id),
  KEY mooc_id (mooc_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_MOOC_N_Publication_N_Concept_T_ConceptDetection (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  mooc_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  PRIMARY KEY (institution_id,mooc_id,publication_id,concept_id),
  KEY institution_id (institution_id),
  KEY mooc_id (mooc_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Person_N_Publication_N_Concept_T_ConceptDetection (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  person_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  PRIMARY KEY (institution_id,person_id,publication_id,concept_id),
  KEY institution_id (institution_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY idx_t_inst_person_concept_score (institution_id,person_id,concept_id,score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Person_N_Publication_T_Authorship (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  person_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  PRIMARY KEY (institution_id,person_id,publication_id),
  KEY institution_id (institution_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Publication_N_Concept_T_ConceptDetection (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  PRIMARY KEY (institution_id,publication_id,concept_id),
  KEY institution_id (institution_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Unit_N_Person_T_Affiliation (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  unit_id varchar(255) NOT NULL,
  person_id varchar(255) NOT NULL,
  position_group varchar(255) NOT NULL,
  PRIMARY KEY (institution_id,unit_id,person_id),
  KEY institution_id (institution_id),
  KEY unit_id (unit_id),
  KEY person_id (person_id),
  KEY position_group (position_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Traversal_N_Unit_N_Publication_N_Concept_T_ConceptDetection (
  institution_id enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  unit_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  PRIMARY KEY (institution_id,unit_id,publication_id,concept_id),
  KEY institution_id (institution_id),
  KEY unit_id (unit_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
