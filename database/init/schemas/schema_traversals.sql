CREATE TABLE IF NOT EXISTS Category_Cluster_Concept__FullOntology (
  root_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  root_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_1_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_1_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_2_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_2_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_3_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_3_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_4_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  category_4_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  cluster_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  concept_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  concept_name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  to_process tinyint(1) unsigned DEFAULT 0,
  deleted tinyint(1) unsigned DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (root_id,category_1_id,category_2_id,category_3_id,category_4_id,cluster_id,concept_id) USING HASH,
  KEY root_id (root_id),
  KEY root_name (root_name),
  KEY category_1_id (category_1_id),
  KEY category_1_name (category_1_name),
  KEY category_2_id (category_2_id),
  KEY category_2_name (category_2_name),
  KEY category_3_id (category_3_id),
  KEY category_3_name (category_3_name),
  KEY category_4_id (category_4_id),
  KEY category_4_name (category_4_name),
  KEY cluster_id (cluster_id),
  KEY concept_id (concept_id),
  KEY concept_name (concept_name),
  KEY to_process (to_process),
  KEY deleted (deleted),
  KEY concept_category4 (concept_id,category_4_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS Concept_Concept__Depth2 (
  from_concept_id varchar(255) NOT NULL,
  to_concept_id varchar(255) NOT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL,
  PRIMARY KEY (row_id),
  UNIQUE KEY row_id (row_id),
  UNIQUE KEY unique_key (from_concept_id,to_concept_id),
  KEY from_concept_id (from_concept_id),
  KEY to_concept_id (to_concept_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course_Concept__CoverageScore (
  course_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float DEFAULT NULL,
  idx_course_id char(3) NOT NULL,
  to_process tinyint(3) unsigned DEFAULT 0,
  deleted tinyint(3) unsigned DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (course_id,concept_id),
  KEY course_id (course_id),
  KEY concept_id (concept_id),
  KEY idx_course_id (idx_course_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course_Lecture_Concept__CoverageScore (
  course_id varchar(255) NOT NULL,
  n_lectures int(10) unsigned DEFAULT NULL,
  lecture_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float DEFAULT NULL,
  idx_course_id char(3) NOT NULL,
  idx_lecture_id char(3) NOT NULL,
  to_process tinyint(3) unsigned DEFAULT 0,
  deleted tinyint(3) unsigned DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (course_id,lecture_id,concept_id),
  KEY course_id (course_id),
  KEY lecture_id (lecture_id),
  KEY concept_id (concept_id),
  KEY idx_course_id (idx_course_id),
  KEY idx_lecture_id (idx_lecture_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course_Lecture_Slide_Concept__ConceptDetection (
  course_id varchar(255) NOT NULL,
  lecture_id varchar(255) NOT NULL,
  slide_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  deleted tinyint(4) DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (course_id,lecture_id,slide_id,concept_id) USING HASH,
  KEY to_process (to_process),
  KEY deleted (deleted),
  KEY course_id (course_id),
  KEY lecture_id (lecture_id),
  KEY slide_id (slide_id),
  KEY concept_id (concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course_Lecture_Slide_Concept__LLMValidated (
  course_id varchar(255) NOT NULL,
  n_lectures int(10) unsigned DEFAULT NULL,
  lecture_id varchar(255) NOT NULL,
  n_slides int(10) unsigned NOT NULL,
  slide_id varchar(255) NOT NULL,
  slide_timestamp int(10) unsigned NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  idx_course_id char(3) NOT NULL,
  idx_lecture_id char(3) NOT NULL,
  to_process tinyint(3) unsigned DEFAULT 0,
  deleted tinyint(3) unsigned DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (course_id,lecture_id,slide_id,concept_id) USING HASH,
  KEY course_id (course_id),
  KEY lecture_id (lecture_id),
  KEY slide_id (slide_id),
  KEY concept_id (concept_id),
  KEY idx_course_id (idx_course_id),
  KEY idx_lecture_id (idx_lecture_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course_Lecture__NSlides (
  course_id varchar(255) NOT NULL,
  lecture_id varchar(255) NOT NULL,
  n_slides bigint(21) NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  deleted tinyint(4) DEFAULT 0,
  UNIQUE KEY unique_key (course_id,lecture_id),
  KEY course_id (course_id),
  KEY lecture_id (lecture_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Course__NLectures (
  course_id varchar(255) NOT NULL,
  n_lectures int(10) unsigned NOT NULL,
  to_process tinyint(4) DEFAULT 0,
  deleted tinyint(4) DEFAULT 0,
  KEY course_id (course_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS MOOC_Person_Publication_Concept__ConceptDetection (
  mooc_id varchar(32) NOT NULL,
  person_id int(10) unsigned NOT NULL,
  publication_id varchar(128) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (mooc_id,person_id,publication_id,concept_id),
  KEY mooc_id (mooc_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS MOOC_Publication_Concept__ConceptDetection (
  mooc_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (mooc_id,publication_id,concept_id),
  KEY mooc_id (mooc_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Person_Publication_Concept__ConceptDetection (
  person_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  idx_publication_id char(2) DEFAULT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (person_id,publication_id,concept_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY deleted (deleted),
  KEY idx_publication_id (idx_publication_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Person_Publication__Authorship (
  person_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  idx_publication_id char(2) DEFAULT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (person_id,publication_id),
  KEY person_id (person_id),
  KEY publication_id (publication_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Publication_Concept__ConceptDetection (
  publication_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  idx_publication_id char(2) DEFAULT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (publication_id,concept_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY to_process (to_process),
  KEY deleted (deleted),
  KEY idx_publication_id (idx_publication_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Unit_Person__Affiliation (
  unit_id varchar(255) NOT NULL,
  person_id varchar(255) NOT NULL,
  position_group varchar(255) NOT NULL,
  to_process tinyint(4) NOT NULL DEFAULT 0,
  deleted tinyint(4) NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (unit_id,person_id),
  KEY unit_id (unit_id),
  KEY person_id (person_id),
  KEY position_group (position_group),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS Unit_Publication_Concept__ConceptDetection (
  unit_id varchar(255) NOT NULL,
  publication_id varchar(255) NOT NULL,
  concept_id varchar(10) DEFAULT NULL,
  score float NOT NULL,
  idx_publication_id char(2) NOT NULL,
  to_process tinyint(3) unsigned NOT NULL DEFAULT 0,
  deleted tinyint(3) unsigned NOT NULL DEFAULT 0,
  row_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (row_id),
  UNIQUE KEY unique_key (unit_id,publication_id,concept_id),
  KEY unit_id (unit_id),
  KEY publication_id (publication_id),
  KEY concept_id (concept_id),
  KEY idx_publication_id (idx_publication_id),
  KEY to_process (to_process),
  KEY deleted (deleted)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
