CREATE TABLE `Edges_N_Concept_N_Concept_T_Undirected` (
  `from_id` varchar(255) NOT NULL,
  `to_id` varchar(255) NOT NULL,
  `score` float NOT NULL,
  `normalised_score` float DEFAULT NULL,
  `record_deleted` tinyint(1) NOT NULL DEFAULT 0,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `unique_key` (`from_id`,`to_id`),
  KEY `from_id` (`from_id`),
  KEY `to_id` (`to_id`),
  KEY `record_deleted` (`record_deleted`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
