CREATE TABLE `Edges_N_ConceptsCluster_N_Concept_T_ParentToChild` (
  `from_id` varchar(255) NOT NULL,
  `to_id` varchar(255) NOT NULL,
  `record_created_date` datetime DEFAULT current_timestamp(),
  `record_updated_date` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `record_deleted` tinyint(1) NOT NULL DEFAULT 0,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `unique_key` (`from_id`,`to_id`),
  KEY `from_id` (`from_id`),
  KEY `to_id` (`to_id`),
  KEY `record_deleted` (`record_deleted`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
