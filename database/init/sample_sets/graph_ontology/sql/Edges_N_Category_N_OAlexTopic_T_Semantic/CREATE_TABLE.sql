CREATE TABLE `Edges_N_Category_N_OAlexTopic_T_Semantic` (
  `category_id` varchar(255) NOT NULL,
  `category_name` varchar(255) NOT NULL,
  `topic_id` varchar(255) NOT NULL,
  `topic_name` varchar(255) NOT NULL,
  `embedding_score` float NOT NULL,
  `wikipedia_score` float NOT NULL,
  `score` float NOT NULL,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`category_id`,`topic_id`),
  UNIQUE KEY `row_id` (`row_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
