CREATE TABLE `Nodes_N_Category` (
  `institution_id` varchar(6) NOT NULL,
  `object_type` varchar(32) NOT NULL,
  `object_id` varchar(255) NOT NULL,
  `id` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `depth` int(10) unsigned NOT NULL,
  `reference_page_id` int(10) unsigned NOT NULL,
  `reference_page_key` varchar(255) NOT NULL,
  `reference_page_url` varchar(255) NOT NULL,
  `record_created_date` datetime DEFAULT current_timestamp(),
  `record_updated_date` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `record_deleted` tinyint(1) NOT NULL DEFAULT 0,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
