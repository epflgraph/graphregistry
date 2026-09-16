CREATE TABLE `Edges_N_Category_N_Category_T_ChildToParent` (
  `from_id` varchar(255) NOT NULL,
  `to_id` varchar(255) NOT NULL,
  `record_created_date` datetime DEFAULT current_timestamp(),
  `record_updated_date` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `record_deleted` tinyint(1) NOT NULL DEFAULT 0,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
