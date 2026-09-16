CREATE TABLE `Data_N_Object_N_Object_T_CustomFields` (
  `from_institution_id` enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  `from_object_type` varchar(32) NOT NULL,
  `from_object_id` varchar(255) NOT NULL,
  `to_institution_id` enum('Ont','EPFL','ETHZ','PSI','Empa','Eawag','WSL') NOT NULL,
  `to_object_type` varchar(32) NOT NULL,
  `to_object_id` varchar(255) NOT NULL,
  `context` varchar(32) NOT NULL,
  `field_language` enum('en','fr','de','it','n/a') NOT NULL,
  `field_name` varchar(64) NOT NULL,
  `field_value` longtext DEFAULT NULL,
  `record_created_date` datetime DEFAULT current_timestamp(),
  `record_updated_date` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `record_deleted` tinyint(1) NOT NULL DEFAULT 0,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
