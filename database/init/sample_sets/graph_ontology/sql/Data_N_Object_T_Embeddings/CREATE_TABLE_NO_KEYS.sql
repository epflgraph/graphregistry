CREATE TABLE `Data_N_Object_T_Embeddings` (
  `institution_id` varchar(6) DEFAULT 'Ont',
  `object_type` varchar(32) NOT NULL,
  `object_id` varchar(255) NOT NULL,
  `embedding` text DEFAULT NULL,
  `row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
