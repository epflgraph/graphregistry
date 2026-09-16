ALTER TABLE `Data_N_Object_T_Embeddings` ADD UNIQUE KEY `object_key` (`institution_id`,`object_type`,`object_id`);
ALTER TABLE `Data_N_Object_T_Embeddings` ADD KEY `institution_id` (`institution_id`);
ALTER TABLE `Data_N_Object_T_Embeddings` ADD KEY `object_type` (`object_type`);
ALTER TABLE `Data_N_Object_T_Embeddings` ADD KEY `object_id` (`object_id`);

