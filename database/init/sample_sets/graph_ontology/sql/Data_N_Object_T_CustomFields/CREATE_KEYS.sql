ALTER TABLE `Data_N_Object_T_CustomFields` ADD UNIQUE KEY `unique_key` (`institution_id`,`object_type`,`object_id`,`field_language`,`field_name`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `institution_id` (`institution_id`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `object_type` (`object_type`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `object_id` (`object_id`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `field_language` (`field_language`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `field_name` (`field_name`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `object_key` (`institution_id`,`object_type`,`object_id`);
ALTER TABLE `Data_N_Object_T_CustomFields` ADD KEY `record_deleted` (`record_deleted`);

