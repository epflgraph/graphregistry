ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD UNIQUE KEY `row_id` (`row_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD UNIQUE KEY `unique_key` (`from_institution_id`,`from_object_type`,`from_object_id`,`to_institution_id`,`to_object_type`,`to_object_id`,`field_language`,`field_name`,`context`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `from_institution_id` (`from_institution_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `from_object_type` (`from_object_type`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `from_object_id` (`from_object_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `to_institution_id` (`to_institution_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `to_object_type` (`to_object_type`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `to_object_id` (`to_object_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `field_language` (`field_language`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `field_name` (`field_name`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `edge_key` (`from_institution_id`,`from_object_type`,`from_object_id`,`to_institution_id`,`to_object_type`,`to_object_id`);
ALTER TABLE `Data_N_Object_N_Object_T_CustomFields` ADD KEY `record_deleted` (`record_deleted`);

