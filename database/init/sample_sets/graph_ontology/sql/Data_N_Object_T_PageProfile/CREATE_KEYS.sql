ALTER TABLE `Data_N_Object_T_PageProfile` ADD UNIQUE KEY `unique_key` (`institution_id`,`object_type`,`object_id`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `institution_id` (`institution_id`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `object_type` (`object_type`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `object_id` (`object_id`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `object_key` (`institution_id`,`object_type`,`object_id`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `subtype_en` (`subtype_en`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `subtype_fr` (`subtype_fr`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `subtype_de` (`subtype_de`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `subtype_it` (`subtype_it`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `short_code` (`short_code`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `numeric_id_fr` (`numeric_id_fr`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `numeric_id_de` (`numeric_id_de`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `numeric_id_it` (`numeric_id_it`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `numeric_id_en` (`numeric_id_en`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `is_visible` (`is_visible`);
ALTER TABLE `Data_N_Object_T_PageProfile` ADD KEY `record_deleted` (`record_deleted`);

