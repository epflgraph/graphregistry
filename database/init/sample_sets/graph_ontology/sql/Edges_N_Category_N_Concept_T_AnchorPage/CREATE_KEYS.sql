ALTER TABLE `Edges_N_Category_N_Concept_T_AnchorPage` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Category_N_Concept_T_AnchorPage` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Category_N_Concept_T_AnchorPage` ADD KEY `to_id` (`to_id`);

