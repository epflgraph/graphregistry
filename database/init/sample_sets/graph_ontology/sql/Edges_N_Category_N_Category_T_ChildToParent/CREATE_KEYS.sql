ALTER TABLE `Edges_N_Category_N_Category_T_ChildToParent` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Category_N_Category_T_ChildToParent` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Category_N_Category_T_ChildToParent` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_Category_N_Category_T_ChildToParent` ADD KEY `record_deleted` (`record_deleted`);

