ALTER TABLE `Edges_N_Category_N_ConceptsCluster_T_ParentToChild` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Category_N_ConceptsCluster_T_ParentToChild` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Category_N_ConceptsCluster_T_ParentToChild` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_Category_N_ConceptsCluster_T_ParentToChild` ADD KEY `record_deleted` (`record_deleted`);

