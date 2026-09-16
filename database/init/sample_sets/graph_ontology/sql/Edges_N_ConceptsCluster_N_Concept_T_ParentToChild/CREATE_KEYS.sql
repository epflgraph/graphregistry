ALTER TABLE `Edges_N_ConceptsCluster_N_Concept_T_ParentToChild` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_ConceptsCluster_N_Concept_T_ParentToChild` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_ConceptsCluster_N_Concept_T_ParentToChild` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_ConceptsCluster_N_Concept_T_ParentToChild` ADD KEY `record_deleted` (`record_deleted`);

