ALTER TABLE `Edges_N_Concept_N_Concept_T_Undirected` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Undirected` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Undirected` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Undirected` ADD KEY `record_deleted` (`record_deleted`);

