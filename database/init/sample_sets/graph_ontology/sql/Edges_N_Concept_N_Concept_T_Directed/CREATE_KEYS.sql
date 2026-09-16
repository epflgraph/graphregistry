ALTER TABLE `Edges_N_Concept_N_Concept_T_Directed` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Directed` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Directed` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Directed` ADD KEY `record_deleted` (`record_deleted`);

