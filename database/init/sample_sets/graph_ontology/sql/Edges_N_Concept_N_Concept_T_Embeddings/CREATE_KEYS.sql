ALTER TABLE `Edges_N_Concept_N_Concept_T_Embeddings` ADD UNIQUE KEY `unique_key` (`from_id`,`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Embeddings` ADD KEY `from_id` (`from_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Embeddings` ADD KEY `to_id` (`to_id`);
ALTER TABLE `Edges_N_Concept_N_Concept_T_Embeddings` ADD KEY `record_deleted` (`record_deleted`);

