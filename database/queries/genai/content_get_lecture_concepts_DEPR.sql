-- Get the detected Wikipedia-style concepts for each slide in one lecture,
-- one row per slide with concept names pipe-concatenated for the mapper.
    SELECT t.from_object_id AS slide_id, GROUP_CONCAT(DISTINCT o.name SEPARATOR '|') AS concepts
      FROM [[lectures]].Edges_N_Object_N_Object_T_ChildToParent t
INNER JOIN [[lectures]].Edges_N_Object_N_Concept_T_ConceptDetection d
        ON (d.object_type, d.object_id) = ('Slide', t.from_object_id)
       AND d.record_deleted = 0
INNER JOIN [[ontology]].Nodes_N_Concept o
        ON d.concept_id = o.object_id
     WHERE (t.from_object_type, t.to_object_type) = ('Slide', 'Lecture')
       AND t.to_object_id = '[[lecture_id]]'
       AND t.record_deleted = 0
     GROUP BY t.from_object_id;
