        -- Get lecture slide detected concepts from registry
    SELECT p.from_object_id AS slide_id, c.concept_id, c.score
      FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent p
INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection c
        ON (p.from_object_type, p.from_object_id) = (c.object_type, c.object_id)
     WHERE (from_object_type, to_object_type, context) = ('Slide', 'Lecture', 'part of')
       AND (to_institution_id, to_object_type, to_object_id) = ('[[institution_id]]', '[[object_type]]', '[[object_id]]')
  ORDER BY from_object_id ASC;
