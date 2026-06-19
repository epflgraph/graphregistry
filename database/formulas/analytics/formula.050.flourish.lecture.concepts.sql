
-- ========= Object type: Lecture
-- ========= Formula: 'top concepts ai validated'
REPLACE INTO _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
            (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT institution_id, object_type, object_id, concept_id,
             'top concepts ai validated' AS calculation_type, score
        FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated
       WHERE object_type = 'Lecture';


-- ========= Object type: Lecture
-- ========= Formula: 'slide count ai validated'
REPLACE INTO _1_DEV_graph_cache.Edges_N_Object_N_Concept_T_CalculatedScores
             (institution_id, object_type, object_id, concept_id, calculation_type, score)
      SELECT 'EPFL' AS institution_id, 'Lecture' AS object_type, lecture_id AS object_id, concept_id,
             'slide count ai validated' AS calculation_type,
             0.5 + 0.5 * (LN(1 + COUNT(DISTINCT slide_id)) / LN(1 + MAX(n_slides))) AS score
        FROM (SELECT p.to_object_id AS lecture_id, v.object_id AS slide_id, concept_id, c.field_value AS n_slides
                FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated v
          INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent p
                  ON p.from_object_id = v.object_id
          INNER JOIN graph_lectures.Data_N_Object_T_CustomFields c
                  ON (c.object_type, c.object_id) = (p.to_object_type, p.to_object_id)
               WHERE (v.object_type, p.to_object_type, p.context, c.field_language, c.field_name) = ('Slide', 'Lecture', 'part of', 'n/a', 'n_slides')
             ) t
    GROUP BY lecture_id, concept_id;
