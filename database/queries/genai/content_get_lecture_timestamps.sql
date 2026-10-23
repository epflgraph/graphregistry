-- Get the persisted start timestamp for each slide in one lecture,
-- from the Slide->Lecture edge custom fields (the live flourish convention,
-- cf. database/formulas/analytics/formula.250.flourish.lecture.concepts.sql).
    SELECT cp.from_object_id AS slide_id, cf.field_value AS start_time_hms
      FROM [[lectures]].Edges_N_Object_N_Object_T_ChildToParent cp
INNER JOIN [[lectures]].Data_N_Object_N_Object_T_CustomFields cf
        ON (cf.from_object_type, cf.from_object_id, cf.to_object_type, cf.to_object_id)
         = (cp.from_object_type,  cp.from_object_id,  cp.to_object_type,  cp.to_object_id)
     WHERE (cp.from_object_type, cp.to_object_type, cp.context) = ('Slide', 'Lecture', 'part of')
       AND (cf.field_language, cf.field_name, cf.context) = ('n/a', 'start_time_hms', 'part of')
       AND cf.record_deleted = 0
       AND cp.record_deleted = 0
       AND cp.to_object_id = '[[lecture_id]]';
