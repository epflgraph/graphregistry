  SELECT from_institution_id, from_object_type, from_object_id,
         to_institution_id,   to_object_type,   to_object_id,
         'n/a' AS field_language, 'latest_teaching_assignment_year' AS field_name, MAX(field_value) AS field_value
    FROM [[registry]].Data_N_Object_N_Object_T_CustomFields
   WHERE (from_object_type, to_object_type) = ('Course', 'Person')
     AND field_name = 'teaching_assignment_year'
GROUP BY from_institution_id, from_object_id, to_institution_id, to_object_id