SELECT n.institution_id, n.object_type, n.object_id, 'n/a' AS field_language, 'is_active_unit' AS field_name,
       NOT EXISTS (SELECT 1
                     FROM [[registry]].Data_N_Object_T_CustomFields cf
                    WHERE (cf.object_id, cf.object_type, cf.field_language, cf.field_name)
                        = (n.object_id, 'Unit', 'n/a', 'date_terminated')
                      AND cf.field_value IS NOT NULL) AS field_value
  FROM [[registry]].Nodes_N_Object n
 WHERE n.object_type = 'Unit'