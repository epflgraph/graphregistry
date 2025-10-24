INSERT INTO graph_registry.Data_N_Object_N_Object_T_CustomFields
(from_institution_id, from_object_type, from_object_id, to_institution_id, to_object_type, to_object_id, context, field_language, field_name, field_value)

SELECT 'EPFL'   AS from_institution_id,
       'Person' AS from_object_type,
       from_id  AS from_object_id,
       'EPFL'   AS to_institution_id,
       'Unit'   AS to_object_type,
       to_id    AS to_object_id,
       'accreditation' AS context,
       'n/a'    AS field_language,
       'last_position_name' AS field_name,
       COALESCE(position_name_en, position_name_fr_male) AS field_value
FROM (
    SELECT e.*,
           ROW_NUMBER() OVER (
               PARTITION BY from_id, to_id
               ORDER BY start_date DESC
           ) AS rn
    FROM graph.Edges_N_Person_N_Unit e
) t
WHERE rn = 1
AND CAST(from_id AS UNSIGNED) IN (SELECT CAST(object_id AS UNSIGNED) FROM graph_registry.Nodes_N_Object WHERE object_type = 'Person')
AND COALESCE(position_name_en, position_name_fr_male) IS NOT NULL
ORDER BY from_id, to_id;