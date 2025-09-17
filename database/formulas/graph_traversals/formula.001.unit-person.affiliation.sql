-- ==============================================
-- Graph traversal: Unit-Person affiliation edges
-- ==============================================

-- ======================= Graph traversal: Unit-Person affiliation edges (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation (
                           institution_id VARCHAR(255) NOT NULL,
                           unit_id        VARCHAR(255) NOT NULL,
                           person_id      VARCHAR(255) NOT NULL,
                           position_group VARCHAR(255) NOT NULL,
                           PRIMARY KEY (institution_id, unit_id, person_id),
                           KEY institution_id (institution_id),
                           KEY unit_id (unit_id),
                           KEY person_id (person_id),
                           KEY position_group (position_group));

-- ============ Graph traversal: Unit-Person affiliation edges (REPLACE)
   REPLACE INTO [[graph_cache]].Traversal_N_Unit_N_Person_T_Affiliation
         SELECT p2u.to_institution_id   AS institution_id,
                p2u.to_object_id        AS unit_id,
                p2u.from_object_id      AS person_id,
                f2.from_field_value     AS position_group
                
           FROM [[airflow]].Operations_N_Object_N_Object_T_FieldsChanged tp

     INNER JOIN [[registry]].Edges_N_Object_N_Object_T_ChildToParent p2u
             ON ( tp.from_institution_id,  tp.from_object_type,  tp.from_object_id,    tp.to_institution_id,    tp.to_object_type,    tp.to_object_id)
              = (p2u.from_institution_id, p2u.from_object_type, p2u.from_object_id,   p2u.to_institution_id,   p2u.to_object_type,   p2u.to_object_id)
     
     INNER JOIN [[registry]].Data_N_Object_N_Object_T_CustomFields cf
             ON ( cf.from_institution_id,  cf.from_object_type,  cf.from_object_id,    cf.to_institution_id,    cf.to_object_type,    cf.to_object_id)
              = (p2u.from_institution_id, p2u.from_object_type, p2u.from_object_id,   p2u.to_institution_id,   p2u.to_object_type,   p2u.to_object_id)

     INNER JOIN [[registry]].Mapping_N_Field_N_Field f1 ON cf.field_value    = f1.from_field_value
     INNER JOIN [[registry]].Mapping_N_Field_N_Field f2 ON f1.to_field_value = f2.from_field_value
          WHERE (f1.from_object_type, f1.from_field_name, f1.to_object_type, f1.to_field_name) = ('Person', 'position_name' , 'Unit', 'position_group')
            AND (f2.from_object_type, f2.from_field_name, f2.to_object_type, f2.to_field_name) = ('Person', 'position_group', 'Unit', 'position_rank')
            
            AND (p2u.from_object_type, p2u.to_object_type) = ('Person', 'Unit')
            AND cf.field_name = 'current_position_name'
            AND f2.from_field_value IN ('Professor', 'Director', 'Researcher')

            AND tp.to_process = 1;
