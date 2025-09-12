CREATE OR REPLACE VIEW graph_registry.Data_N_Object_T_PageProfileAll AS
                SELECT *, row_number() OVER (ORDER BY institution_id ASC, object_type ASC, object_id ASC) AS row_id
                  FROM (SELECT *
                          FROM graph_registry.Data_N_Object_T_PageProfile
                    UNION ALL
                        SELECT *
                          FROM graph_ontology.Data_N_Object_T_PageProfile
                    UNION ALL
                        SELECT *
                          FROM graph_lectures.Data_N_Object_T_PageProfile
                       ) t;