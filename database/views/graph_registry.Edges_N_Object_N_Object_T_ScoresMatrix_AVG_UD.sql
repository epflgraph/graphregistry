CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ScoresMatrix_AVG_UD AS
                SELECT from_institution_id, from_object_type, to_institution_id, to_object_type,
                       AVG(score) AS avg_score, COUNT(*) AS n_rows
                  FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                 WHERE (from_object_type, to_object_type)
                    IN (SELECT DISTINCT from_object_type, to_object_type
                                   FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
                                  WHERE to_process = 1
                               GROUP BY from_object_type, to_object_type)
              GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type;