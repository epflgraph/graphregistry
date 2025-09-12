CREATE OR REPLACE VIEW graph_registry.Edges_N_Object_N_Object_T_ScoresMatrix_AVG AS
                SELECT from_institution_id, from_object_type, to_institution_id, to_object_type,
                       AVG(score) AS avg_score, COUNT(*) AS n_rows
                  FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_GBC
              GROUP BY from_institution_id, from_object_type, to_institution_id, to_object_type;