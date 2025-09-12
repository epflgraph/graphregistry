CREATE OR REPLACE VIEW graph_registry.Stats_N_Object_N_Object_T_CachedDateSymmetric AS
                SELECT from_institution_id AS from_institution_id,
                       from_object_type    AS from_object_type,
                       from_object_id      AS from_object_id, 
                       to_institution_id   AS to_institution_id,
                       to_object_type      AS to_object_type,
                       to_object_id        AS to_object_id,
                       datetime_cached
                  FROM graph_cache.Stats_N_Object_N_Object_T_CachedDate
             UNION ALL
                SELECT to_institution_id   AS from_institution_id,
                       to_object_type      AS from_object_type,
                       to_object_id        AS from_object_id, 
                       from_institution_id AS to_institution_id,
                       from_object_type    AS to_object_type,
                       from_object_id      AS to_object_id,
                       datetime_cached
                  FROM graph_cache.Stats_N_Object_N_Object_T_CachedDate;