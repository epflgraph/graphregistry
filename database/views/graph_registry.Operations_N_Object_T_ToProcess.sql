CREATE OR REPLACE VIEW graph_registry.Operations_N_Object_T_ToProcess AS

       SELECT DISTINCT ccs.institution_id, ccs.object_type, ccs.object_id
                  FROM graph_registry.Stats_N_Object_T_CurrentFieldChecksums ccs
             LEFT JOIN graph_registry.Stats_N_Object_T_PreviousFieldChecksums pcs
                 USING (institution_id, object_type, object_id, calculation_type, field_language)
                 WHERE ccs.checksum != COALESCE(pcs.checksum, '0');