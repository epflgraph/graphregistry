CREATE OR REPLACE VIEW graph_lectures.Index_D_Lecture_L_Person_T_ORG AS

SELECT DISTINCT row_number() OVER (ORDER BY doc_id, link_id ASC) AS row_id,
                doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id,
                GROUP_CONCAT(DISTINCT lecture_source ORDER BY lecture_source ASC SEPARATOR ',') AS teaching_formats,
                is_at_epfl, gender, latest_academic_year,
                COALESCE(ds.avg_norm_log_degree, 0.001) AS degree_score, 1 AS row_score, 1 AS row_rank
           FROM (
				   SELECT DISTINCT cl.to_institution_id AS doc_institution,  cl.to_object_type AS  doc_type,                                    cl.to_object_id AS  doc_id,
								   cp.to_institution_id AS link_institution, cp.to_object_type AS link_type, 'Child-to-Parent' AS link_subtype, cp.to_object_id AS link_id,
								   'Course' AS lecture_source,
								   ie.field_value AS is_at_epfl, gd.field_value AS gender, ac.field_value AS latest_academic_year,
								   1 AS degree_score, 1 AS row_score, 1 AS row_rank
							  FROM graph_lectures.Edges_N_Course_N_Lecture_T_ParentToChild cl
						INNER JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent cp
								ON (cl.from_institution_id, cl.from_object_type, cl.from_object_id) = (cp.from_institution_id, cp.from_object_type, cp.from_object_id)
						INNER JOIN graph_registry.Data_N_Object_N_Object_T_CalculatedFields ac
								ON (cl.from_institution_id, cl.from_object_type, cl.from_object_id, 'en', 'latest_academic_year') = (ac.from_institution_id, ac.from_object_type, ac.from_object_id, ac.field_language, ac.field_name)
						INNER JOIN graph_registry.Data_N_Object_T_CalculatedFields cf
								ON (cl.from_institution_id, cl.from_object_type, cl.from_object_id, 'en', 'latest_academic_year') = (cf.institution_id, cf.object_type, cf.object_id, cf.field_language, cf.field_name)
						INNER JOIN graph_registry.Data_N_Object_T_CustomFields gd
								ON (cp.to_institution_id, cp.to_object_type, cp.to_object_id, 'en', 'gender') = (gd.institution_id, gd.object_type, gd.object_id, gd.field_language, gd.field_name)
						INNER JOIN graph_registry.Data_N_Object_T_CalculatedFields ie
								ON (cp.to_institution_id, cp.to_object_type, cp.to_object_id, 'en', 'is_at_epfl')   = (ie.institution_id, ie.object_type, ie.object_id, ie.field_language, ie.field_name)
							 WHERE (cp.from_institution_id, cp.from_object_type, cp.to_institution_id, cp.to_object_type) = ('EPFL', 'Course', 'EPFL', 'Person')
							   AND ac.field_value = cf.field_value
							   
						 UNION ALL
							   
				   SELECT DISTINCT cl.to_institution_id AS doc_institution,  cl.to_object_type AS  doc_type,                                    cl.to_object_id AS  doc_id,
								   cp.to_institution_id AS link_institution, cp.to_object_type AS link_type, 'Child-to-Parent' AS link_subtype, cp.to_object_id AS link_id,
								   'MOOC' AS lecture_source,
								   ie.field_value AS is_at_epfl, gd.field_value AS gender, NULL AS latest_academic_year,
								   1 AS degree_score, 1 AS row_score, 1 AS row_rank
							  FROM graph_lectures.Edges_N_MOOC_N_Lecture_T_ParentToChild cl
						 LEFT JOIN graph_registry.Edges_N_Object_N_Object_T_ChildToParent cp
								ON (cl.from_institution_id, cl.from_object_type, cl.from_object_id) = (cp.from_institution_id, cp.from_object_type, cp.from_object_id)
						 LEFT JOIN graph_registry.Data_N_Object_T_CustomFields gd
								ON (cp.to_institution_id, cp.to_object_type, cp.to_object_id, 'en', 'gender') = (gd.institution_id, gd.object_type, gd.object_id, gd.field_language, gd.field_name)
						 LEFT JOIN graph_registry.Data_N_Object_T_CalculatedFields ie
								ON (cp.to_institution_id, cp.to_object_type, cp.to_object_id, 'en', 'is_at_epfl')   = (ie.institution_id, ie.object_type, ie.object_id, ie.field_language, ie.field_name)
							 WHERE (cp.from_institution_id, cp.from_object_type, cp.to_institution_id, cp.to_object_type) = ('EPFL', 'MOOC', 'EPFL', 'Person')
                ) tt
      LEFT JOIN graph_cache.Stats_N_Object_T_DegreeScores ds
             ON (ds.institution_id, ds.object_type, ds.object_id) = (tt.link_institution, tt.link_type, tt.link_id)
       GROUP BY doc_institution, doc_type, doc_id, link_institution, link_type, link_subtype, link_id;