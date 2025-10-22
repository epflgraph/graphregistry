
-- ================= Calculate final degree scores
        REPLACE INTO [[graph_cache]].Nodes_N_Object_T_DegreeScores
                     (institution_id, object_type, object_id, avg_degree, avg_log_degree, avg_norm_log_degree, to_process)

              SELECT d.from_institution_id  AS institution_id,
                     d.from_object_type     AS object_type,
                     d.from_object_id       AS object_id,
                     AVG(d.degree)          AS avg_degree,
                     AVG(d.log_degree)      AS avg_log_degree,
                     AVG(d.norm_log_degree) AS avg_norm_log_degree,
                     1                      AS to_process

                FROM [[graph_cache]].Edges_N_Object_N_Object_T_NormLogDegrees d

          INNER JOIN [[airflow]].Operations_N_Object_T_ScoresExpired se
                  ON (    se.institution_id,     se.object_type,     se.object_id)
                   = (d.from_institution_id, d.from_object_type, d.from_object_id)

          INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
               USING (institution_id, object_type)

               WHERE (d.from_object_type, d.to_object_type) IN
                     (
                       -- ======= Category ======= --
                       (   'Category' , 'Course'     ),
                       (   'Category' , 'Lecture'    ),
                       (   'Category' , 'Person'     ),
                       (   'Category' , 'Publication'),
                       -- ======== Concept ======== --
                       (    'Concept' , 'Course'     ),
                       (    'Concept' , 'Lecture'    ),
                       (    'Concept' , 'Publication'),
                       -- ======== Course ======== --
                       (     'Course' , 'Concept'    ),
                       (     'Course' , 'Lecture'    ),
                       -- ======== Lecture ======== --
                       (    'Lecture' , 'Concept'    ),
                       (    'Lecture' , 'Slide'      ),
                       -- ========= MOOC ========= --
                       (       'MOOC' , 'Concept'    ),
                       (       'MOOC' , 'Lecture'    ),
                       -- ======== Person ======== --
                       (     'Person' , 'Publication'),
                       -- ====== Publication ====== --
                       ('Publication' , 'Concept'    ),
                       -- ======== Startup ======== --
                       (    'Startup' , 'Concept'    ),
                       -- ========= Unit ========= --
                       (       'Unit' , 'Concept'    ),
                       (       'Unit' , 'Person'     ),
                       -- ======== Widget ======== --
                       (     'Widget' , 'Concept'    )
                     )

                 AND se.to_process = 1
                 AND tf.flag_type  = 'scores'
                 AND tf.to_process = 1

            GROUP BY d.from_institution_id, d.from_object_type, d.from_object_id;
