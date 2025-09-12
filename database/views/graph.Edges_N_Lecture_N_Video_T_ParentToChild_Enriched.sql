CREATE OR REPLACE VIEW graph.Edges_N_Lecture_N_Video_T_ParentToChild_Enriched AS
                SELECT nl.id AS lecture_id, nv.id AS video_id, COALESCE(nc.n_slides, 0) AS n_slides, nv.is_restricted, nv.subtype
                  FROM graph.Nodes_N_Lecture nl
            INNER JOIN graph.Edges_N_Lecture_N_Video_T_ParentToChild elv ON nl.id = elv.from_id
            INNER JOIN graph.Nodes_N_Video nv ON elv.to_id = nv.id
             LEFT JOIN (SELECT from_id AS id, COUNT(to_id) AS n_slides
                          FROM graph.Edges_N_Video_N_Slide_T_ParentToChild
                      GROUP BY from_id) nc ON nc.id = nv.id;