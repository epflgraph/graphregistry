SELECT COUNT(link_id) AS n_total,
       [[sum_isnull_field_list]]
  FROM (SELECT DISTINCT link_id, [[custom_fields_list]]
        FROM [[graphsearch_test]].Index_D_[[doc_type]]_L_[[link_type]]_T_[[table_partition]]) t;

-- Example:
-- SELECT COUNT(link_id) AS n_total,
--        SUM(ISNULL(video_stream_url)) AS n_null_video_stream_url,
--        SUM(ISNULL(video_duration)) AS n_null_video_duration,
--        SUM(ISNULL(is_restricted)) AS n_null_is_restricted
--   FROM (SELECT DISTINCT link_id, video_stream_url, video_duration, is_restricted
--         FROM graphsearch_test.Index_D_Course_L_Lecture_T_SEM) t;
