SELECT COUNT(doc_id) AS n_total,
       [[sum_isnull_field_list]]
  FROM [[graphsearch_test]].Index_D_[[doc_type]];

-- Example:
-- SELECT COUNT(doc_id) AS n_total,
--        SUM(ISNULL(video_stream_url)) AS n_null_video_stream_url,
--        SUM(ISNULL(video_duration)) AS n_null_video_duration,
--        SUM(ISNULL(is_restricted)) AS n_null_is_restricted
--   FROM [[graphsearch_test]].Index_D_Lecture;
