-- ==========================================
-- Graph traversal: Course number of lectures
-- ==========================================

-- ======================= Graph traversal: Course number of lectures (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Course__NLectures (
                            course_id  VARCHAR(255) NOT NULL,
                            n_lectures INT(10) unsigned NOT NULL,
                            to_process TINYINT(1) NOT NULL DEFAULT 0,
                            deleted    TINYINT(1) NOT NULL DEFAULT 0,
                            UNIQUE KEY course_id (course_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted));

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Course__NLectures
            SET to_process = 0
          WHERE to_process = 1;

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Course_Lecture_Slide_Concept__LLMValidated
            SET to_process = 0
          WHERE to_process = 1;

-- ============ Flag setting: Set to_process flags for next query
         UPDATE [[traversals]].Course_Lecture_Slide_Concept__LLMValidated t
     INNER JOIN [[airflow]].Operations_N_Object_T_FieldsChanged tp
             ON t.course_id = tp.object_id
            AND tp.object_type = 'Course'
     INNER JOIN [[airflow]].Operations_N_Object_T_TypeFlags tf
          USING (object_type)
			SET t.to_process = 1
          WHERE tf.flag_type = 'fields'
            AND tf.to_process = 1
            AND tp.to_process = 1;

-- ========= Graph traversal: Course number of lectures (REPLACE)
REPLACE INTO [[traversals]].Course__NLectures
             (course_id, n_lectures, to_process, deleted)
       SELECT course_id, COUNT(DISTINCT lecture_id) AS n_lectures,
              1 AS to_process,
              0 AS deleted
        FROM [[traversals]].Course_Lecture_Slide_Concept__LLMValidated
       WHERE to_process = 1
         AND deleted = 0
    GROUP BY course_id;
