-- ==========================================
-- Graph traversal: Course number of lectures
-- ==========================================

-- ======================= Graph traversal: Course number of lectures (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Course__NLectures (
                            course_id VARCHAR(255) NOT NULL,
                            n_lectures INT(10) unsigned NOT NULL,
                            UNIQUE KEY course_id (course_id));

-- ========= Graph traversal: Course number of lectures (REPLACE)
REPLACE INTO [[traversals]].Course__NLectures
             (course_id, n_lectures)
      SELECT course_id, COUNT(DISTINCT lecture_id) AS n_lectures
        FROM [[traversals]].Course_Lecture_Slide_Concept__LLMValidated
       WHERE to_process = 1
    GROUP BY course_id;
