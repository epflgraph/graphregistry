-- ==========================================
-- Graph traversal: Course number of lectures
-- ==========================================

-- ======================= Graph traversal: Course number of lectures (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Course_Lecture__NSlides (
                            course_id  VARCHAR(255) NOT NULL,
                            lecture_id VARCHAR(255) NOT NULL,
                            n_slides   INT(10) UNSIGNED NOT NULL,
                            UNIQUE KEY unique_key (course_id, lecture_id),
                            KEY course_id (course_id),
                            KEY lecture_id (lecture_id));

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Course_Lecture__NSlides
            SET to_process = 0
          WHERE to_process = 1;

-- ========= Graph traversal: Course number of lectures (REPLACE)
REPLACE INTO [[traversals]].Course_Lecture__NSlides
             (course_id, lecture_id, n_slides, to_process)
       SELECT course_id, lecture_id, COUNT(DISTINCT slide_id) AS n_slides,
              1 AS to_process
        FROM [[traversals]].Course_Lecture_Slide_Concept__LLMValidated
       WHERE to_process = 1
         AND deleted = 0
    GROUP BY course_id, lecture_id;
