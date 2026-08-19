-- =======================================================
-- Graph traversal: Course-Lecture-Concept Coverage Scores
-- =======================================================

-- ======================= Graph traversal: Course-Lecture-Concept Coverage Scores (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Course_Lecture_Concept__CoverageScore (
                            course_id      VARCHAR(255) NOT NULL,
                            n_lectures     INT(10) UNSIGNED DEFAULT NULL,
                            lecture_id     VARCHAR(255) NOT NULL,
                            concept_id     VARCHAR(255) NOT NULL,
                            score          FLOAT UNSIGNED DEFAULT NULL,
                            idx_course_id  CHAR(3) NOT NULL,
                            idx_lecture_id CHAR(3) NOT NULL,
                            to_process     TINYINT(3) UNSIGNED DEFAULT 0,
                            deleted        TINYINT(3) UNSIGNED DEFAULT 0,
                            row_id         BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (course_id, lecture_id, concept_id),
                            KEY course_id (course_id),
                            KEY lecture_id (lecture_id),
                            KEY concept_id (concept_id),
                            KEY idx_course_id (idx_course_id),
                            KEY idx_lecture_id (idx_lecture_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted));

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Course_Lecture_Concept__CoverageScore
            SET to_process = 0
          WHERE to_process = 1;

-- ========= Graph traversal: Course-Lecture-Concept Coverage Scores (REPLACE)
REPLACE INTO [[traversals]].Course_Lecture_Concept__CoverageScore
             (course_id, n_lectures, lecture_id, concept_id, idx_course_id, idx_lecture_id, score, to_process, deleted)
       SELECT course_id, n_lectures, lecture_id, concept_id,
              LEFT(course_id, 3) AS idx_course_id, LEFT(lecture_id, 3) AS idx_lecture_id,
              COUNT(DISTINCT slide_id) / MAX(n_slides) * (1 - EXP(-MAX(n_slides) / 15.0)) AS score,
              1 AS to_process,
              0 AS deleted
        FROM [[traversals]].Course_Lecture_Slide_Concept__LLMValidated
       WHERE to_process = 1
         AND deleted = 0
    GROUP BY course_id, lecture_id, concept_id;
