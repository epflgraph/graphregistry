-- =======================================================
-- Graph traversal: Course-Concept Coverage Scores
-- =======================================================

-- ======================= Graph traversal: Course-Concept Coverage Scores (CREATE TABLE)
CREATE TABLE IF NOT EXISTS [[traversals]].Course_Concept__CoverageScore (
                            course_id     VARCHAR(255) NOT NULL,
                            concept_id    VARCHAR(255) NOT NULL,
                            score         FLOAT UNSIGNED DEFAULT NULL,
                            idx_course_id CHAR(3) NOT NULL,
                            to_process    TINYINT(3) UNSIGNED DEFAULT 0,
                            deleted       TINYINT(3) UNSIGNED DEFAULT 0,
                            row_id        BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
                            PRIMARY KEY (row_id),
                            UNIQUE KEY unique_key (course_id,concept_id),
                            KEY course_id (course_id),
                            KEY concept_id (concept_id),
                            KEY idx_course_id (idx_course_id),
                            KEY to_process (to_process),
                            KEY deleted (deleted));

-- ============ Cleanup: Reset to_process flags
         UPDATE [[traversals]].Course_Concept__CoverageScore
            SET to_process = 0
          WHERE to_process = 1;

-- ========= Graph traversal: Course-Concept Coverage Scores (REPLACE)
REPLACE INTO [[traversals]].Course_Concept__CoverageScore
			(course_id, concept_id, idx_course_id, score, to_process, deleted)
       SELECT course_id, concept_id,
              LEFT(course_id, 3) AS idx_course_id,
              COUNT(DISTINCT lecture_id) / MAX(n_lectures) * (1 - EXP(-MAX(n_lectures) / 15.0)) AS score,
              1 AS to_process,
              0 AS deleted
        FROM [[traversals]].Course_Lecture_Concept__CoverageScore
       WHERE to_process = 1
         AND deleted = 0
    GROUP BY course_id, concept_id;
