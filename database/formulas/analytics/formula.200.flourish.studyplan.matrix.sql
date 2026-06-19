

-- CREATE INDEX idx_sp_concept ON Flourish_CS_119_Concept_to_StudyPlan(study_plan_id, concept_id);
-- CREATE INDEX idx_concept_sp ON Flourish_CS_119_Concept_to_StudyPlan(concept_id, study_plan_id);


CREATE TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix AS
WITH plan_concepts AS (
    SELECT DISTINCT
        study_plan_id,
        study_plan_name,
        concept_id
    FROM Flourish_CS_119_Concept_to_StudyPlan
),

plan_sizes AS (
    SELECT
        study_plan_id,
        MAX(study_plan_name) AS study_plan_name,
        COUNT(*) AS n_concepts
    FROM plan_concepts
    GROUP BY study_plan_id
),

intersections AS (
    SELECT
        a.study_plan_id AS from_study_plan_id,
        b.study_plan_id AS to_study_plan_id,
        COUNT(*) AS intersection_concepts
    FROM plan_concepts a
    JOIN plan_concepts b
        ON a.concept_id = b.concept_id
    GROUP BY
        a.study_plan_id,
        b.study_plan_id
)

SELECT
    i.from_study_plan_id,
    pf.study_plan_name AS from_study_plan_name,
    i.to_study_plan_id,
    pt.study_plan_name AS to_study_plan_name,
    i.intersection_concepts,
    pf.n_concepts AS from_concepts,
    pt.n_concepts AS to_concepts,
    pf.n_concepts + pt.n_concepts - i.intersection_concepts AS union_concepts,
    i.intersection_concepts /
        NULLIF(pf.n_concepts + pt.n_concepts - i.intersection_concepts, 0) AS score
FROM intersections i
JOIN plan_sizes pf
    ON pf.study_plan_id = i.from_study_plan_id
JOIN plan_sizes pt
    ON pt.study_plan_id = i.to_study_plan_id;


    ALTER TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);
           



CREATE TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix_pLevel AS
     SELECT from_study_plan_id, from_study_plan_name, CONCAT(from_study_plan_name, ' [', c1.study_plan_level, ']') AS from_study_plan_name_w_level, c1.study_plan_level AS from_study_plan_level,
              to_study_plan_id,   to_study_plan_name, CONCAT(  to_study_plan_name, ' [', c2.study_plan_level, ']') AS   to_study_plan_name_w_level, c2.study_plan_level AS   to_study_plan_level,
			intersection_concepts, from_concepts, to_concepts, union_concepts, score
	   FROM graph_analytics.Flourish_StudyPlan_Jaccard_Matrix m
 INNER JOIN graph_analytics.StudyPlan_Levels c1
         ON m.from_study_plan_id = c1.study_plan_id
 INNER JOIN graph_analytics.StudyPlan_Levels c2
         ON m.to_study_plan_id = c2.study_plan_id;
         
         
         
    ALTER TABLE graph_analytics.Flourish_StudyPlan_Jaccard_Matrix_pLevel
     ADD COLUMN row_id int(11) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (row_id);