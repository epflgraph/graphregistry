rm -rf ~/data/mysql_exports/2026-*
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Concept_to_Course
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Concept_to_StudyPlan
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Course_n_lectures
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Course_to_StudyPlan
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_StudyPlan_Jaccard_Matrix_pLevel
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_Lecture_Concepts
graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Concept_thresholds

# DROP TABLE IF EXISTS graph_analytics.Flourish_CS_119_Concept_to_Course;
# DROP TABLE IF EXISTS graph_analytics.Flourish_CS_119_Concept_to_StudyPlan;
# DROP TABLE IF EXISTS graph_analytics.Flourish_CS_119_Course_n_lectures;
# DROP TABLE IF EXISTS graph_analytics.Flourish_CS_119_Course_to_StudyPlan;
# DROP TABLE IF EXISTS graph_analytics.Flourish_StudyPlan_Jaccard_Matrix_pLevel;
# DROP TABLE IF EXISTS graph_analytics.Flourish_Lecture_Concepts;

# graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_CS_119_Full_Data
# graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name Flourish_StudyPlan_Jaccard_Matrix
# graphdb copy --from_env xaas_coresrv --to_env analyticsdb --from_schema graph_analytics --to_schema graph_analytics --chunk_size 100000 --table_name StudyPlan_Levels

