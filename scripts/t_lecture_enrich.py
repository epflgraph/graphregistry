from typing import cast
import datetime, pickle, rich
from loguru import logger as sysmsg
from graphdb.core.graphdb import GraphDB
from graphregistry.adapters.gateways.genai.agt_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.common.config import GlobalConfig

# Initialize the MySQL connection and the graph database
db = GraphDB()

# Get schema name
engine_name = "xaas_coresrv"
schema_name = DefaultSchemaResolver(engine_name=engine_name, glbcfg=GlobalConfig())

# Initialize the lecture operations with the MySQL repository and the GenAI enrichment gateway
lecture_ops = LectureOperations(
    repo=MySQLLectureRepository(db, schema_name),
    ai_gateways=cast(
        GatewayDict,
        {
            "concept_detection": GraphAIConceptDetectionGateway(),
            "lecture_enrichment": GenAILectureEnrichmentGateway(),
        },
    ),
)

# Get n_batch from command line arguments
import sys
if len(sys.argv) > 1:
    n_batch = int(sys.argv[1])
else:
    raise ValueError("Please provide n_batch as a command line argument.")

# Run from scratch?
if True:

    # Get list on unprocessed lecture IDs
    # if True:
    #     list_of_lectures = [r[0] for r in db.execute_query(engine_name=engine_name,
    #         query="""
    #         SELECT DISTINCT v.object_id AS lecture_id
    #                    FROM graph_lectures.Nodes_N_Object v
    #              INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent c
    #                      ON (c.from_object_type, c.from_object_id) = (v.object_type, v.object_id)
    #              INNER JOIN (SELECT course_id, AVG(score) AS score
    #                            FROM (
    #                               SELECT to_object_id AS course_id, score
    #                                 FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS
    #                                WHERE (from_object_type, to_object_type) = ('Course', 'Course')
    #                                  AND from_object_id LIKE 'CS-119%'
    #                                  AND score >= 0.5
    #                                UNION
    #                               SELECT from_object_id AS course_id, score
    #                                 FROM graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS
    #                                WHERE (from_object_type, to_object_type) = ('Course', 'Course')
    #                                  AND to_object_id LIKE 'CS-119%'
    #                                  AND score >= 0.5
    #                                 ) t
    #                        GROUP BY course_id) tt
    #                      ON c.to_object_id = tt.course_id
    #                   WHERE v.object_type = 'Lecture'
    #                     AND v.object_id NOT IN (SELECT DISTINCT object_id FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated)
    #                ORDER BY tt.score DESC;
    #     """)]
    # else:
    # if True:
    #     list_of_lectures = [r[0] for r in db.execute_query(engine_name=engine_name,
    #         query="""
    #         SELECT DISTINCT l.from_object_id AS lecture_id
    #                    FROM graph_registry.Edges_N_Object_N_Object_T_ChildToParent c
    #              INNER JOIN graph_registry.Data_N_Object_T_CustomFields f
    #                      ON (c.to_object_type, c.to_object_id, c.context, 'en', 'level') = (f.object_type, f.object_id, 'coursebook', f.field_language, f.field_name)
    #              INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent l 
    #                      ON (c.from_object_type, c.from_object_id, 'part of') = (l.to_object_type, l.to_object_id, l.context)
    #                   WHERE (c.from_object_type, c.to_object_type, c.context) = ('Course', 'StudyPlan', 'coursebook')
    #                     AND f.field_value = 'Bachelor Cycle' 
    #                     AND c.to_object_id LIKE '%2025-2026'
    #                     AND c.to_object_id NOT IN ('bachelor-architecture-2025-2026', 'bachelor-projeter-ensemble-enac-2025-2026')
    #                     AND l.from_object_id NOT IN (SELECT DISTINCT object_id FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated)
    #                ORDER BY c.to_object_id, l.to_object_id, l.from_object_id;
    #     """)]
    if True:
        
        sql_query=f"""
            SELECT DISTINCT l.from_object_id AS lecture_id
                       FROM graph_registry.Edges_N_Object_N_Object_T_ChildToParent c
                 INNER JOIN graph_registry.Data_N_Object_T_CustomFields f
                         ON (c.to_object_type, c.to_object_id, c.context, 'en', 'level') = (f.object_type, f.object_id, 'coursebook', f.field_language, f.field_name)
                 INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent l 
                         ON (c.from_object_type, c.from_object_id, 'part of') = (l.to_object_type, l.to_object_id, l.context)
                      WHERE (c.from_object_type, c.to_object_type, c.context) = ('Course', 'StudyPlan', 'coursebook')
                        AND f.field_value IN ('Bachelor Cycle', 'Propedeutics')
                        AND c.to_object_id LIKE '%2025-2026'
                        AND c.to_object_id NOT IN ('bachelor-architecture-2025-2026', 'bachelor-projeter-ensemble-enac-2025-2026')
                        AND l.from_object_id NOT IN (SELECT DISTINCT object_id FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated)
                        AND (MOD(CRC32(l.from_object_id), 9) + 1) = {n_batch}
                   ORDER BY c.to_object_id, l.to_object_id, l.from_object_id;
        """
        list_of_lectures = [r[0] for r in db.execute_query(engine_name=engine_name, query=sql_query)]
    else:
        list_of_lectures = ['0_rja1k2lk']

    # Get total number of lectures to process
    N = len(list_of_lectures)

    # Loop over lecture IDs
    for k, lecture_id in enumerate(list_of_lectures, start=1):
        
        # Get current time
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Print the lecture ID being processed
        rich.print(f"🎥 Processing lecture ID ({k}/{N}): {lecture_id} at {current_time}")
        
        # Run the enrichment operation for a specific lecture ID
        start_time = datetime.datetime.now()
        try:
            if True:
                result = lecture_ops.enrich(lecture_id=lecture_id)
            else:
                with open(f"data/lecture_refined_concepts/enrichment_result_{lecture_id}.pkl", "rb") as f:
                    result = pickle.load(f)
        except Exception as exc:
            # Defensive fallback so one failure does not stop the whole batch.
            sysmsg.exception(
                "Skipping lecture_id={} due to unexpected enrichment failure: {}",
                lecture_id,
                exc,
            )
            continue

        if result is None:
            continue

        rich.print(result)

        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        rich.print(f"Enrichment completed in {elapsed_time.total_seconds()} seconds")

        # Write to pickle file
        with open(f"data/lecture_refined_concepts/enrichment_result_{lecture_id}.pkl", "wb") as f:
            pickle.dump(result, f)

        # Save enriched node
        try:
            lecture_ops.save_enrichment(result)
        except Exception as exc:
            # Defensive fallback so one failure does not stop the whole batch.
            sysmsg.exception(
                "Failed to save enrichment for lecture_id={}: {}", lecture_id, exc
            )
            continue

# Run from cache
else:
    # Load from pickle file (for testing)
    with open("data/lecture_refined_concepts/enrichment_result_0_2hrj7yhs.pkl", "rb") as f:
        result = pickle.load(f)
    rich.print(result)
