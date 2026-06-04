from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.genai.agt_lectureenrich import GenAILectureEnrichmentGateway
from typing import cast
import rich, pickle, datetime

# Initialize the MySQL connection and the graph database
db = GraphDB()

# Get schema name
engine_name = "xaas_coresrv"
schema_name = DefaultSchemaResolver(engine_name=engine_name, glbcfg=GlobalConfig())

# Initialize the lecture operations with the MySQL repository and the GenAI enrichment gateway
lecture_ops = LectureOperations(
    repo = MySQLLectureRepository(db, schema_name),
    ai_gateways = cast(GatewayDict, {
        "concept_detection"  : GraphAIConceptDetectionGateway(),
        "lecture_enrichment" : GenAILectureEnrichmentGateway()
    })
)

# Run from scratch?
if True:

    # Get list on unprocessed lecture IDs
    list_of_lectures = [r[0] for r in db.execute_query(engine_name=engine_name, query="""
        SELECT object_id FROM _1_DEV_graph_lectures.Nodes_N_Object
         WHERE object_type = 'Lecture'
           AND object_id NOT IN (SELECT object_id FROM _1_DEV_graph_lectures.Edges_N_Object_N_Concept_T_LLMPostValidated WHERE object_type = 'Lecture');
    """) if r is not None]

    # Loop over lecture IDs
    for lecture_id in list_of_lectures:

        # Run the enrichment operation for a specific lecture ID
        start_time = datetime.datetime.now()
        result = lecture_ops.enrich(lecture_id=lecture_id)
        if result is None:
            continue
        else:
            rich.print(result)

        # rich.print(result)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        rich.print(f"Enrichment completed in {elapsed_time.total_seconds()} seconds")

        # Write to pickle file
        with open(f"data/lecture_refined_concepts/enrichment_result_{lecture_id}.pkl", "wb") as f:
            pickle.dump(result, f)

        # Save enriched node
        lecture_ops.save_enrichment(result)

# Run from cache
else:
    # Load from pickle file (for testing)
    with open("data/lecture_refined_concepts/enrichment_result_0_2hrj7yhs.pkl", "rb") as f:
        result = pickle.load(f)
    rich.print(result)
