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

# Initialize the lecture operations with the MySQL repository and the GenAI enrichment gateway
lecture_ops = LectureOperations(
    repo = MySQLLectureRepository(
        db = GraphDB(),
        schema_resolver = DefaultSchemaResolver(engine_name="xaas_coresrv", glbcfg=GlobalConfig())
    ),
    ai_gateways = cast(GatewayDict, {
        "concept_detection"  : GraphAIConceptDetectionGateway(),
        "lecture_enrichment" : GenAILectureEnrichmentGateway()
    })
)

# Run from scratch?
if False:

    # Run the enrichment operation for a specific lecture ID
    start_time = datetime.datetime.now()
    result = lecture_ops.enrich(lecture_id="0_2hrj7yhs")
    if result is None:
        print("Enrichment failed or no enrichment result returned.")
        exit()

    # rich.print(result)
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    rich.print(f"Enrichment completed in {elapsed_time.total_seconds()} seconds")

    # Write to pickle file
    with open("enrichment_result.pkl", "wb") as f:
        pickle.dump(result, f)

# Run from cache
else:
    # Load from pickle file (for testing)
    with open("enrichment_result.pkl", "rb") as f:
        result = pickle.load(f)
    rich.print(result)

# Save enriched node
lecture_ops.save_enrichment(result)