from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.genai.agt_lectureenrich import GenAILectureEnrichmentGateway
from typing import cast
import rich

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

# Run the enrichment operation for a specific lecture ID
result = lecture_ops.enrich(lecture_id="0_2hrj7yhs")
rich.print(result)
