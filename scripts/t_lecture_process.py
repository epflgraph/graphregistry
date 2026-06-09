from typing import cast
import datetime
import pickle
import rich
from loguru import logger as sysmsg
from graphdb.core.graphdb import GraphDB
from graphregistry.adapters.gateways.genai.agt_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.entities.mdl_base import NodeKey

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
            "video_processing"   : GraphAIVideoGateway(),
            "concept_detection"  : GraphAIConceptDetectionGateway(),
            "lecture_enrichment" : GenAILectureEnrichmentGateway(),
        },
    ),
)

# Generate a test NodeKey for a lecture
test_key = NodeKey(
    institution_id = 'EPFL',
    object_type    = 'Lecture',
    object_id      = '0_042zj2ns'
)

if False:

    # Get file URL for the lecture
    lecture_file_url = lecture_ops.get_file_url(test_key)
    rich.print(f"File URL: {lecture_file_url}")

    # Launch video download and get task ID
    task_id = lecture_ops.launch_video_download(lecture_file_url)
    rich.print(f"Launched video download with task ID: {task_id}")

    # Save the video download task ID in the database
    lecture_ops.save_video_download_task_id(test_key, task_id)

    # Retrieve the video download task ID from the database and print it
    rich.print(f"Video download task ID: {lecture_ops.get_video_download_task_id(test_key)}")

if False:

    # Get the video download result using the task ID and print it
    result = lecture_ops.get_video_download_result(test_key)
    rich.print(f"Video download result:")
    rich.print_json(data=result)

    # Check if the result is not None before proceeding
    assert result is not None, "Video download result is None"

    # Save video token in the database
    lecture_ops.save_video_token(lecture_key=test_key, video_token=result["token"])

    out_token = lecture_ops.get_video_token(test_key)
    rich.print(f"Video token: {out_token}")
    rich.print(f"Video token matches: {out_token == result['token']}")

if False:
    list_of_lectures = lecture_ops.get_unfinished_video_tasks(limit=16)
    for f in list_of_lectures.item_list:
        rich.print(f)

if True:
    list_of_lectures = lecture_ops.get_with_unextracted_audio(limit=16)
    lecture_key = list_of_lectures.item_list[0]
    rich.print(f"Lecture key with unextracted audio: {lecture_key}")

    # Get video token
    video_token = lecture_ops.get_video_token(lecture_key)
    rich.print(f"Video token: {video_token}")

    # Launch audio extraction
    task_id = lecture_ops.launch_audio_extraction(video_token)
    rich.print(f"Launched audio extraction with task ID: {task_id}")

