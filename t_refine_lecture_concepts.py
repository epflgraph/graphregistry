
# # # import graph db
# # from graphdb.core.graphdb import GraphDB

# # db = GraphDB()

# # lecture_id = '0_192jngsv'

# # sql_query = f"""
# # SELECT DISTINCT t2.from_object_id AS slide_id, c.field_value AS ocr_content,
# #                 GROUP_CONCAT(o.name SEPARATOR '|') AS concepts
# #            FROM graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t1
# #      INNER JOIN graph_lectures.Edges_N_Object_N_Object_T_ChildToParent t2
# #              ON t1.from_object_id = t2.to_object_id
# #      INNER JOIN graph_lectures.Data_N_Object_T_CustomFields c
# #              ON (c.object_type, c.object_id, c.field_language, c.field_name) = ('Slide', t2.from_object_id, 'en', 'text')
# #      INNER JOIN graph_lectures.Edges_N_Object_N_Concept_T_ConceptDetection d
# #              ON (d.object_type, d.object_id) = ('Slide', t2.from_object_id)
# #      INNER JOIN graph_ontology.Nodes_N_Concept o
# # 			 ON d.concept_id = o.object_id
# #           WHERE (t1.from_object_type, t1.to_object_type, t1.to_object_id) = ('Lecture', 'Course', 'CS-119(a)')
# #             AND (t2.from_object_type, t2.to_object_type) = ('Slide', 'Lecture')
# #             AND t1.from_object_id = '{lecture_id}'
# # 	   GROUP BY t2.from_object_id, c.field_value;
# # """

# # out = db.execute_query(engine_name='xaas_coresrv', query=sql_query)


# # json_output = {"lecture_id":lecture_id, "keyframes": []}
# # for slide_id, ocr_content, concepts in out:
# #     concept_list = concepts.split('|')
# #     json_output["keyframes"].append({
# #         slide_id : {
# #             "ocr_content": ocr_content,
# #             "concepts": concept_list
# #         }
# #     })

# # import rich
# # rich.print_json(data=json_output)

# # import repo for enrichment tasks
# from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
# from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask
# from graphregistry.domain.models.entities.mdl_base import NodeKey
# from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper

# # Global config
# from graphregistry.common.config import GlobalConfig
# from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver

# from graphdb.core.graphdb import GraphDB

# db = GraphDB()

# # Helper: Build default schema resolver
# def _make_schema_resolver() -> DefaultSchemaResolver:
#     return DefaultSchemaResolver(
#         engine_name="xaas_coresrv",
#         glbcfg=GlobalConfig(),
#     )

# # Helper: Build lecture repository
# def _make_lecture_repo() -> MySQLLectureRepository:
#     return MySQLLectureRepository(
#         db=db,
#         schema_resolver=_make_schema_resolver(),
#     )


# lecture_id = '0_192jngsv'

# key = NodeKey(
#     institution_id = 'EPFL',
#     object_type    = 'Lecture',
#     object_id      = lecture_id,
# )

# # Initialize the repository and fetch the enrichment task for the lecture
# repo = _make_lecture_repo()
# enrich_task = repo.get_enrichment_task(key)

# assert isinstance(enrich_task, LectureEnrichmentTask)

# import rich
# # rich.print(enrich_task)

# data_json = MySQLLectureEnrichmentTaskMapper.to_dict(enrich_task)

# rich.print_json(data=data_json)


# Initialise lecture operations
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.application.gateways.types import GatewayDict
# LectureEnrichmentGateway
from graphregistry.application.gateways.gtw_lectureenrich import LectureEnrichmentGateway

# Initialise repo
lecture_repo: LectureRepository = _make_lecture_repo()


lecture_ops = LectureOperations(
    repo = lecture_repo,
    ai_gateways = LectureEnrichmentGateway()
)