# graphregistry/entrypoints/cli/dependencies.py
"""Centralized dependency builders for CLI handlers and scripts.

These builders wire adapters (MySQL UnitOfWork, GraphAI/GenAI gateways) with
application operations while keeping the wiring logic in one place. They are
entrypoint concerns: they know about concrete adapters so the handlers and
scripts don't have to repeat that knowledge.
"""
from __future__ import annotations
from typing import Callable
from graphdb.core.graphdb import GraphDB
from graphregistry.adapters.gateways.genai.gtw_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_lecturerepo import MySQLLectureRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.common.config import GlobalConfig
from graphregistry.entrypoints.dependencies import build_uow_factory

#================================================================#
# Function Group: Schema resolver builders                       #
#================================================================#

# Public Method: Build the default schema resolver for a registry environment.
def build_schema_resolver(*, engine_name: str, global_config: GlobalConfig) -> DefaultSchemaResolver:
    """Build the default schema resolver for a registry environment."""
    return DefaultSchemaResolver(engine_name=engine_name, glbcfg=global_config)

#================================================================#
# Function Group: Operation builders                             #
#================================================================#

# Public Method: Build node operations wired to a UnitOfWork factory.
def build_node_operations(*, uow_factory: Callable[[], UnitOfWork], concept_detection_gateway: ConceptDetectionGateway | None = None) -> NodeOperations:
    """Build node operations wired to a UnitOfWork factory."""
    return NodeOperations(
        uow_factory               = uow_factory,
        concept_detection_gateway = concept_detection_gateway,
    )

# Public Method: Build edge operations wired to a UnitOfWork factory.
def build_edge_operations(*, uow_factory: Callable[[], UnitOfWork]) -> EdgeOperations:
    """Build edge operations wired to a UnitOfWork factory."""
    return EdgeOperations(uow_factory=uow_factory)

#----------------------------------------------------------------#
# Internal Function: Build lecture operations wired to the MySQL repository
# and AI gateways.
#----------------------------------------------------------------#
# Public Method: build lecture operations
def build_lecture_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
    include_video_gateway: bool = True,
    include_concept_gateway: bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
#----------------------------------------------------------------#
    """Build lecture operations wired to the MySQL repository and AI gateways."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    node_repo = MySQLNodeRepository(db=db, schema_resolver=schema_resolver)
    lecture_repo = MySQLLectureRepository(
        db              = db,
        schema_resolver = schema_resolver,
        node_repo       = node_repo,
    )

    # Instantiate the requested AI gateways, using None for disabled features.
    video_processing_gateway = GraphAIVideoGateway() if include_video_gateway else None
    concept_detection_gateway = GraphAIConceptDetectionGateway() if include_concept_gateway else None
    lecture_enrichment_gateway = GenAILectureEnrichmentGateway() if include_enrichment_gateway else None

    # Assemble the lecture operations with all selected gateways.
    return LectureOperations(
        repo                      = lecture_repo,
        video_processing_gateway  = video_processing_gateway,
        concept_detection_gateway = concept_detection_gateway,
        lecture_enrichment_gateway= lecture_enrichment_gateway,
    )

# Public Method: Build lecture operations for enrichment workflows (no video gateway).
def build_lecture_enrichment_operations(*, db: GraphDB, engine_name: str, global_config: GlobalConfig) -> LectureOperations:
    """Build lecture operations for enrichment workflows (no video gateway)."""
    return build_lecture_operations(
        db                    = db,
        engine_name           = engine_name,
        global_config         = global_config,
        include_video_gateway = False,
    )

#================================================================#
# Function Group: CLI-specific builders from argparse context    #
#================================================================#

# Public Method: Build node operations from a CLI args namespace.
def build_node_operations_from_args(args, *, concept_detection_gateway: ConceptDetectionGateway | None = None) -> NodeOperations:
    """Build node operations from a CLI args namespace."""
    uow_factory = build_uow_factory(db=args.ctx.db, engine_name=args.env)
    return build_node_operations(
        uow_factory               = uow_factory,
        concept_detection_gateway = concept_detection_gateway,
    )

# Public Method: Build edge operations from a CLI args namespace.
def build_edge_operations_from_args(args) -> EdgeOperations:
    """Build edge operations from a CLI args namespace."""
    uow_factory = build_uow_factory(db=args.ctx.db, engine_name=args.env)
    return build_edge_operations(uow_factory=uow_factory)

# Public Method: Build node operations with a GraphAI concept-detection gateway from CLI
# args.
# Public Method: Build node operations with a GraphAI concept-detection gateway
def build_node_operations_with_concept_detection_from_args(args) -> NodeOperations:
    """Build node operations with a GraphAI concept-detection gateway."""
    uow_factory = build_uow_factory(db=args.ctx.db, engine_name=args.env)
    return build_node_operations(
        uow_factory               = uow_factory,
        concept_detection_gateway = GraphAIConceptDetectionGateway(),
    )

# Internal Function: Build node and edge operations from a CLI args namespace, sharing one
# UnitOfWork
# factory.
# Public Method: Build node and edge operations from a CLI args namespace, sharing one...
def build_registry_operations_from_args(args) -> tuple[NodeOperations, EdgeOperations]:
    """Build node and edge operations from a CLI args namespace, sharing one UnitOfWork factory."""
    uow_factory = build_uow_factory(db=args.ctx.db, engine_name=args.env)
    return (
        build_node_operations(uow_factory=uow_factory),
        build_edge_operations(uow_factory=uow_factory),
    )

#----------------------------------------------------------------#
# Internal Function: Build lecture operations from a CLI args namespace.
#----------------------------------------------------------------#
# Public Method: build lecture operations from args
def build_lecture_operations_from_args(
    args,
    *,
    include_video_gateway: bool = True,
    include_concept_gateway: bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
#----------------------------------------------------------------#
    """Build lecture operations from a CLI args namespace."""
    return build_lecture_operations(
        db                        = args.ctx.db,
        engine_name               = args.env,
        global_config             = args.ctx.global_config,
        include_video_gateway     = include_video_gateway,
        include_concept_gateway   = include_concept_gateway,
        include_enrichment_gateway= include_enrichment_gateway,
    )
