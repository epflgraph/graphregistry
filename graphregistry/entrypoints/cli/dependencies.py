# graphregistry/entrypoints/cli/dependencies.py
"""Centralized dependency builders for CLI handlers and scripts.

These builders wire adapters (MySQL repositories, GraphAI/GenAI gateways) with
application operations while keeping the wiring logic in one place.  They are
entrypoint concerns: they know about concrete adapters so the handlers and
scripts don't have to repeat that knowledge.
"""
from __future__ import annotations

from typing import Any

from graphdb.core.graphdb import GraphDB

from graphregistry.adapters.gateways.genai.agt_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_lecturerepo import MySQLLectureRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.common.config import GlobalConfig


def build_schema_resolver(*, engine_name: str, global_config: GlobalConfig) -> DefaultSchemaResolver:
    """Build the default schema resolver for a registry environment."""
    return DefaultSchemaResolver(engine_name=engine_name, glbcfg=global_config)


def build_node_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
    concept_detection_gateway: ConceptDetectionGateway | None = None,
) -> NodeOperations:
    """Build node operations wired to the MySQL repository."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    repo = MySQLNodeRepository(db=db, schema_resolver=schema_resolver)

    ai_gateways: dict[str, Any] = {}
    if concept_detection_gateway is not None:
        ai_gateways["concept_detection"] = concept_detection_gateway

    return NodeOperations(repo=repo, ai_gateways=ai_gateways)


def build_edge_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
) -> EdgeOperations:
    """Build edge operations wired to the MySQL repository."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    repo = MySQLEdgeRepository(db=db, schema_resolver=schema_resolver)
    return EdgeOperations(repo=repo)


def build_lecture_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
    include_video_gateway: bool = True,
    include_concept_gateway: bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
    """Build lecture operations wired to the MySQL repository and AI gateways."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    node_repo = MySQLNodeRepository(db=db, schema_resolver=schema_resolver)
    lecture_repo = MySQLLectureRepository(
        db=db,
        schema_name=global_config.schema_lectures,
        node_repo=node_repo,
    )

    ai_gateways: dict[str, Any] = {}
    if include_video_gateway:
        ai_gateways["video_processing"] = GraphAIVideoGateway()
    if include_concept_gateway:
        ai_gateways["concept_detection"] = GraphAIConceptDetectionGateway()
    if include_enrichment_gateway:
        ai_gateways["lecture_enrichment"] = GenAILectureEnrichmentGateway()

    return LectureOperations(repo=lecture_repo, ai_gateways=ai_gateways)


def build_lecture_enrichment_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
) -> LectureOperations:
    """Build lecture operations for enrichment workflows (no video gateway)."""
    return build_lecture_operations(
        db=db,
        engine_name=engine_name,
        global_config=global_config,
        include_video_gateway=False,
    )


def build_registry_operations(
    *,
    db: GraphDB,
    engine_name: str,
    global_config: GlobalConfig,
) -> tuple[NodeOperations, EdgeOperations]:
    """Build node and edge operations sharing one schema resolver."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    node_repo = MySQLNodeRepository(db=db, schema_resolver=schema_resolver)
    edge_repo = MySQLEdgeRepository(db=db, schema_resolver=schema_resolver)
    return NodeOperations(repo=node_repo), EdgeOperations(repo=edge_repo)


# ---------------------------------------------------------------------------
# CLI-specific helpers that read from the standard argparse context.
# ---------------------------------------------------------------------------

def build_node_operations_from_args(
    args,
    *,
    concept_detection_gateway: ConceptDetectionGateway | None = None,
) -> NodeOperations:
    """Build node operations from a CLI args namespace."""
    return build_node_operations(
        db=args.ctx.db,
        engine_name=args.env,
        global_config=args.ctx.global_config,
        concept_detection_gateway=concept_detection_gateway,
    )


def build_edge_operations_from_args(args) -> EdgeOperations:
    """Build edge operations from a CLI args namespace."""
    return build_edge_operations(
        db=args.ctx.db,
        engine_name=args.env,
        global_config=args.ctx.global_config,
    )


def build_node_operations_with_concept_detection_from_args(args) -> NodeOperations:
    """Build node operations with a GraphAI concept-detection gateway."""
    return build_node_operations(
        db=args.ctx.db,
        engine_name=args.env,
        global_config=args.ctx.global_config,
        concept_detection_gateway=GraphAIConceptDetectionGateway(),
    )


def build_registry_operations_from_args(args) -> tuple[NodeOperations, EdgeOperations]:
    """Build node and edge operations from a CLI args namespace, sharing one schema resolver."""
    return build_registry_operations(
        db=args.ctx.db,
        engine_name=args.env,
        global_config=args.ctx.global_config,
    )


def build_lecture_operations_from_args(
    args,
    *,
    include_video_gateway: bool = True,
    include_concept_gateway: bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
    """Build lecture operations from a CLI args namespace."""
    return build_lecture_operations(
        db=args.ctx.db,
        engine_name=args.env,
        global_config=args.ctx.global_config,
        include_video_gateway=include_video_gateway,
        include_concept_gateway=include_concept_gateway,
        include_enrichment_gateway=include_enrichment_gateway,
    )
