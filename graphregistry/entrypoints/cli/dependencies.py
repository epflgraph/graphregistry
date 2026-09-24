# graphregistry/entrypoints/cli/dependencies.py
"""Centralized dependency builders for the Typer CLI.

These builders wire adapters (MySQL UnitOfWork, GraphAI/GenAI gateways) with
application operations while keeping the wiring logic in one place. They are
entrypoint concerns: they know about concrete adapters so the handlers don't
have to repeat that knowledge.
"""
from __future__ import annotations
from typing import Callable
from graphdb.core.graphdb import GraphDB
from graphregistry.adapters.gateways.genai.gtw_lectureenrich import GenAILectureEnrichmentGateway
from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_cacheprojection import MySQLCacheProjectionRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_changetracking import MySQLChangeTrackingRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexbuildup import MySQLIndexBuildupRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_formula import MySQLFormulaRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdoclink import MySQLIndexDocLinkRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexintegrity import MySQLIndexIntegrityRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdocs import MySQLIndexDocRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_lecturerepo import MySQLLectureRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_pageprofile import MySQLPageProfileRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_scoresmatrix import MySQLScoresMatrixRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_searchindexexport import MySQLSearchIndexExportRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_typeflags import MySQLTypeFlagsRepository
from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.application.operations.ops_indexpatch import IndexPatchOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.repositories.prt_cacheprojection import CacheProjectionRepository
from graphregistry.application.ports.repositories.prt_changetracking import ChangeTrackingRepository
from graphregistry.application.ports.repositories.prt_indexbuildup import IndexBuildupRepository
from graphregistry.application.ports.repositories.prt_formula import FormulaRepository
from graphregistry.application.ports.repositories.prt_indexdoclink import IndexDocLinkRepository
from graphregistry.application.ports.repositories.prt_indexintegrity import IndexIntegrityRepository
from graphregistry.application.ports.repositories.prt_indexdocs import IndexDocRepository
from graphregistry.application.ports.repositories.prt_pageprofile import PageProfileRepository
from graphregistry.application.ports.repositories.prt_scoresmatrix import ScoresMatrixRepository
from graphregistry.application.ports.repositories.prt_searchindexexport import SearchIndexExportRepository
from graphregistry.application.ports.repositories.prt_typeflags import TypeFlagsRepository
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.common.config import GlobalConfig, IndexConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.entrypoints.cli.context import CLIContext
from graphregistry.entrypoints.dependencies import build_uow_factory

#================================================================#
# Function Group: Schema resolver builders                       #
#================================================================#

# Public Method: Build the default schema resolver for a registry environment.
def build_schema_resolver(*, engine_name: str, global_config: GlobalConfig) -> DefaultSchemaResolver:
    """Build the default schema resolver for a registry environment."""
    return DefaultSchemaResolver(engine_name=engine_name, glbcfg=global_config)

#================================================================#
# Function Group: Repository builders                            #
#================================================================#

# Public Method: Build the typeflags repository wired to the CLI database client.
def build_typeflags_repository(*, db: GraphDB, global_config: GlobalConfig, engine_name: str = "coresrv", verbose: bool = False) -> TypeFlagsRepository:
    """Build the typeflags repository wired to the CLI database client.

    The engine defaults to 'coresrv', the engine the legacy airflow pipeline
    uses for all typeflag queries.
    """
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLTypeFlagsRepository(db=db, schema_resolver=schema_resolver, verbose=verbose)

# Public Method: Build the change-tracking repository wired to the CLI database client.
def build_change_tracking_repository(*, db: GraphDB, global_config: GlobalConfig, engine_name: str = "coresrv", verbose: bool = False) -> ChangeTrackingRepository:
    """Build the change-tracking repository wired to the CLI database client.

    The engine defaults to 'coresrv', the engine the legacy airflow pipeline
    uses for all tracking queries.
    """
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLChangeTrackingRepository(db=db, schema_resolver=schema_resolver, global_config=global_config, verbose=verbose)

# Public Method: Build the cache projection repository wired to the CLI database client.
def build_cacheprojection_repository(*, db: GraphDB, global_config: GlobalConfig, index_config: IndexConfig, engine_name: str = "coresrv", verbose: bool = False) -> CacheProjectionRepository:
    """Build the cache projection repository wired to the CLI database client.

    The engine defaults to 'coresrv', the engine the legacy pipeline uses for
    all cache projection queries.
    """
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLCacheProjectionRepository(db=db, schema_resolver=schema_resolver, global_config=global_config, index_config=index_config, verbose=verbose)

# Public Method: Build the scores matrix repository wired to the CLI database client.
def build_scoresmatrix_repository(*, db: GraphDB, global_config: GlobalConfig, scores_config: ScoresConfig, engine_name: str = "coresrv", verbose: bool = False) -> ScoresMatrixRepository:
    """Build the scores matrix repository wired to the CLI database client.

    The engine defaults to 'coresrv', the engine the legacy cache management
    uses for all scores matrix queries.
    """
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLScoresMatrixRepository(db=db, schema_resolver=schema_resolver, global_config=global_config, scores_config=scores_config, verbose=verbose)

# Public Method: Build the index patch operation wired to the CLI database
# client, composing the page-profile, docs, and doc-link repositories.
def build_index_patch_operations(*, db: GraphDB, global_config: GlobalConfig, index_config: IndexConfig, scores_config: ScoresConfig, engine_name: str = "coresrv", verbose: bool = False) -> IndexPatchOperations:
    """Build the index patch operation wired to the CLI database client."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return IndexPatchOperations(
        pageprofile_repo = MySQLPageProfileRepository(db=db, schema_resolver=schema_resolver, verbose=verbose),
        docs_repo        = MySQLIndexDocRepository(db=db, schema_resolver=schema_resolver, index_config=index_config, verbose=verbose),
        doclinks_repo    = MySQLIndexDocLinkRepository(db=db, schema_resolver=schema_resolver, index_config=index_config, scores_config=scores_config, verbose=verbose),
        buildup_repo     = MySQLIndexBuildupRepository(db=db, schema_resolver=schema_resolver, index_config=index_config, verbose=verbose),
        index_config     = index_config,
        scores_config    = scores_config,
    )

# Public Method: Build the index integrity repository wired to the CLI
# database client.
def build_index_integrity_repository(*, db: GraphDB, global_config: GlobalConfig, engine_name: str = "coresrv", verbose: bool = False) -> IndexIntegrityRepository:
    """Build the index integrity repository wired to the CLI database client."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLIndexIntegrityRepository(db=db, schema_resolver=schema_resolver, global_config=global_config, verbose=verbose)

# Public Method: Build the search index export repository wired to the CLI
# database client and the configured export path.
def build_searchindex_export_repository(*, db: GraphDB, global_config: GlobalConfig, index_config: IndexConfig, engine_name: str = "coresrv", verbose: bool = False) -> SearchIndexExportRepository:
    """Build the search index export repository wired to the CLI database
    client and the configured export path."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    export_path = global_config.settings.get("elasticsearch", {}).get("data_export_path", "exports/elasticsearch")
    return MySQLSearchIndexExportRepository(db=db, schema_resolver=schema_resolver, index_config=index_config, export_path=export_path, verbose=verbose)

# Public Method: Build the formula repository wired to the CLI database
# client.
def build_formula_repository(*, db: GraphDB, global_config: GlobalConfig, engine_name: str = "coresrv", verbose: bool = False) -> FormulaRepository:
    """Build the formula repository wired to the CLI database client."""
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)
    return MySQLFormulaRepository(db=db, schema_resolver=schema_resolver, global_config=global_config, verbose=verbose)

#================================================================#
# Function Group: Processing scope builders                      #
#================================================================#

# Public Method: Build the processing scope from the active typeflags, optionally
# restricted to the indexable edge families of the index configuration.
def build_processing_scope(*, db: GraphDB, global_config: GlobalConfig, index_config: IndexConfig, restrict_to_indexable: bool = False) -> ProcessingScope:
    """Build the processing scope from the active typeflags.

    The restriction mirrors the legacy get_types_to_process intersection and
    must only be applied to checksum updates, which the legacy command scoped
    to the indexable edge families; refresh and expiration use the raw flags.
    """
    typeflags_repo = build_typeflags_repository(db=db, global_config=global_config)
    flags = typeflags_repo.load()
    indexable_edge_pairs = None
    if restrict_to_indexable:

        # The index configuration keys the indexable edge families as sorted
        # (from, to) tuples.
        indexable_edge_pairs = [
            EdgeTypePair.from_tuple(key)
            for key in index_config.settings.get("edge_selection_contexts", {})
        ]
    return ProcessingScope.from_type_flag_config(flags, indexable_edge_pairs=indexable_edge_pairs)

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

# Public Method: Build lecture operations wired to MySQL repository and AI gateways #
def build_lecture_operations(
    *,
    db                        : GraphDB,
    engine_name               : str,
    global_config             : GlobalConfig,
    include_video_gateway     : bool = True,
    include_concept_gateway   : bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
#---------------------------------------------------------------------------------------#
    """Build lecture operations wired to the MySQL repository and AI gateways."""

    # Build the schema resolver for the target registry environment.
    schema_resolver = build_schema_resolver(engine_name=engine_name, global_config=global_config)

    # The node repository is required by the lecture repository to resolve and persist.
    node_repo = MySQLNodeRepository(db=db, schema_resolver=schema_resolver)

    # The lecture repository needs the node repository to resolve and persist.
    lecture_repo = MySQLLectureRepository(
        db              = db,
        schema_resolver = schema_resolver,
        node_repo       = node_repo,
    )

    # Instantiate the requested AI gateways, using None for disabled features.
    video_processing_gateway   = GraphAIVideoGateway()     if include_video_gateway     else None
    concept_detection_gateway  = GraphAIConceptDetectionGateway() if include_concept_gateway   else None
    lecture_enrichment_gateway = GenAILectureEnrichmentGateway()  if include_enrichment_gateway else None

    # Assemble the lecture operations with all selected gateways.
    return LectureOperations(
        repo                       = lecture_repo,
        video_processing_gateway   = video_processing_gateway,
        concept_detection_gateway  = concept_detection_gateway,
        lecture_enrichment_gateway = lecture_enrichment_gateway,
    )

#================================================================#
# Function Group: CLI-specific builders from CLI context         #
#================================================================#

# Public Method: Build node operations from the CLI context.
def build_node_operations_from_cli(
    *,
    ctx: CLIContext,
    env: str,
    verbose: bool = False,
    concept_detection_gateway: ConceptDetectionGateway | None = None,
) -> NodeOperations:
    """Build node operations from the CLI context."""
    uow_factory = build_uow_factory(
        db          = ctx.db,
        engine_name = env,
        verbose     = verbose,
    )
    return build_node_operations(
        uow_factory               = uow_factory,
        concept_detection_gateway = concept_detection_gateway,
    )

# Public Method: Build edge operations from the CLI context.
def build_edge_operations_from_cli(*, ctx: CLIContext, env: str, verbose: bool = False) -> EdgeOperations:
    """Build edge operations from the CLI context."""
    uow_factory = build_uow_factory(
        db          = ctx.db,
        engine_name = env,
        verbose     = verbose,
    )
    return build_edge_operations(uow_factory=uow_factory)

# Public Method: Build node operations with a GraphAI concept-detection gateway.
def build_node_operations_with_concept_detection_from_cli(*, ctx: CLIContext, env: str, verbose: bool = False) -> NodeOperations:
    """Build node operations with a GraphAI concept-detection gateway."""
    uow_factory = build_uow_factory(
        db          = ctx.db,
        engine_name = env,
        verbose     = verbose,
    )
    return build_node_operations(
        uow_factory               = uow_factory,
        concept_detection_gateway = GraphAIConceptDetectionGateway(),
    )

# Public Method: Build node and edge operations from the CLI context.
def build_registry_operations_from_cli(*, ctx: CLIContext, env: str, verbose: bool = False) -> tuple[NodeOperations, EdgeOperations]:
    """Build node and edge operations from the CLI context, sharing one UnitOfWork factory."""
    uow_factory = build_uow_factory(
        db          = ctx.db,
        engine_name = env,
        verbose     = verbose,
    )
    return (
        build_node_operations(uow_factory=uow_factory),
        build_edge_operations(uow_factory=uow_factory),
    )

# Public Method: Build lecture operations from the CLI context.
def build_lecture_operations_from_cli(
    *,
    ctx: CLIContext,
    env: str,
    include_video_gateway     : bool = True,
    include_concept_gateway   : bool = True,
    include_enrichment_gateway: bool = True,
) -> LectureOperations:
    """Build lecture operations from the CLI context."""
    return build_lecture_operations(
        db                         = ctx.db,
        engine_name                = env,
        global_config              = ctx.global_config,
        include_video_gateway      = include_video_gateway,
        include_concept_gateway    = include_concept_gateway,
        include_enrichment_gateway = include_enrichment_gateway,
    )
