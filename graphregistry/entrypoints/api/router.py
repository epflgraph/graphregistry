# graphregistry/entrypoints/api/router.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Callable
from fastapi import APIRouter, Depends, Request
from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.policies.pol_graphunits import GraphUnitsValidator
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.common.config import APIConfig, GlobalConfig
from graphregistry.domain.models.entities.mdl_base import EdgeKey, EdgeKeyList, NodeKey, NodeKeyList
from graphregistry.adapters.gateways.graphai.gtw_conceptdet import GraphAIConceptDetectionGateway
from graphregistry.application.factories.fct_node import NodeFactory
from graphregistry.entrypoints.dependencies import build_uow_factory
from graphregistry.entrypoints.mappers import SpecMapper
import graphregistry.entrypoints.api.schemas as apispecs

# Environment variables
API_ENV_VAR = "GRAPHREGISTRY_API_ENV"
DEFAULT_API_ENV = "xaas_coresrv"

# Router object
router = APIRouter(
    prefix="/api",
    responses={404: {"description": "Not found"}},
)

#=================#
# Helper methods  #
#=================#

def _get_api_env() -> str:
    """
    Return the single environment used by this API instance.

    The API should expose one configured registry environment, not let every
    client choose the environment per request.
    """
    return os.getenv(API_ENV_VAR, DEFAULT_API_ENV)


def _get_uow_factory(request: Request) -> Callable[[], UnitOfWork]:
    """Build a UnitOfWork factory backed by the process-scoped GraphDB client."""
    return build_uow_factory(db=request.app.state.db, engine_name=_get_api_env())


def _make_node_ops(uow_factory: Callable[[], UnitOfWork]) -> NodeOperations:
    """Build node operations wired to a UnitOfWork factory."""
    return NodeOperations(uow_factory=uow_factory)


def _make_edge_ops(uow_factory: Callable[[], UnitOfWork]) -> EdgeOperations:
    """Build edge operations wired to a UnitOfWork factory."""
    return EdgeOperations(uow_factory=uow_factory)

#-------------------------#
# API config / validation #
#-------------------------#

_api_config: APIConfig | None = None


def _get_api_config() -> APIConfig:
    """Lazy-load the API configuration once per process."""
    global _api_config
    if _api_config is None:
        _api_config = APIConfig()
    return _api_config


def get_graph_units_validator() -> GraphUnitsValidator:
    """Build the graph-units validator from the API configuration."""
    cfg = _get_api_config()
    return GraphUnitsValidator(
        allowed_node_types=cfg.allowed_node_types,
        allowed_edge_tuples=cfg.allowed_edge_tuples,
    )

#======================#
# FastAPI dependencies #
#======================#

def get_node_ops(uow_factory: Callable[[], UnitOfWork] = Depends(_get_uow_factory)) -> NodeOperations:
    """
    Build NodeOperations for one request.

    Tests can override `_get_uow_factory` via `app.dependency_overrides` to
    inject a fake UnitOfWork.
    """
    return _make_node_ops(uow_factory)

def get_edge_ops(uow_factory: Callable[[], UnitOfWork] = Depends(_get_uow_factory)) -> EdgeOperations:
    """
    Build EdgeOperations for one request.

    Tests can override `_get_uow_factory` via `app.dependency_overrides` to
    inject a fake UnitOfWork.
    """
    return _make_edge_ops(uow_factory)

def get_node_factory() -> NodeFactory:
    """
    Build NodeFactory for one request.
    """
    gtw = GraphAIConceptDetectionGateway(debug=True)
    return NodeFactory(concept_gateway=gtw)

@dataclass(frozen=True)
class RegistryOps:
    """
    Operations that share one UnitOfWork.

    Useful for operations that need to coordinate node and edge persistence
    atomically in the same request.
    """
    node_ops: NodeOperations
    edge_ops: EdgeOperations

def get_registry_ops(uow_factory: Callable[[], UnitOfWork] = Depends(_get_uow_factory)) -> RegistryOps:
    """
    Build node and edge operations sharing the same UnitOfWork factory.
    """
    return RegistryOps(
        node_ops=_make_node_ops(uow_factory),
        edge_ops=_make_edge_ops(uow_factory),
    )

#==================#
# System endpoints #
#==================#

@router.get("", response_model=apispecs.StatusResponse, tags=["system"])
def registry_status() -> apispecs.StatusResponse:
    """
    Check that the registry API is reachable.
    """
    return apispecs.StatusResponse(
        success=True,
        message=(
            "GraphRegistry API ready. "
            f"Environment: {_get_api_env()}. "
            "Open /docs for the Swagger UI."
        ),
    )

#=====================================#
# API Endpoint Group: Node operations #
#=====================================#

#-------------------------------#
# API Endpoint: /api/nodes/list #
#-------------------------------#
@router.post("/nodes/list", response_model=apispecs.APINodesListResponse, tags=["nodes"])
def nodes_list(request: apispecs.APINodesListRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesListResponse:
    """
    List existing nodes for one object type.
    """
    # Fetch list of nodes from the database for the given object type and optional id pattern
    rows = node_ops.list(object_type=request.type, id_pattern=request.id_pattern)

    # Convert list of tuples returned by the repository to list of NodeKey objects
    node_keys = [NodeKey.from_tuple(tuple(row)) for row in rows]

    # Convert NodeKey objects to the spec format (type/id) for the response
    node_key_specs = [SpecMapper.to_node_key_spec(key) for key in node_keys]

    # Return the list of node key specs and the count in the response
    return apispecs.APINodesListResponse(nodes=node_key_specs, count=len(node_key_specs))

#---------------------------------#
# API Endpoint: /api/nodes/exists #
#---------------------------------#
@router.post("/nodes/exists", response_model=apispecs.APINodesExistsResponse, tags=["nodes"])
def nodes_exists(request: apispecs.APINodesExistsRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesExistsResponse:
    """
    Check whether one node exists.
    """
    # Check if the node exists in the database by key
    exists = node_ops.exists(SpecMapper.from_node_key_spec(request.key))

    # Return the existence result in the response
    return apispecs.APINodesExistsResponse(exists=exists)

#--------------------------------------#
# API Endpoint: /api/nodes/exists_many #
# -------------------------------------#
@router.post("/nodes/exists_many", response_model=apispecs.APINodesExistsManyResponse, tags=["nodes"])
def nodes_exists_many(request: apispecs.APINodesExistsManyRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesExistsManyResponse:
    """
    Check whether a list of nodes exist.
    """
    # Covert the API request key list to a list of domain model node keys
    node_key_list = SpecMapper.from_node_key_list_spec(request.key_list)

    # Check if the nodes exist in the database by key, and get list of boolean results for each key
    exist_keys = node_ops.exists_many(node_key_list)

    # Return the existence results in the response, including list of individual results and count
    return apispecs.APINodesExistsManyResponse(exist_keys=exist_keys, count=len(exist_keys))

#------------------------------#
# API Endpoint: /api/nodes/get #
#------------------------------#
@router.post("/nodes/get", response_model=apispecs.APINodesGetResponse, response_model_exclude_none=True, tags=["nodes"])
def nodes_get(request: apispecs.APINodesGetRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesGetResponse:
    """
    Get one node by key.
    """
    # Fetch the node from the database by key
    node = node_ops.get(SpecMapper.from_node_key_spec(request.key))

    # If returned node is None, the node was not found
    if node is None:
        return apispecs.APINodesGetResponse(found=False, node=None)

    # Get simplified dict representation of the node for the response
    node_spec = SpecMapper.to_node_spec(node)

    # Return the node data in the response
    return apispecs.APINodesGetResponse(found=True, node=node_spec.model_dump(exclude_none=True))

#-----------------------------------#
# API Endpoint: /api/nodes/get_many #
#-----------------------------------#
@router.post("/nodes/get_many", response_model=apispecs.APINodesGetManyResponse, tags=["nodes"])
def nodes_get_many(request: apispecs.APINodesGetManyRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesGetManyResponse:
    """
    Get a list of nodes by key.
    """
    # Covert the API request key list to a list of domain model node keys
    node_key_list = SpecMapper.from_node_key_list_spec(request.key_list)

    # Fetch the nodes from the database by key list
    node_list = node_ops.get_many(node_key_list)

    # Get simplified dict representation of the nodes for the response
    node_list_spec = SpecMapper.to_node_list_spec(node_list)

    # Return the node data in the response, including list of found keys,
    # list of node data (or None if not found), and count
    return apispecs.APINodesGetManyResponse(
        found_keys = [node is not None for node in node_list_spec.item_list],
        nodes = [node.model_dump(exclude_none=True) if node is not None else None for node in node_list_spec.item_list],
        count = len(node_list_spec.item_list)
    )

#-------------------------------#
# API Endpoint: /api/nodes/save #
#-------------------------------#
@router.post("/nodes/save", response_model=apispecs.APINodesSaveResponse, tags=["nodes"])
def nodes_save(
    request: apispecs.APINodesSaveRequest,
    node_ops: NodeOperations = Depends(get_node_ops),
    validator: GraphUnitsValidator = Depends(get_graph_units_validator),
) -> apispecs.APINodesSaveResponse:
    """
    Save one node.
    """
    # Convert the API request to a domain model node
    node = SpecMapper.from_node_spec(request.node)

    # Enforce the configured allow-list before persisting
    validator.validate_node(node)

    # Save the node and return the saved node object
    saved_node = node_ops.save(node, actions=('commit',))

    # Get saved key (spec format)
    saved_key_spec = SpecMapper.to_node_key_spec(saved_node.key)

    # Return the saved node key in the response
    return apispecs.APINodesSaveResponse(
        success   = True,
        saved_key = saved_key_spec.model_dump()
    )

#------------------------------------#
# API Endpoint: /api/nodes/save_many #
#------------------------------------#
@router.post("/nodes/save_many", response_model=apispecs.APINodesSaveManyResponse, tags=["nodes"])
def nodes_save_many(
    request: apispecs.APINodesSaveManyRequest,
    node_ops: NodeOperations = Depends(get_node_ops),
    validator: GraphUnitsValidator = Depends(get_graph_units_validator),
) -> apispecs.APINodesSaveManyResponse:
    """
    Save a list of nodes.
    """
    # Convert the API request to a domain model node list
    node_list = SpecMapper.from_node_list_spec(request.node_list)

    # Enforce the configured allow-list before persisting
    validator.validate_nodes(node_list)

    # Save the nodes and return the saved node objects
    saved_nodes = node_ops.save_many(node_list, actions=('commit',))

    # Get saved keys (spec format)
    saved_keys_spec = SpecMapper.to_node_key_list_spec(NodeKeyList(item_list=[n.key for n in saved_nodes.item_list]))

    # Return the saved node keys in the response
    return apispecs.APINodesSaveManyResponse(
        success    = True,
        saved_keys = [saved_key.model_dump() for saved_key in saved_keys_spec.item_list],
        count      = len(saved_nodes.item_list)
    )

#---------------------------------#
# API Endpoint: /api/nodes/delete #
#---------------------------------#
@router.post("/nodes/delete", response_model=apispecs.APINodesDeleteResponse, tags=["nodes"])
def nodes_delete(request: apispecs.APINodesDeleteRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesDeleteResponse:
    """
    Delete one node.
    """
    # Get node key from the API request
    node_key = SpecMapper.from_node_key_spec(request.key)

    # Delete the node from the database by key
    deleted = node_ops.delete(node_key, actions=('commit',))

    # Return the deletion result in the response
    return apispecs.APINodesDeleteResponse(success=bool(deleted))

#--------------------------------------#
# API Endpoint: /api/nodes/delete_many #
#--------------------------------------#
@router.post("/nodes/delete_many", response_model=apispecs.APINodesDeleteManyResponse, tags=["nodes"])
def nodes_delete_many(request: apispecs.APINodesDeleteManyRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> apispecs.APINodesDeleteManyResponse:
    """
    Delete a list of nodes.
    """
    # Convert the API request key list to a list of domain model node keys
    node_key_list = SpecMapper.from_node_key_list_spec(request.key_list)

    # Delete the nodes from the database by key list
    raw_results = node_ops.delete_many(node_key_list, actions=('commit',))

    # Convert raw results to boolean (in case the repository returns other types of results)
    bool_results = [bool(result) for result in raw_results]

    # Return the deletion results in the response, including overall success,
    # list of individual results, and count of deleted nodes
    return apispecs.APINodesDeleteManyResponse(
        success   = all(bool_results) if bool_results else True,
        results   = bool_results,
        n_deleted = sum(bool_results)
    )

#=====================================#
# API Endpoint Group: Edge operations #
#=====================================#

#-------------------------------#
# API Endpoint: /api/edges/list #
#-------------------------------#
@router.post("/edges/list", response_model=apispecs.APIEdgesListResponse, tags=["edges"])
def edges_list(request: apispecs.APIEdgesListRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesListResponse:
    """
    List existing edges for one pair of object types.
    """
    # Fetch list of edges from the database for the given pair of object types and optional id pattern
    rows = edge_ops.list(object_type=(request.from_type, request.to_type), id_pattern=request.id_pattern)

    # Convert list of tuples returned by the repository to list of EdgeKey objects
    edge_keys = [EdgeKey.from_tuple(tuple(row)) for row in rows]

    # Convert EdgeKey objects to the spec format (from_type/from_id/to_type/to_id/context)
    edge_key_specs = [SpecMapper.to_edge_key_spec(key) for key in edge_keys]

    # Return the list of edge key specs and the count in the response
    return apispecs.APIEdgesListResponse(edges=edge_key_specs, count=len(edge_key_specs))

#---------------------------------#
# API Endpoint: /api/edges/exists #
#---------------------------------#
@router.post("/edges/exists", response_model=apispecs.APIEdgesExistsResponse, tags=["edges"])
def edges_exists(request: apispecs.APIEdgesExistsRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesExistsResponse:
    """
    Check whether one edge exists.
    """
    # Check if the edge exists in the database by key
    exists = edge_ops.exists(SpecMapper.from_edge_key_spec(request.key))

    # Return the existence result in the response
    return apispecs.APIEdgesExistsResponse(exists=exists)

#--------------------------------------#
# API Endpoint: /api/edges/exists_many #
# -------------------------------------#
@router.post("/edges/exists_many", response_model=apispecs.APIEdgesExistsManyResponse, tags=["edges"])
def edges_exists_many(request: apispecs.APIEdgesExistsManyRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesExistsManyResponse:
    """
    Check whether a list of edges exist.
    """
    # Covert the API request key list to a list of domain model edge keys
    edge_key_list = SpecMapper.from_edge_key_list_spec(request.key_list)

    # Check if the edges exist in the database by key, and get list of boolean results for each key
    exist_keys = edge_ops.exists_many(edge_key_list)

    # Return the existence results in the response, including list of individual results and count
    return apispecs.APIEdgesExistsManyResponse(exist_keys=exist_keys, count=len(exist_keys))

#------------------------------#
# API Endpoint: /api/edges/get #
#------------------------------#
@router.post("/edges/get", response_model=apispecs.APIEdgesGetResponse, response_model_exclude_none=True, tags=["edges"])
def edges_get(request: apispecs.APIEdgesGetRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesGetResponse:
    """
    get one edge by key.
    """
    # Fetch the edge from the database by key
    edge = edge_ops.get(SpecMapper.from_edge_key_spec(request.key))

    # If returned edge is None, the edge was not found
    if edge is None:
        return apispecs.APIEdgesGetResponse(found=False, edge=None)

    # Get simplified dict representation of the edge for the response
    edge_spec = SpecMapper.to_edge_spec(edge)

    # Return the edge data in the response
    return apispecs.APIEdgesGetResponse(found=True, edge=edge_spec.model_dump(exclude_none=True))

#-----------------------------------#
# API Endpoint: /api/edges/get_many #
#-----------------------------------#
@router.post("/edges/get_many", response_model=apispecs.APIEdgesGetManyResponse, tags=["edges"])
def edges_get_many(request: apispecs.APIEdgesGetManyRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesGetManyResponse:
    """
    Get a list of edges by key.
    """
    # Covert the API request key list to a list of domain model edge keys
    edge_key_list = SpecMapper.from_edge_key_list_spec(request.key_list)

    # Fetch the edges from the database by key list
    edge_list = edge_ops.get_many(edge_key_list)

    # Get simplified dict representation of the edges for the response
    edge_list_spec = SpecMapper.to_edge_list_spec(edge_list)

    # Return the edge data in the response, including list of found keys,
    # list of edge data (or None if not found), and count
    return apispecs.APIEdgesGetManyResponse(
        found_keys = [edge is not None for edge in edge_list.item_list],
        edges = [edge.model_dump(exclude_none=True) if edge is not None else None for edge in edge_list_spec.item_list],
        count = len(edge_list_spec.item_list)
    )

#-------------------------------#
# API Endpoint: /api/edges/save #
#-------------------------------#
@router.post("/edges/save", response_model=apispecs.APIEdgesSaveResponse, tags=["edges"])
def edges_save(
    request: apispecs.APIEdgesSaveRequest,
    edge_ops: EdgeOperations = Depends(get_edge_ops),
    validator: GraphUnitsValidator = Depends(get_graph_units_validator),
) -> apispecs.APIEdgesSaveResponse:
    """
    Save one edge.
    """
    # Convert the API request to a domain model edge
    edge = SpecMapper.from_edge_spec(request.edge)

    # Enforce the configured allow-list before persisting
    validator.validate_edge(edge)

    # Save the edge and return the saved edge object
    saved_edge = edge_ops.save(edge, actions=('commit',))

    # Get saved key (spec format)
    saved_key_spec = SpecMapper.to_edge_key_spec(saved_edge.key)

    # Return the saved edge key in the response
    return apispecs.APIEdgesSaveResponse(
        success   = True,
        saved_key = saved_key_spec.model_dump()
    )

#------------------------------------#
# API Endpoint: /api/edges/save_many #
#------------------------------------#
@router.post("/edges/save_many", response_model=apispecs.APIEdgesSaveManyResponse, tags=["edges"])
def edges_save_many(
    request: apispecs.APIEdgesSaveManyRequest,
    edge_ops: EdgeOperations = Depends(get_edge_ops),
    validator: GraphUnitsValidator = Depends(get_graph_units_validator),
) -> apispecs.APIEdgesSaveManyResponse:
    """
    Save a list of edges.
    """
    # Convert the API request to a domain model edge list
    edge_list = SpecMapper.from_edge_list_spec(request.edge_list)

    # Enforce the configured allow-list before persisting
    validator.validate_edges(edge_list)

    # Save the edges and return the saved edge objects``
    saved_edges = edge_ops.save_many(edge_list, actions=('commit',))

    # Get saved keys (spec format)
    saved_keys_spec = SpecMapper.to_edge_key_list_spec(EdgeKeyList(item_list=[e.key for e in saved_edges.item_list]))

    # Return the saved edge keys in the response
    return apispecs.APIEdgesSaveManyResponse(
        success    = True,
        saved_keys = [saved_key.model_dump() for saved_key in saved_keys_spec.item_list],
        count      = len(saved_edges.item_list)
    )

#---------------------------------#
# API Endpoint: /api/edges/delete #
#---------------------------------#
@router.post("/edges/delete", response_model=apispecs.APIEdgesDeleteResponse, tags=["edges"])
def edges_delete(request: apispecs.APIEdgesDeleteRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesDeleteResponse:
    """
    Delete one edge.
    """
    # Get edge key from the API request
    edge_key = SpecMapper.from_edge_key_spec(request.key)

    # Delete the edge from the database by key
    deleted = edge_ops.delete(edge_key, actions=('commit',))

    # Return the deletion result in the response
    return apispecs.APIEdgesDeleteResponse(success=bool(deleted))

#--------------------------------------#
# API Endpoint: /api/edges/delete_many #
#--------------------------------------#
@router.post("/edges/delete_many", response_model=apispecs.APIEdgesDeleteManyResponse, tags=["edges"])
def edges_delete_many(request: apispecs.APIEdgesDeleteManyRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> apispecs.APIEdgesDeleteManyResponse:
    """
    Delete a list of edges.
    """
    # Covert the API request key list to a list of domain model edge keys
    edge_key_list = SpecMapper.from_edge_key_list_spec(request.key_list)

    # Delete the edges from the database by key list
    raw_results = edge_ops.delete_many(edge_key_list, actions=('commit',))

    # Convert raw results to boolean (in case the repository returns other types of results)
    bool_results = [bool(result) for result in raw_results]

    # Return the deletion results in the response, including overall success,
    # list of individual results, and count of deleted edges
    return apispecs.APIEdgesDeleteManyResponse(
        success   = all(bool_results) if bool_results else True,
        results   = bool_results,
        n_deleted = sum(bool_results)
    )

# #====================#
# # Subgraph endpoints #
# #====================#

# @router.post("/subgraphs/save", response_model=apispecs.SubGraphSaveResponse, tags=["subgraphs"])
# def save_subgraph(request: apispecs.SubGraphSaveRequest, ops: RegistryOps = Depends(get_registry_ops)) -> apispecs.SubGraphSaveResponse:
#     """
#     Save a subgraph: first nodes, then edges.

#     Node and edge operations share the same GraphDB client for this request.
#     """
#     actions = ('commit',)

#     saved_nodes = ops.node_ops.save_many(
#         request.subgraph.nodes,
#         actions=actions,
#     )

#     saved_edges = ops.edge_ops.save_many(
#         request.subgraph.edges,
#         actions=actions,
#     )

#     return apispecs.SubGraphSaveResponse(
#         success=True,
#         nodes_saved=len(saved_nodes),
#         edges_saved=len(saved_edges),
#         subgraph=request.subgraph,
#     )
