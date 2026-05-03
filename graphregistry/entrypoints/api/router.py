# graphregistry/entrypoints/api/router.py
from __future__ import annotations
import os
from dataclasses import dataclass
from fastapi import APIRouter, Depends
from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.services.schema.asv_schema_default import DefaultSchemaResolver
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.entities.mdl_edge import EdgeList
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.entrypoints.api import schemas
from graphregistry.workflows.operations.entities.ops_edge import EdgeOperations
from graphregistry.workflows.operations.entities.ops_node import NodeOperations

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

def _make_db() -> GraphDB:
    """
    Build the GraphDB client.
    """
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)

def _make_schema_resolver() -> DefaultSchemaResolver:
    """
    Build the default schema resolver for the configured API environment.
    """
    env = _get_api_env()

    global_config = GlobalConfig()
    db_config = GraphDBConfig.from_file("config/config_db.yaml")

    if env not in db_config.environments:
        available_envs = ", ".join(db_config.environments.keys())
        raise ValueError(
            f"Unknown API env '{env}'. "
            f"Set {API_ENV_VAR} to one of: {available_envs}"
        )

    return DefaultSchemaResolver(
        engine_name=env,
        glbcfg=global_config,
    )

def _make_node_repo(db: GraphDB | None = None) -> NodeRepository:
    """
    Build node repository.
    """
    return MySQLNodeRepository(
        db=db if db is not None else _make_db(),
        schema_resolver=_make_schema_resolver(),
    )

def _make_edge_repo(db: GraphDB | None = None) -> EdgeRepository:
    """
    Build edge repository.
    """
    return MySQLEdgeRepository(
        db=db if db is not None else _make_db(),
        schema_resolver=_make_schema_resolver(),
    )

#======================#
# FastAPI dependencies #
#======================#

def get_node_ops() -> NodeOperations:
    """
    Build NodeOperations for one request.
    """
    node_repo: NodeRepository = _make_node_repo()
    return NodeOperations(repo=node_repo)

def get_edge_ops() -> EdgeOperations:
    """
    Build EdgeOperations for one request.
    """
    edge_repo: EdgeRepository = _make_edge_repo()
    return EdgeOperations(repo=edge_repo)

@dataclass(frozen=True)
class RegistryOps:
    """
    Operations that share one database client.

    Useful for operations that need to coordinate node and edge persistence
    in the same request.
    """
    node_ops: NodeOperations
    edge_ops: EdgeOperations

def get_registry_ops() -> RegistryOps:
    """
    Build node and edge operations sharing the same GraphDB client.
    """
    db = _make_db()

    node_repo: NodeRepository = _make_node_repo(db=db)
    edge_repo: EdgeRepository = _make_edge_repo(db=db)

    return RegistryOps(
        node_ops=NodeOperations(repo=node_repo),
        edge_ops=EdgeOperations(repo=edge_repo),
    )

#==================#
# System endpoints #
#==================#

@router.get("", response_model=schemas.StatusResponse, tags=["system"])
def registry_status() -> schemas.StatusResponse:
    """
    Check that the registry API is reachable.
    """
    return schemas.StatusResponse(
        success=True,
        message=(
            "GraphRegistry API ready. "
            f"Environment: {_get_api_env()}. "
            "Open /docs for the Swagger UI."
        ),
    )

#================#
# Node endpoints #
#================#

@router.post("/nodes/list", response_model=schemas.NodeListResponse, tags=["nodes"])
def list_nodes(request: schemas.NodeListRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeListResponse:
    """
    List existing nodes for one object type.
    """
    rows = node_ops.list(object_type=request.object_type, id_pattern=request.id_pattern)
    node_keys = [NodeKey.from_tuple(tuple(row)) for row in rows]
    return schemas.NodeListResponse(nodes=node_keys, count=len(node_keys))

@router.post("/nodes/exists", response_model=schemas.NodeExistsResponse, tags=["nodes"])
def node_exists(request: schemas.NodeExistsAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeExistsResponse:
    """
    Check whether one node exists.
    """
    exists = node_ops.exists(request.key)
    return schemas.NodeExistsResponse(exists=exists)

@router.post("/nodes/fetch", response_model=schemas.NodeFetchResponse, response_model_exclude_none=True, tags=["nodes"])
def fetch_node(request: schemas.NodeFetchRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeFetchResponse:
    """
    Fetch one node by key.
    """
    node = node_ops.get(request.key)
    return schemas.NodeFetchResponse(found=node is not None, node=node)

@router.post("/nodes/save", response_model=schemas.NodeSaveAPIResponse, tags=["nodes"])
def save_node(request: schemas.NodeSaveAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeSaveAPIResponse:
    """
    Save one node.
    """
    saved_node = node_ops.save(request.node, actions=('commit',))
    return schemas.NodeSaveAPIResponse(success=True, node=saved_node)

@router.post("/nodes/save-many", response_model=schemas.NodeListSaveResponse, tags=["nodes"])
def save_node_list(request: schemas.NodeListSaveRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeListSaveResponse:
    """
    Save a list of nodes.
    """
    saved_nodes = node_ops.save_many(request.node_list, actions=('commit',))
    saved_node_list = NodeList(item_list=saved_nodes)
    return schemas.NodeListSaveResponse(success=True, node_list=saved_node_list, count=len(saved_node_list.item_list))

@router.post("/nodes/delete", response_model=schemas.NodeDeleteResponse, tags=["nodes"])
def delete_node(request: schemas.NodeDeleteAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeDeleteResponse:
    """
    Delete one node.
    """
    deleted = node_ops.delete(request.key, actions=('commit',))
    return schemas.NodeDeleteResponse(success=bool(deleted))

@router.post("/nodes/delete-many", response_model=schemas.NodeDeleteManyResponse, tags=["nodes"])
def delete_node_list(request: schemas.NodeDeleteManyRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeDeleteManyResponse:
    """
    Delete a list of nodes.
    """
    bool_results = node_ops.delete_many(request.keys, actions=('commit',))
    return schemas.NodeDeleteManyResponse(success=all(bool_results) if bool_results else True, results=bool_results, count=len(bool_results))

#================#
# Edge endpoints #
#================#

@router.post("/edges/list", response_model=schemas.EdgeListResponse, tags=["edges"])
def list_edges(request: schemas.EdgeListRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeListResponse:
    """
    List existing edges for one pair of object types.
    """
    rows = edge_ops.list(object_type=(request.from_object_type, request.to_object_type), id_pattern=request.id_pattern)
    edge_keys = [EdgeKey.from_tuple(tuple(row)) for row in rows]
    return schemas.EdgeListResponse(edges=edge_keys, count=len(edge_keys))

@router.post("/edges/exists", response_model=schemas.EdgeExistsResponse, tags=["edges"])
def edge_exists(request: schemas.EdgeExistsAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeExistsResponse:
    """
    Check whether one edge exists.
    """
    exists = edge_ops.exists(request.key)
    return schemas.EdgeExistsResponse(exists=exists)

@router.post("/edges/fetch", response_model=schemas.EdgeFetchResponse, response_model_exclude_none=True, tags=["edges"])
def fetch_edge(request: schemas.EdgeFetchRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeFetchResponse:
    """
    Fetch one edge by key.
    """
    edge = edge_ops.get(request.key)
    return schemas.EdgeFetchResponse(found=edge is not None, edge=edge)

@router.post("/edges/save", response_model=schemas.EdgeSaveAPIResponse, tags=["edges"])
def save_edge(request: schemas.EdgeSaveAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeSaveAPIResponse:
    """
    Save one edge.
    """
    saved_edge = edge_ops.save(request.edge, actions=('commit',))
    return schemas.EdgeSaveAPIResponse(success=True, edge=saved_edge)

@router.post("/edges/save-many", response_model=schemas.EdgeListSaveResponse, tags=["edges"])
def save_edge_list(request: schemas.EdgeListSaveRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeListSaveResponse:
    """
    Save a list of edges.
    """
    saved_edges = edge_ops.save_many(request.edge_list, actions=('commit',))
    saved_edge_list = EdgeList(item_list=saved_edges)
    return schemas.EdgeListSaveResponse(success=True, edge_list=saved_edge_list, count=len(saved_edge_list.item_list))

@router.post("/edges/delete", response_model=schemas.EdgeDeleteResponse, tags=["edges"])
def delete_edge(request: schemas.EdgeDeleteAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeDeleteResponse:
    """
    Delete one edge.
    """
    deleted = edge_ops.delete(request.key, actions=('commit',))
    return schemas.EdgeDeleteResponse(success=bool(deleted))

@router.post("/edges/delete-many", response_model=schemas.EdgeDeleteManyResponse, tags=["edges"])
def delete_edge_list(request: schemas.EdgeDeleteManyRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeDeleteManyResponse:
    """
    Delete a list of edges.
    """
    bool_results = edge_ops.delete_many(request.keys, actions=('commit',))
    return schemas.EdgeDeleteManyResponse(success=all(bool_results) if bool_results else True, results=bool_results, count=len(bool_results))

#====================#
# Subgraph endpoints #
#====================#

@router.post("/subgraphs/save", response_model=schemas.SubGraphSaveResponse, tags=["subgraphs"])
def save_subgraph(request: schemas.SubGraphSaveRequest, ops: RegistryOps = Depends(get_registry_ops)) -> schemas.SubGraphSaveResponse:
    """
    Save a subgraph: first nodes, then edges.

    Node and edge operations share the same GraphDB client for this request.
    """
    actions = ('commit',)

    saved_nodes = ops.node_ops.save_many(
        request.subgraph.nodes,
        actions=actions,
    )

    saved_edges = ops.edge_ops.save_many(
        request.subgraph.edges,
        actions=actions,
    )

    return schemas.SubGraphSaveResponse(
        success=True,
        nodes_saved=len(saved_nodes),
        edges_saved=len(saved_edges),
        subgraph=request.subgraph,
    )
