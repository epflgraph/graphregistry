# graphregistry/entrypoints/api/router.py
from __future__ import annotations

from typing import cast, NoReturn

from fastapi import APIRouter, HTTPException

from graphdb.core.config import GraphDBConfig
from graphdb.core.graphdb import GraphDB

from graphregistry.common.config import GlobalConfig
from graphregistry.adapters.services.schema.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.workflows.operations.entities.ops_node import NodeOperations
from graphregistry.workflows.operations.entities.ops_edge import EdgeOperations
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.repositories.rpo_edge import EdgeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKey, EdgeKey
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.domain.models.entities.mdl_edge import EdgeList

from graphregistry.entrypoints.api import schemas


#================#
# Router object  #
#================#

router = APIRouter(
    prefix="/registry",
    responses={404: {"description": "Not found"}},
)


#=================#
# Helper methods  #
#=================#

def _raise_api_error(message: str, exc: Exception, status_code: int = 500) -> NoReturn:
    """
    Convert internal exceptions into readable API errors.
    """
    raise HTTPException(
        status_code=status_code,
        detail=f"{message}: {type(exc).__name__}: {exc}",
    ) from exc


def _make_db() -> GraphDB:
    """
    Build the GraphDB client.
    """
    db_config = GraphDBConfig.from_file("config/config_db.yaml")
    return GraphDB(config=db_config)


def _make_schema_resolver(env: str) -> DefaultSchemaResolver:
    """
    Build the default schema resolver for one environment.
    """
    global_config = GlobalConfig()
    db_config = GraphDBConfig.from_file("config/config_db.yaml")

    if env not in db_config.environments:
        available_envs = ", ".join(db_config.environments.keys())
        raise ValueError(f"Unknown env '{env}'. Available environments: {available_envs}")

    return DefaultSchemaResolver(
        engine_name=env,
        glbcfg=global_config,
    )


def _make_node_repo(env: str, db: GraphDB | None = None) -> NodeRepository:
    """
    Build node repository for one request.
    """
    return MySQLNodeRepository(
        db=db if db is not None else _make_db(),
        schema_resolver=_make_schema_resolver(env),
    )


def _make_edge_repo(env: str, db: GraphDB | None = None) -> EdgeRepository:
    """
    Build edge repository for one request.
    """
    return MySQLEdgeRepository(
        db=db if db is not None else _make_db(),
        schema_resolver=_make_schema_resolver(env),
    )


def _actions_tuple(actions: list[schemas.ActionName] | None) -> tuple[schemas.ActionName, ...]:
    """
    Convert API action list into the tuple expected by repositories/operations.
    """
    return tuple(actions or ["eval"])


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
        message="GraphRegistry API ready. Open /docs for the Swagger UI.",
    )


#================#
# Node endpoints #
#================#

@router.post("/nodes/list", response_model=schemas.NodeListResponse, tags=["nodes"])
def list_nodes(request: schemas.NodeListRequest) -> schemas.NodeListResponse:
    """
    List existing nodes for one object type.
    """

    try:
        # Fetch input options
        env = request.env
        object_type = request.object_type
        id_pattern = request.id_pattern

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        rows = node_ops.list(
            object_type=object_type,
            id_pattern=id_pattern,
        )

        # Convert rows to keys
        node_keys = [
            NodeKey.from_tuple(cast(tuple[str, str, str], row))
            for row in rows
        ]

        # Return response
        return schemas.NodeListResponse(
            nodes=node_keys,
            count=len(node_keys),
        )

    except ValueError as exc:
        _raise_api_error("Invalid node list request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to list nodes", exc)


@router.post("/nodes/exists", response_model=schemas.NodeExistsResponse, tags=["nodes"])
def node_exists(request: schemas.NodeExistsAPIRequest) -> schemas.NodeExistsResponse:
    """
    Check whether one node exists.
    """

    try:
        # Fetch input options
        env = request.env
        node_key = request.key

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        exists = node_ops.exists(node_key)

        # Return response
        return schemas.NodeExistsResponse(exists=exists)

    except ValueError as exc:
        _raise_api_error("Invalid node exists request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to check node existence", exc)


@router.post(
    "/nodes/fetch",
    response_model=schemas.NodeFetchResponse,
    response_model_exclude_none=True,
    tags=["nodes"],
)
def fetch_node(request: schemas.NodeFetchRequest) -> schemas.NodeFetchResponse:
    """
    Fetch one node by key.
    """

    try:
        # Fetch input options
        env = request.env
        node_key = request.key

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        node = node_ops.get(node_key)

        # Return response
        return schemas.NodeFetchResponse(
            found=node is not None,
            node=node,
        )

    except ValueError as exc:
        _raise_api_error("Invalid node fetch request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to fetch node", exc)


@router.post("/nodes/save", response_model=schemas.NodeSaveAPIResponse, tags=["nodes"])
def save_node(request: schemas.NodeSaveAPIRequest) -> schemas.NodeSaveAPIResponse:
    """
    Save one node.

    The default action is `eval`, which is dry-run behavior.
    Add `commit` to persist.
    """

    try:
        # Fetch input options
        env = request.env
        node = request.node
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        saved_node = node_ops.save(
            node,
            actions=actions,
        )

        # Return response
        return schemas.NodeSaveAPIResponse(
            success=True,
            node=saved_node,
        )

    except ValueError as exc:
        _raise_api_error("Invalid node save request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to save node", exc)


@router.post("/nodes/save-many", response_model=schemas.NodeListSaveResponse, tags=["nodes"])
def save_node_list(request: schemas.NodeListSaveRequest) -> schemas.NodeListSaveResponse:
    """
    Save a list of nodes.
    """

    try:
        # Fetch input options
        env = request.env
        node_list = request.node_list
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        saved_nodes = node_ops.save_many(
            node_list,
            actions=actions,
        )

        # Convert saved nodes back into a NodeList
        saved_node_list = NodeList(item_list=saved_nodes)

        # Return response
        return schemas.NodeListSaveResponse(
            success=True,
            node_list=saved_node_list,
            count=len(saved_node_list.item_list),
        )

    except ValueError as exc:
        _raise_api_error("Invalid node list save request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to save node list", exc)


@router.post("/nodes/delete", response_model=schemas.NodeDeleteResponse, tags=["nodes"])
def delete_node(request: schemas.NodeDeleteAPIRequest) -> schemas.NodeDeleteResponse:
    """
    Delete one node.

    The default action is `eval`, which checks behavior without committing.
    Add `commit` to actually delete.
    """

    try:
        # Fetch input options
        env = request.env
        node_key = request.key
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        deleted = node_ops.delete(
            node_key,
            actions=actions,
        )

        # Return response
        return schemas.NodeDeleteResponse(
            success=bool(deleted),
        )

    except ValueError as exc:
        _raise_api_error("Invalid node delete request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to delete node", exc)


@router.post("/nodes/delete-many", response_model=schemas.NodeDeleteManyResponse, tags=["nodes"])
def delete_node_list(request: schemas.NodeDeleteManyRequest) -> schemas.NodeDeleteManyResponse:
    """
    Delete a list of nodes.
    """

    try:
        # Fetch input options
        env = request.env
        node_keys = request.keys
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        node_repo: NodeRepository = _make_node_repo(env)
        node_ops = NodeOperations(repo=node_repo)

        # Execute command
        bool_results = node_ops.delete_many(
            node_keys,
            actions=actions,
        )

        # Return response
        return schemas.NodeDeleteManyResponse(
            success=all(bool_results) if bool_results else True,
            results=bool_results,
            count=len(bool_results),
        )

    except ValueError as exc:
        _raise_api_error("Invalid node list delete request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to delete node list", exc)


#================#
# Edge endpoints #
#================#

@router.post("/edges/list", response_model=schemas.EdgeListResponse, tags=["edges"])
def list_edges(request: schemas.EdgeListRequest) -> schemas.EdgeListResponse:
    """
    List existing edges for one pair of object types.
    """

    try:
        # Fetch input options
        env = request.env
        from_object_type = request.from_object_type
        to_object_type = request.to_object_type
        id_pattern = request.id_pattern

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        rows = edge_ops.list(
            object_type=(from_object_type, to_object_type),
            id_pattern=id_pattern,
        )

        # Convert rows to keys
        edge_keys = [
            EdgeKey.from_tuple(cast(tuple[str, str, str, str, str, str, str], row))
            for row in rows
        ]

        # Return response
        return schemas.EdgeListResponse(
            edges=edge_keys,
            count=len(edge_keys),
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge list request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to list edges", exc)


@router.post("/edges/exists", response_model=schemas.EdgeExistsResponse, tags=["edges"])
def edge_exists(request: schemas.EdgeExistsAPIRequest) -> schemas.EdgeExistsResponse:
    """
    Check whether one edge exists.
    """

    try:
        # Fetch input options
        env = request.env
        edge_key = request.key

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        exists = edge_ops.exists(edge_key)

        # Return response
        return schemas.EdgeExistsResponse(exists=exists)

    except ValueError as exc:
        _raise_api_error("Invalid edge exists request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to check edge existence", exc)


@router.post(
    "/edges/fetch",
    response_model=schemas.EdgeFetchResponse,
    response_model_exclude_none=True,
    tags=["edges"],
)
def fetch_edge(request: schemas.EdgeFetchRequest) -> schemas.EdgeFetchResponse:
    """
    Fetch one edge by key.
    """

    try:
        # Fetch input options
        env = request.env
        edge_key = request.key

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        edge = edge_ops.get(edge_key)

        # Return response
        return schemas.EdgeFetchResponse(
            found=edge is not None,
            edge=edge,
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge fetch request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to fetch edge", exc)


@router.post("/edges/save", response_model=schemas.EdgeSaveAPIResponse, tags=["edges"])
def save_edge(request: schemas.EdgeSaveAPIRequest) -> schemas.EdgeSaveAPIResponse:
    """
    Save one edge.
    """

    try:
        # Fetch input options
        env = request.env
        edge = request.edge
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        saved_edge = edge_ops.save(
            edge,
            actions=actions,
        )

        # Return response
        return schemas.EdgeSaveAPIResponse(
            success=True,
            edge=saved_edge,
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge save request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to save edge", exc)


@router.post("/edges/save-many", response_model=schemas.EdgeListSaveResponse, tags=["edges"])
def save_edge_list(request: schemas.EdgeListSaveRequest) -> schemas.EdgeListSaveResponse:
    """
    Save a list of edges.
    """

    try:
        # Fetch input options
        env = request.env
        edge_list = request.edge_list
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        saved_edges = edge_ops.save_many(
            edge_list,
            actions=actions,
        )

        # Convert saved edges back into an EdgeList
        saved_edge_list = EdgeList(item_list=saved_edges)

        # Return response
        return schemas.EdgeListSaveResponse(
            success=True,
            edge_list=saved_edge_list,
            count=len(saved_edge_list.item_list),
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge list save request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to save edge list", exc)


@router.post("/edges/delete", response_model=schemas.EdgeDeleteResponse, tags=["edges"])
def delete_edge(request: schemas.EdgeDeleteAPIRequest) -> schemas.EdgeDeleteResponse:
    """
    Delete one edge.
    """

    try:
        # Fetch input options
        env = request.env
        edge_key = request.key
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        deleted = edge_ops.delete(
            edge_key,
            actions=actions,
        )

        # Return response
        return schemas.EdgeDeleteResponse(
            success=bool(deleted),
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge delete request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to delete edge", exc)


@router.post("/edges/delete-many", response_model=schemas.EdgeDeleteManyResponse, tags=["edges"])
def delete_edge_list(request: schemas.EdgeDeleteManyRequest) -> schemas.EdgeDeleteManyResponse:
    """
    Delete a list of edges.
    """

    try:
        # Fetch input options
        env = request.env
        edge_keys = request.keys
        actions = _actions_tuple(request.actions)

        # Build repository and operations
        edge_repo: EdgeRepository = _make_edge_repo(env)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Execute command
        bool_results = edge_ops.delete_many(
            edge_keys,
            actions=actions,
        )

        # Return response
        return schemas.EdgeDeleteManyResponse(
            success=all(bool_results) if bool_results else True,
            results=bool_results,
            count=len(bool_results),
        )

    except ValueError as exc:
        _raise_api_error("Invalid edge list delete request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to delete edge list", exc)


#====================#
# Subgraph endpoints #
#====================#

@router.post("/subgraphs/save", response_model=schemas.SubGraphSaveResponse, tags=["subgraphs"])
def save_subgraph(request: schemas.SubGraphSaveRequest) -> schemas.SubGraphSaveResponse:
    """
    Save a subgraph: first nodes, then edges.
    """

    try:
        # Fetch input options
        env = request.env
        subgraph = request.subgraph
        actions = _actions_tuple(request.actions)

        # Build shared database client
        db = _make_db()

        # Build repositories and operations
        node_repo: NodeRepository = _make_node_repo(env, db=db)
        edge_repo: EdgeRepository = _make_edge_repo(env, db=db)
        node_ops = NodeOperations(repo=node_repo)
        edge_ops = EdgeOperations(repo=edge_repo)

        # Save nodes first
        saved_nodes = node_ops.save_many(
            subgraph.nodes,
            actions=actions,
        )

        # Save edges second
        saved_edges = edge_ops.save_many(
            subgraph.edges,
            actions=actions,
        )

        # Return response
        return schemas.SubGraphSaveResponse(
            success=True,
            nodes_saved=len(saved_nodes),
            edges_saved=len(saved_edges),
            subgraph=subgraph,
        )

    except ValueError as exc:
        _raise_api_error("Invalid subgraph save request", exc, status_code=400)

    except Exception as exc:
        _raise_api_error("Failed to save subgraph", exc)
