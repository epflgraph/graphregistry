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
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_edge import MySQLEdgeMapper
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptGateway
from graphregistry.workflows.factories.fct_node import NodeFactory

# Environment variables
API_ENV_VAR = "GRAPHREGISTRY_API_ENV"
DEFAULT_API_ENV = "xaas_coresrv"
INSTITUTION_ID = "EPFL"

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

def get_node_factory() -> NodeFactory:
    """
    Build NodeFactory for one request.
    """
    gtw = GraphAIConceptGateway(debug=True)
    return NodeFactory(concept_gateway=gtw)

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

#=====================================#
# API Endpoint Group: Node operations #
#=====================================#

#-------------------------------#
# API Endpoint: /api/nodes/list #
#-------------------------------#
@router.post("/nodes/list", response_model=schemas.NodeListResponse, tags=["nodes"])
def list_nodes(request: schemas.NodeListRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeListResponse:
    """
    List existing nodes for one object type.
    """
    # Fetch list of nodes from the database for the given object type and optional id pattern
    rows = node_ops.list(object_type=request.object_type, id_pattern=request.id_pattern)

    # Convert list of tuples returned by the repository to list of NodeKey objects for the response
    node_keys = [NodeKey.from_tuple(tuple(row)) for row in rows]

    # Return the list of node keys and the count in the response
    return schemas.NodeListResponse(nodes=node_keys, count=len(node_keys))

#---------------------------------#
# API Endpoint: /api/nodes/exists #
#---------------------------------#
@router.post("/nodes/exists", response_model=schemas.NodeExistsResponse, tags=["nodes"])
def node_exists(request: schemas.NodeExistsAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeExistsResponse:
    """
    Check whether one node exists.
    """
    # Check if the node exists in the database by key
    exists = node_ops.exists(
        NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = request.key.object_type,
            object_id      = request.key.object_id
        )
    )
    # Return the existence result in the response
    return schemas.NodeExistsResponse(exists=exists)

#------------------------------#
# API Endpoint: /api/nodes/get #
#------------------------------#
@router.post("/nodes/get", response_model=schemas.NodeGetResponse, response_model_exclude_none=True, tags=["nodes"])
def get_node(request: schemas.NodeGetRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeGetResponse:
    """
    Get one node by key.
    """
    # Fetch the node from the database by key
    node = node_ops.get(
        NodeKey(
            institution_id = INSTITUTION_ID,
            object_type    = request.key.object_type,
            object_id      = request.key.object_id
        )
    )
    # If returned node is None, the node was not found
    if node is None:
        return schemas.NodeGetResponse(found=False, node=None)

    # Get simplified dict representation of the node for the response
    json_output = MySQLNodeMapper.to_simplified_dict(node)

    # Drop concept list from the output, since it's not part of the response schema
    json_output.pop('detected_concepts', None)

    # Return the node data in the response
    return schemas.NodeGetResponse(found=True, node=json_output)

#-------------------------------#
# API Endpoint: /api/nodes/save #
#-------------------------------#
@router.post("/nodes/save", response_model=schemas.NodeSaveAPIResponse, tags=["nodes"])
def save_node(request: schemas.NodeSaveAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeSaveAPIResponse:
    """
    Save one node.
    """
    # Add institution_id to the node data, since it's not part of the input schema
    # (but required for the domain model and persistence)
    json_input = request.node.model_dump()
    json_input.update({'institution_id': INSTITUTION_ID})

    # Convert simplified input to domain model
    node = MySQLNodeMapper.from_simplified_dict(json_input)

    # Save the node and return the saved node object
    saved_node = node_ops.save(node, actions=('commit',))

    # Return the saved node key in the response
    return schemas.NodeSaveAPIResponse(
        success   = True,
        saved_key = {
            'object_type' : saved_node.key.object_type,
            'object_id'   : saved_node.key.object_id,
        }
    )

#------------------------------------#
# API Endpoint: /api/nodes/save_many #
#------------------------------------#
@router.post("/nodes/save_many", response_model=schemas.NodeListSaveResponse, tags=["nodes"])
def save_node_list(request: schemas.NodeListSaveRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeListSaveResponse:
    """
    Save a list of nodes.
    """
    # Initialise json_input list
    json_input_list = []

    # Convert each simplified input to domain model, and add institution_id to the node data
    for node in request.node_list:

        # Add institution_id to the node data, since it's not part of the input schema
        # (but required for the domain model and persistence)
        json_input = node.model_dump()
        json_input.update({'institution_id': INSTITUTION_ID})

        # Append json_input to the list
        json_input_list.append(json_input)

    # Convert simplified input to domain model
    node_list = MySQLNodeMapper.from_simplified_dict_list(json_input_list)

    # Save the nodes and return the saved node objects
    saved_nodes = node_ops.save_many(node_list, actions=('commit',))

    # Return the saved node keys in the response
    return schemas.NodeListSaveResponse(
        success    = True,
        saved_keys = [saved_node.key for saved_node in saved_nodes.item_list],
        count      = len(saved_nodes.item_list)
    )

#---------------------------------#
# API Endpoint: /api/nodes/delete #
#---------------------------------#
@router.post("/nodes/delete", response_model=schemas.NodeDeleteResponse, tags=["nodes"])
def delete_node(request: schemas.NodeDeleteAPIRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeDeleteResponse:
    """
    Delete one node.
    """
    # Delete the node from the database by key
    deleted = node_ops.delete(NodeKey(
        institution_id = INSTITUTION_ID,
        object_type    = request.key.object_type,
        object_id      = request.key.object_id
    ), actions=('commit',))

    # Return the deletion result in the response
    return schemas.NodeDeleteResponse(success=bool(deleted))

#--------------------------------------#
# API Endpoint: /api/nodes/delete_many #
#--------------------------------------#
@router.post("/nodes/delete_many", response_model=schemas.NodeDeleteManyResponse, tags=["nodes"])
def delete_node_list(request: schemas.NodeDeleteManyRequest, node_ops: NodeOperations = Depends(get_node_ops)) -> schemas.NodeDeleteManyResponse:
    """
    Delete a list of nodes.
    """
    # Delete the nodes from the database by key,
    # and get list of boolean results for each deletion
    bool_results = node_ops.delete_many([NodeKey(
        institution_id = INSTITUTION_ID,
        object_type    = key.object_type,
        object_id      = key.object_id
    ) for key in request.key_list], actions=('commit',))

    # Return the deletion results in the response, including overall success,
    # list of individual results, and count of deleted nodes
    return schemas.NodeDeleteManyResponse(
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
@router.post("/edges/list", response_model=schemas.EdgeListResponse, tags=["edges"])
def list_edges(request: schemas.EdgeListRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeListResponse:
    """
    List existing edges for one pair of object types.
    """
    # Fetch list of edges from the database for the given pair of object types and optional id pattern
    rows = edge_ops.list(object_type=(request.from_object_type, request.to_object_type), id_pattern=request.id_pattern)

    # Convert list of tuples returned by the repository to list of EdgeKey objects for the response
    edge_keys = [EdgeKey.from_tuple(tuple(row)) for row in rows]

    # Return the list of edge keys and the count in the response
    return schemas.EdgeListResponse(edges=edge_keys, count=len(edge_keys))

#---------------------------------#
# API Endpoint: /api/edges/exists #
#---------------------------------#
@router.post("/edges/exists", response_model=schemas.EdgeExistsResponse, tags=["edges"])
def edge_exists(request: schemas.EdgeExistsAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeExistsResponse:
    """
    Check whether one edge exists.
    """
    # Check if the edge exists in the database by key
    exists = edge_ops.exists(
        EdgeKey(
            from_institution_id = INSTITUTION_ID,
            from_object_type    = request.key.from_object_type,
            from_object_id      = request.key.from_object_id,
            to_institution_id   = INSTITUTION_ID,
            to_object_type      = request.key.to_object_type,
            to_object_id        = request.key.to_object_id,
            context             = request.key.context
        )
    )
    # Return the existence result in the response
    return schemas.EdgeExistsResponse(exists=exists)

#------------------------------#
# API Endpoint: /api/edges/get #
#------------------------------#
@router.post("/edges/get", response_model=schemas.EdgeGetResponse, response_model_exclude_none=True, tags=["edges"])
def get_edge(request: schemas.EdgeGetRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeGetResponse:
    """
    get one edge by key.
    """
    edge = edge_ops.get(
        EdgeKey(
            from_institution_id = INSTITUTION_ID,
            from_object_type    = request.key.from_object_type,
            from_object_id      = request.key.from_object_id,
            to_institution_id   = INSTITUTION_ID,
            to_object_type      = request.key.to_object_type,
            to_object_id        = request.key.to_object_id,
            context             = request.key.context
        )
    )
    # If returned edge is None, the edge was not found
    if edge is None:
        return schemas.EdgeGetResponse(found=False, edge=None)

    # Get simplified dict representation of the edge for the response
    json_output = MySQLEdgeMapper.to_simplified_dict(edge)

    # Return the edge data in the response
    return schemas.EdgeGetResponse(found=True, edge=json_output)

#-------------------------------#
# API Endpoint: /api/edges/save #
#-------------------------------#
@router.post("/edges/save", response_model=schemas.EdgeSaveAPIResponse, tags=["edges"])
def save_edge(request: schemas.EdgeSaveAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeSaveAPIResponse:
    """
    Save one edge.
    """
    # Add institution_id to the edge data, since it's not part of the input schema
    # (but required for the domain model and persistence)
    json_input = request.edge.model_dump()
    json_input.update({
        'from_institution_id' : INSTITUTION_ID,
        'to_institution_id'   : INSTITUTION_ID
    })

    # Convert simplified input to domain model
    edge = MySQLEdgeMapper.from_simplified_dict(json_input)

    # Save the edge using the edge operations
    saved_edge = edge_ops.save(edge, actions=('commit',))

    # Return the saved edge key in the response
    return schemas.EdgeSaveAPIResponse(
        success   = True,
        saved_key = {
            'from_object_type'    : saved_edge.key.from_object_type,
            'from_object_id'      : saved_edge.key.from_object_id,
            'to_object_type'      : saved_edge.key.to_object_type,
            'to_object_id'        : saved_edge.key.to_object_id,
            'context'             : saved_edge.key.context,
        }
    )

#------------------------------------#
# API Endpoint: /api/edges/save_many #
#------------------------------------#
@router.post("/edges/save_many", response_model=schemas.EdgeListSaveResponse, tags=["edges"])
def save_edge_list(request: schemas.EdgeListSaveRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeListSaveResponse:
    """
    Save a list of edges.
    """
    # Initialise json_input list
    json_input_list = []

    # Convert each simplified input to domain model, and add institution_id to the edge data
    for edge in request.edge_list:

        # Add institution_id to the edge data, since it's not part of the input schema
        # (but required for the domain model and persistence)
        json_input = edge.model_dump()
        json_input.update({
            'from_institution_id' : INSTITUTION_ID,
            'to_institution_id'   : INSTITUTION_ID
        })

        # Append json_input to the list
        json_input_list.append(json_input)

    # Convert simplified input to domain model
    edge_list = MySQLEdgeMapper.from_simplified_dict_list(json_input_list)

    # Save the edges and return the saved edge objects
    saved_edges = edge_ops.save_many(edge_list, actions=('commit',))

    # Return the saved edge keys in the response
    return schemas.EdgeListSaveResponse(
        success    = True,
        saved_keys = [saved_edge.key for saved_edge in saved_edges.item_list],
        count      = len(saved_edges.item_list)
    )

#---------------------------------#
# API Endpoint: /api/edges/delete #
#---------------------------------#
@router.post("/edges/delete", response_model=schemas.EdgeDeleteResponse, tags=["edges"])
def delete_edge(request: schemas.EdgeDeleteAPIRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeDeleteResponse:
    """
    Delete one edge.
    """
    # Delete the edge from the database by key
    deleted = edge_ops.delete(EdgeKey(
        from_institution_id = INSTITUTION_ID,
        from_object_type    = request.key.from_object_type,
        from_object_id      = request.key.from_object_id,
        to_institution_id   = INSTITUTION_ID,
        to_object_type      = request.key.to_object_type,
        to_object_id        = request.key.to_object_id,
        context             = request.key.context
    ), actions=('commit',))

    # Return the deletion result in the response
    return schemas.EdgeDeleteResponse(success=bool(deleted))

#--------------------------------------#
# API Endpoint: /api/edges/delete_many #
#--------------------------------------#
@router.post("/edges/delete_many", response_model=schemas.EdgeDeleteManyResponse, tags=["edges"])
def delete_edge_list(request: schemas.EdgeDeleteManyRequest, edge_ops: EdgeOperations = Depends(get_edge_ops)) -> schemas.EdgeDeleteManyResponse:
    """
    Delete a list of edges.
    """
    # Delete the edges from the database by key,
    # and get list of boolean results for each deletion
    bool_results = edge_ops.delete_many([EdgeKey(
        from_institution_id = INSTITUTION_ID,
        from_object_type    = key.from_object_type,
        from_object_id      = key.from_object_id,
        to_institution_id   = INSTITUTION_ID,
        to_object_type      = key.to_object_type,
        to_object_id        = key.to_object_id,
        context             = key.context
    ) for key in request.key_list], actions=('commit',))

    # Return the deletion results in the response, including overall success,
    # list of individual results, and count of deleted edges
    return schemas.EdgeDeleteManyResponse(
        success   = all(bool_results) if bool_results else True,
        results   = bool_results,
        n_deleted = sum(bool_results)
    )

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
