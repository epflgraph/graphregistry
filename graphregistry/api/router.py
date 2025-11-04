#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from graphregistry.clients.mysql import GraphDB
from graphregistry.core.registry import GraphRegistry
from graphregistry.core.dbbridge import RegistryDB
import graphregistry.api.schemas as schemas
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

# Initalise Registry db bridge
rbridge = RegistryDB()

# Initialize the GraphRegistry instance
gr = GraphRegistry()

# Initialize the API router
router = APIRouter(
    prefix="/registry",
    tags=["registry"],
    responses={404: {"description": "Not found"}}
)


@router.post("/insert")
def method_insert(request: schemas.InsertItemRequest):
    """
    Inserts multiple items into the registry.
    """
    print(
        f"Inserting {len(request.data)} items - Type: {request.type}, Update Existing: {request.update_existing},",
        f"Actions: {', '.join(request.actions)}"
    )
    if request.type == schemas.ItemType.nodes:
        obj_list = gr.NodeList()
    elif request.type == schemas.ItemType.edges:
        obj_list = gr.EdgeList()
    else:
        return JSONResponse(content={"message": "Invalid item type"})
    try:
        obj_list.set_from_json(request.data)
        # obj_list.info()  # too verbose
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to create the objects, the error was: {type(e).__name__}({exception_args})'
        )
    try:
        r = obj_list.commit(actions=request.actions)
        print(
            f"Inserted {len(request.data)} items of type {request.type}."
        )
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to commit the objects, the error was: {type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"{request.type} inserted successfully", "eval_results": r})


@router.post("/delete_nodes")
def method_delete_nodes(request: schemas.DeleteNodesRequest):
    """
    Deletes multiple nodes from the registry by ID.
    """
    print(
        f"Deleting {len(request.nodes_id)} nodes ({request.institution_id}, {request.object_type}), "
        f"Actions: {', '.join(request.actions)}"
    )
    try:
        r = rbridge.delete_nodes_by_ids(
            institution_id=request.institution_id, object_type=request.object_type, nodes_id=request.nodes_id,
            engine_name=request.engine_name, actions=request.actions,
        )
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to delete the nodes, the error was: {type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"Nodes deleted successfully", "eval_results": r})


@router.post("/delete_edges")
def method_delete_edges(request: schemas.DeleteEdgesRequest):
    """
    Deletes multiple edges from the registry by ID.
    """
    print(
        f"Deleting {len(request.items_id)} edges - Type: {request.type}, Actions: {', '.join(request.actions)}"
    )
    try:
        r = rbridge.delete_edges_by_ids(
            from_institution_id=request.from_institution_id, from_object_type=request.from_object_type,
            to_institution_id=request.to_institution_id, to_object_type=request.to_object_type,
            edges_id=request.edges_id, engine_name=request.engine_name, actions=request.actions,
        )
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to delete the edges, the error was: {type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"Edges deleted successfully", "eval_results": r})


@router.post("/list_nodes")
def method_list_nodes(request: schemas.ListNodesRequest):
    """
    List nodes existing in the registry.
    """
    try:
        objects_id = rbridge.get_existing_nodes_id(
            institution_id=request.institution_id, object_type=request.object_type,
            engine_name=request.engine_name
        )
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to list the nodes {request.institution_id} {request.object_type}, '
                   f'the error was: {type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"Nodes listed successfully", "nodes_id": objects_id})


@router.post("/list_edges")
def method_list_edges(request: schemas.ListEdgesRequest):
    """
    List edges existing in the registry.
    """
    try:
        objects_id = rbridge.get_existing_edges_id(
            from_institution_id=request.from_institution_id, from_object_type=request.from_object_type,
            to_institution_id=request.to_institution_id, to_object_type=request.to_object_type,
            engine_name=request.engine_name
        )
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to list the edges {request.from_institution_id} {request.from_object_type} -> '
                   f'{request.to_institution_id} {request.to_object_type}, the error was: '
                   f'{type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"Edges listed successfully", "edges_id": objects_id})


@router.post("/exists")
def method_exists(request: schemas.InsertItemRequest):
    """
    Checks if an item exists in the registry by ID.
    """
    if request.type == schemas.ItemType.edges:
        edge_list = gr.EdgeList()
        edge_list.set_from_json(request.data)
        out = edge_list.exists()

    return out

@router.post("/list")
def method_list(request: schemas.InsertItemRequest):
    """
    Returns a list of all items in the registry.
    """
    return "true"