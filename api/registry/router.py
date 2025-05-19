from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import api.registry.schemas as schemas
from api.registry.graphregistry import GraphRegistry

# Initialize the GraphRegistry instance
gr = GraphRegistry()

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
        obj_list.info()
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to create the objects, the error was: {type(e).__name__}({exception_args})'
        )
    try:
        r = obj_list.commit(actions=request.actions)
    except Exception as e:
        exception_args = ", ".join(["'" + a + "'" for a in e.args])
        raise HTTPException(
            status_code=500,
            detail=f'Failed to commit the objects, the error was: {type(e).__name__}({exception_args})'
        )
    return JSONResponse(content={"message": f"{request.type} inserted successfully", "eval_results": r})


@router.post("/delete")
def method_delete(data: schemas.DeleteItemRequest):
    """
    Deletes an item from the registry by ID.
    """
    return "gr.delete_item(data.item_id)"

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