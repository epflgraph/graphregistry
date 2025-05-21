from pydantic import BaseModel, Field
from typing import List, Tuple, Dict, Any, Optional, Literal
from enum import Enum


class ItemType(str, Enum):
    """
    Enum defining allowed item types.
    """
    nodes = "nodes"
    edges = "edges"


class InsertItemRequest(BaseModel):
    """
    Schema for inserting multiple items into the registry.
    """
    type: ItemType = Field(..., title="Type", description="The type of the items (nodes, edges)")
    data: List[Dict[str, Any]] = Field(..., title="Data", description="A list of items to insert")
    update_existing: bool = Field(False, title="Update Existing", description="Whether to update existing items")
    actions: List[Literal['eval', 'commit', 'print']] = Field(
        ('eval',), title="Actions", description="A list of actions to perform."
    )


class DeleteNodesRequest(BaseModel):
    """
    Schema for deleting multiple nodes from the database.
    """
    institution_id: str = Field("EPFL", title="Institution ID", description="The institution ID of the nodes to delete")
    object_type: str = Field(..., title="object type", description="The object type of the nodes to delete")
    nodes_id: List[str] = Field(..., title="nodes ID", description="A list of the id of the nodes to delete")
    actions: List[Literal['eval', 'commit', 'print']] = Field(
        ('eval',), title="Actions", description="A list of actions to perform."
    )
    engine_name: str = Field("test", title="engine name", description="The name of the engine to use")


class DeleteEdgesRequest(BaseModel):
    """
    Schema for deleting multiple edges from the database.
    """
    from_institution_id: str = Field(
        "EPFL", title="Child Institution ID", description="The child institution ID of the edges to delete"
    )
    from_object_type: str = Field(
        ..., title="Child Object type", description="The child object type of the edges to delete"
    )
    to_institution_id: str = Field(
        "EPFL", title="Parent Institution ID", description="The parent institution ID of the edges to delete"
    )
    to_object_type: str = Field(
        ..., title="Parent Object Type", description="The parent object type of the edges to delete"
    )
    edges_id: List[Tuple[str, str]] = Field(
        ..., title="Edges IDs", description="A list of the id of the edges to delete. "
                "The first eleemnt is the child object ID and the second is the parent object ID."
    )
    actions: List[Literal['eval', 'commit', 'print']] = Field(
        ('eval',), title="Actions", description="A list of actions to perform."
    )


class ListNodesRequest(BaseModel):
    """
    Schema for listing nodes present in the database.
    """
    institution_id: str = Field("EPFL", title="Institution ID", description="The institution ID of the nodes to list")
    object_type: str = Field(..., title="object type", description="The object type of the nodes to list")
    engine_name: str = Field("test", title="engine name", description="The name of the engine to use")


class ListEdgesRequest(BaseModel):
    """
    Schema for listing edges present in the database.
    """
    from_institution_id: str = Field(
        "EPFL", title="Child Institution ID", description="The child institution ID of the edges to list"
    )
    from_object_type: str = Field(
        ..., title="Child Object Type", description="The child object type of the edges to list"
    )
    to_institution_id: str = Field(
        "EPFL", title="Parent Institution ID", description="The parent institution ID of the edges to list"
    )
    to_object_type: str = Field(
        ..., title="Parent Object Type", description="The parent object type of the edges to list"
    )
    engine_name: str = Field("test", title="engine name", description="The name of the engine to use")


class ExistsItemRequest(BaseModel):
    """
    Schema for checking if an item exists in the database.
    """
    type: ItemType = Field(..., title="Type", description="The type of the items (nodes, edges)")
    data: List[Dict[str, Any]] = Field(..., title="Data", description="A list of items to insert")

class ItemResponse(BaseModel):
    """
    Schema representing an item in the database.
    """
    item_id: str
    name: str
    description: Optional[str]

class ListItemsResponse(BaseModel):
    """
    Schema for returning a list of items.
    """
    items: List[ItemResponse]