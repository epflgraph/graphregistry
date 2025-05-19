from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal
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


class DeleteItemRequest(BaseModel):
    """
    Schema for deleting an item from the database.
    """
    item_id: str = Field(..., title="Item ID", description="Unique identifier of the item to be deleted")

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