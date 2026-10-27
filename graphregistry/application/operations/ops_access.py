# graphregistry/application/operations/ops_access.py
"""Access-controlled wrappers around Node/Edge operations.

These implement the same public interface as NodeOperations/EdgeOperations so
the API entrypoint can swap them in transparently: every listed/overridden
method consults the AccessControl policy before delegating to the inner
operations. Anything not overridden is forwarded unchanged via __getattr__.

The CLI keeps using the raw operations for now; when it needs enforcement it
resolves its own principal and wraps the same way.
"""
from __future__ import annotations

from typing import Any

from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.policies.pol_access import AccessControl
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList, NodeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.values.mdl_access import AccessAction
from graphregistry.domain.types import ActionSet


#-----------------------------------#
# Internal helper functions         #
#-----------------------------------#
def _node_keys(key_list: NodeKeyList | list[NodeKey]) -> list[NodeKey]:
    """Normalize a NodeKeyList or plain list into a list of NodeKey."""
    if isinstance(key_list, NodeKeyList):
        return list(key_list.item_list)
    return list(key_list)


def _edge_keys(key_list: EdgeKeyList | list[EdgeKey]) -> list[EdgeKey]:
    """Normalize an EdgeKeyList or plain list into a list of EdgeKey."""
    if isinstance(key_list, EdgeKeyList):
        return list(key_list.item_list)
    return list(key_list)


#==================#
# Class Definition #
#==================#
class AuthorizedNodeOperations:
    """Node operations with access-control enforcement in front."""

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, inner: NodeOperations, access: AccessControl, token: str | None) -> None:
        self._inner  = inner
        self._access = access
        self._token  = token

    #----------------#
    # Public methods #
    #----------------#
    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str]]:
        """List nodes, filtered to keys readable by the principal."""
        rows = self._inner.list(object_type=object_type, id_pattern=id_pattern)
        keys = [NodeKey.from_tuple(tuple(row)) for row in rows]
        return [key.to_tuple() for key in self._access.filter_node_keys(self._token, keys)]

    def exists(self, key: NodeKey) -> bool:
        self._access.require_node(self._token, AccessAction.READ, key)
        return self._inner.exists(key)

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        visible = self._access.filter_node_keys(self._token, _node_keys(key_list))
        return self._inner.exists_many(visible)

    def get(self, key: NodeKey) -> Node | None:
        self._access.require_node(self._token, AccessAction.READ, key)
        return self._inner.get(key)

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        visible = self._access.filter_node_keys(self._token, _node_keys(key_list))
        return self._inner.get_many(visible)

    def save(self, node: Node, actions: ActionSet = ("commit",)) -> Node:
        self._access.require_node(self._token, AccessAction.WRITE, node.key)
        return self._inner.save(node, actions=actions)

    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("commit",)) -> NodeList:
        keys = [node.key for node in node_list.item_list] \
            if isinstance(node_list, NodeList) else [node.key for node in node_list]
        self._access.require_node_all(self._token, AccessAction.WRITE, keys)
        return self._inner.save_many(node_list, actions=actions)

    def delete(self, key: NodeKey, actions: ActionSet = ("commit",)) -> bool | None:
        self._access.require_node(self._token, AccessAction.WRITE, key)
        return self._inner.delete(key, actions=actions)

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        self._access.require_node_all(self._token, AccessAction.WRITE, _node_keys(key_list))
        return self._inner.delete_many(key_list, actions=actions)

    #---------------------------#
    # Passthrough for the rest  #
    #---------------------------#
    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


#==================#
# Class Definition #
#==================#
class AuthorizedEdgeOperations:
    """Edge operations with access-control enforcement in front."""

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, inner: EdgeOperations, access: AccessControl, token: str | None) -> None:
        self._inner  = inner
        self._access = access
        self._token  = token

    #----------------#
    # Public methods #
    #----------------#
    def list(self, object_type: tuple[str, str], id_pattern: str | None = None) -> list[tuple[str, str, str, str, str]]:
        """List edges, filtered to keys readable by the principal."""
        rows = self._inner.list(object_type=object_type, id_pattern=id_pattern)
        keys = [EdgeKey.from_tuple(tuple(row)) for row in rows]
        return [key.to_tuple() for key in self._access.filter_edge_keys(self._token, keys)]

    def exists(self, key: EdgeKey) -> bool:
        self._access.require_edge(self._token, AccessAction.READ, key)
        return self._inner.exists(key)

    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        visible = self._access.filter_edge_keys(self._token, _edge_keys(key_list))
        return self._inner.exists_many(visible)

    def get(self, key: EdgeKey) -> Edge | None:
        self._access.require_edge(self._token, AccessAction.READ, key)
        return self._inner.get(key)

    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        visible = self._access.filter_edge_keys(self._token, _edge_keys(key_list))
        return self._inner.get_many(visible)

    def save(self, edge: Edge, actions: ActionSet = ("commit",)) -> Edge:
        self._access.require_edge(self._token, AccessAction.WRITE, edge.key)
        return self._inner.save(edge, actions=actions)

    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ("commit",)) -> EdgeList:
        keys = [edge.key for edge in edge_list.item_list] \
            if isinstance(edge_list, EdgeList) else [edge.key for edge in edge_list]
        self._access.require_edge_all(self._token, AccessAction.WRITE, keys)
        return self._inner.save_many(edge_list, actions=actions)

    def delete(self, key: EdgeKey, actions: ActionSet = ("commit",)) -> bool | None:
        self._access.require_edge(self._token, AccessAction.WRITE, key)
        return self._inner.delete(key, actions=actions)

    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        self._access.require_edge_all(self._token, AccessAction.WRITE, _edge_keys(key_list))
        return self._inner.delete_many(key_list, actions=actions)

    #---------------------------#
    # Passthrough for the rest  #
    #---------------------------#
    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
