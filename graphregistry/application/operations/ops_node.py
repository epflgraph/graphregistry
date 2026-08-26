# graphregistry/application/operations/ops_node.py
from __future__ import annotations
from typing import Any, Callable
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.repositories.prt_node import NodeRepository
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.application.resilience import retry_on_transient_db_error
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.entities.types import ConceptMapType
from graphregistry.domain.types import ActionSet

#==================#
# Class Definition #
#==================#
class _RepoAsNodeUoW(UnitOfWork):
    """Backward-compat wrapper that exposes a single repository as a UoW."""

    # Class initialization and dependency injection
    def __init__(self, repo: NodeRepository) -> None:
        self._repo = repo

    # Public Method: Return the wrapped node repository.
    @property
    def nodes(self) -> NodeRepository:
        return self._repo

    # Public Method: Edges are not available in this backward-compat wrapper.
    @property
    def edges(self) -> Any:
        raise NotImplementedError("Edges are not available in this backward-compat wrapper.")

    # Public Method: No-op commit for the backward-compat wrapper.
    def commit(self) -> None:
        pass

    # Public Method: No-op rollback for the backward-compat wrapper.
    def rollback(self) -> None:
        pass

    # Internal Function: Enter the backward-compat context.
    def __enter__(self) -> UnitOfWork:
        return self

    # Internal Function: Exit the backward-compat context without action.
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        pass

#==================#
# Class Definition #
#==================#
class NodeOperations:
    """Application service for node-related use cases.

    The service depends on a factory that produces a fresh UnitOfWork for each
    business operation. This keeps transaction boundaries explicit and ensures
    that every public method owns its own persistence scope.
    """

    # Class initialization and dependency injection
    def __init__(self, uow_factory: Callable[[], UnitOfWork] | None = None, *, repo: NodeRepository | None = None, concept_detection_gateway: ConceptDetectionGateway | None = None) -> None:

        # Validate that either a factory or a repository is provided, but not both.
        if repo is not None and uow_factory is not None:
            raise ValueError("Provide either uow_factory= or repo=, not both.")

        # If a repository is provided directly, wrap it in a backward-compatible
        # UnitOfWork implementation.
        if repo is not None:
            self.uow_factory = lambda: _RepoAsNodeUoW(repo)

        # If a factory is provided, use it as-is.
        elif uow_factory is not None:
            self.uow_factory = uow_factory
        else:
            raise ValueError("NodeOperations requires either uow_factory= or repo=.")

        # Optional gateway used to detect concepts for nodes.
        self.concept_detection_gateway = concept_detection_gateway
        # Logger for domain-level operation messages.
        self.msg = GraphLogger()

    #================================================================#
    # Function Group: Internal helpers                               #
    #================================================================#

    # Internal Function: Return the node repository from a new unit of work.
    def _repo(self) -> NodeRepository:
        """Return a repository from a new unit of work.

        Callers are responsible for entering the UoW context.
        """
        return self.uow_factory().nodes

    #================================================================#
    # Method Group: Basic Node CRUD/persistence operations           #
    #================================================================#

    # Public Method: List nodes of a given object type and optional ID pattern.
    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str]]:
        with self.uow_factory() as uow:
            return uow.nodes.list(object_type=object_type, id_pattern=id_pattern)

    # Public Method: Check whether a single node exists.
    def exists(self, key: NodeKey) -> bool:
        with self.uow_factory() as uow:
            return uow.nodes.exists(key)

    # Public Method: Check whether a list of nodes exist.
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        with self.uow_factory() as uow:
            return uow.nodes.exists_many(key_list)

    # Public Method: Retrieve a single node by key.
    def get(self, key: NodeKey) -> Node | None:
        with self.uow_factory() as uow:
            return uow.nodes.get(key)

    # Public Method: Retrieve a list of nodes by key.
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        with self.uow_factory() as uow:
            return uow.nodes.get_many(key_list)

    # Public Method: Save a single node, retrying transient database errors.
    @retry_on_transient_db_error()
    def save(self, node: Node, actions: ActionSet = ("commit",)) -> Node:
        with self.uow_factory() as uow:
            return uow.nodes.save(node, actions=actions)

    # Public Method: Save a list of nodes, retrying transient database errors.
    @retry_on_transient_db_error()
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("commit",)) -> NodeList:
        with self.uow_factory() as uow:
            return uow.nodes.save_many(node_list, actions=actions)

    # Public Method: Delete a single node, retrying transient database errors.
    @retry_on_transient_db_error()
    def delete(self, key: NodeKey, actions: ActionSet = ("commit",)) -> bool | None:
        with self.uow_factory() as uow:
            return uow.nodes.delete(key, actions=actions)

    # Public Method: Delete a list of nodes, retrying transient database errors.
    @retry_on_transient_db_error()
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        with self.uow_factory() as uow:
            return uow.nodes.delete_many(key_list, actions=actions)

    #================================================================#
    # Method Group: Node diagnostics and special get/save operations #
    #================================================================#

    # Public Method: Return nodes that have no concepts attached.
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        with self.uow_factory() as uow:
            return uow.nodes.get_with_no_concepts(object_type=object_type, id_pattern=id_pattern)

    # Public Method: Check whether a node has concepts for the given mapping type.
    def has_concepts(self, node_or_key: Node | NodeKey, map_type: ConceptMapType) -> bool:

        # If only a key was provided, load the node from persistence first.
        if isinstance(node_or_key, NodeKey):
            with self.uow_factory() as uow:
                node = uow.nodes.get(node_or_key)
            if not node:
                raise ValueError(f"Node with key {node_or_key} not found")
        else:
            node = node_or_key

        # Return False if the concept mapping is missing or empty.
        if not getattr(node.concepts, map_type):
            return False
        if not getattr(node.concepts, map_type).item_list:
            return False
        if len(getattr(node.concepts, map_type).item_list) == 0:
            return False
        return True

    #================================================================#
    # Method Group: Node field enrichment operations                 #
    #================================================================#

    # Public Method: Detect and attach concepts to one or more nodes.
    def enrich_with_concepts(self, nodes: Node | NodeList) -> Node | NodeList:

        # Ensure the concept-detection gateway is configured.
        gateway = self.concept_detection_gateway
        if gateway is None:
            raise ValueError("Concept detection gateway not configured")

        # Detect concepts for each node in a list.
        if isinstance(nodes, NodeList):
            for node in nodes.item_list:
                concepts = gateway.detect_concepts(f"{node.title}. {node.raw_text}" or "")
                node.concepts.detected = concepts
                self.msg.concepts_detected(node.key)
        else:
            # Detect concepts for a single node.
            concepts = gateway.detect_concepts(f"{nodes.title}. {nodes.raw_text}" or "")
            nodes.concepts.detected = concepts
            self.msg.concepts_detected(nodes.key)

        # Return the nodes with detected concepts attached.
        return nodes

    #================================================================#
    # Method Group: Backward-compatible repo accessor                #
    #================================================================#

    # Public Method: Expose the node repository for callers that still expect it.
    @property
    def repo(self) -> NodeRepository:
        """Expose the node repository for callers that still expect it.

        Deprecated: prefer to obtain repositories through a UnitOfWork.
        """
        return self._repo()
