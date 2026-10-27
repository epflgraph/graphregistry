# graphregistry/application/policies/pol_access.py
"""AccessControl: authorization policy over GraphAuth decisions.

Modes (EnforcementMode):
- OFF:     no gateway calls; everything allowed (pre-integration behaviour).
- LOG:     every decision is logged, denials never block (dry-run rollout).
- ENFORCE: denials raise AccessDeniedError (mapped to 403 by the entrypoint).

Enforcement points (per the agreed semantics):
- writes (save/delete, single and batch): all-or-nothing, offending keys reported.
- targeted reads (get/exists): checked per key; unauthorized keys are invisible.
- enumeration reads (list/get_many): filtered to readable keys.
- edge grants are independent of node grants.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable

from graphregistry.application.ports.gateways.prt_access import AccessGateway
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.values.mdl_access import AccessAction, AccessDecision, EnforcementMode

# Dedicated logger so rollout dry-run lines are easy to grep.
logger = logging.getLogger("graphregistry.access")


#==================#
# Class Definition #
#==================#
class AccessDeniedError(Exception):
    """Raised in enforce mode when one or more keys are not authorized."""

    #--------------------#
    # Internal variables #
    #--------------------#
    action     : str
    denied_keys: list[tuple]

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, message: str, action: str, denied_keys: list[tuple]) -> None:
        self.action      = action
        self.denied_keys = denied_keys
        super().__init__(message)


#==================#
# Class Definition #
#==================#
class AccessControl:
    """Authorization decision point wrapping the GraphAuth gateway."""

    #--------------------#
    # Internal variables #
    #--------------------#
    _gateway: AccessGateway | None
    mode    : EnforcementMode

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, gateway: AccessGateway | None, mode: EnforcementMode | str) -> None:
        self._gateway = gateway
        self.mode     = EnforcementMode(mode)

    #----------------#
    # Public methods #
    #----------------#
    # Node: single-key check, raising in enforce mode.
    def require_node(self, token: str | None, action: AccessAction, key: NodeKey) -> None:
        """Require the action on one node key (all-or-nothing helper)."""
        decision = self._decide_node(token, action, key)
        if self.mode is EnforcementMode.ENFORCE and not decision.allowed:
            raise AccessDeniedError(
                f"Not authorized to {action.value} node "
                f"{key.object_type}:{key.object_id}"
                + (f" ({decision.reason})" if decision.reason else ""),
                action     = action.value,
                denied_keys= [key.to_tuple()],
            )

    # Node: batch check, all-or-nothing, returns the (unchanged) key list.
    def require_node_all(self, token: str | None, action: AccessAction,
                         keys: Iterable[NodeKey]) -> list[NodeKey]:
        """Require the action on every node key; raise if any is denied."""
        key_list = list(keys)
        if self.mode is not EnforcementMode.ENFORCE:
            for key in key_list:
                self._decide_node(token, action, key)      # logging only
            return key_list
        denied: list[tuple] = []
        for key in key_list:
            decision = self._decide_node(token, action, key)
            if not decision.allowed:
                denied.append(key.to_tuple())
        if denied:
            raise AccessDeniedError(
                f"Not authorized to {action.value} {len(denied)} node key(s)",
                action     = action.value,
                denied_keys= denied,
            )
        return key_list

    # Node: read filtering for enumeration endpoints.
    def filter_node_keys(self, token: str | None, keys: Iterable[NodeKey]) -> list[NodeKey]:
        """Return only the node keys readable by the token's principal."""
        if self.mode is EnforcementMode.OFF:
            return list(keys)
        return [key for key in keys if self._decide_node(token, AccessAction.READ, key).allowed]

    # Edge: single-key check, raising in enforce mode.
    def require_edge(self, token: str | None, action: AccessAction, key: EdgeKey) -> None:
        """Require the action on one edge key."""
        decision = self._decide_edge(token, action, key)
        if self.mode is EnforcementMode.ENFORCE and not decision.allowed:
            raise AccessDeniedError(
                f"Not authorized to {action.value} edge "
                f"{key.from_object_type}:{key.from_object_id}-[{key.context}]->"
                f"{key.to_object_type}:{key.to_object_id}"
                + (f" ({decision.reason})" if decision.reason else ""),
                action     = action.value,
                denied_keys= [key.to_tuple()],
            )

    # Edge: batch check, all-or-nothing.
    def require_edge_all(self, token: str | None, action: AccessAction,
                         keys: Iterable[EdgeKey]) -> list[EdgeKey]:
        """Require the action on every edge key; raise if any is denied."""
        key_list = list(keys)
        if self.mode is not EnforcementMode.ENFORCE:
            for key in key_list:
                self._decide_edge(token, action, key)      # logging only
            return key_list
        denied: list[tuple] = []
        for key in key_list:
            decision = self._decide_edge(token, action, key)
            if not decision.allowed:
                denied.append(key.to_tuple())
        if denied:
            raise AccessDeniedError(
                f"Not authorized to {action.value} {len(denied)} edge key(s)",
                action     = action.value,
                denied_keys= denied,
            )
        return key_list

    # Edge: read filtering for enumeration endpoints.
    def filter_edge_keys(self, token: str | None, keys: Iterable[EdgeKey]) -> list[EdgeKey]:
        """Return only the edge keys readable by the token's principal."""
        if self.mode is EnforcementMode.OFF:
            return list(keys)
        return [key for key in keys if self._decide_edge(token, AccessAction.READ, key).allowed]

    #---------------------------#
    # Internal helper functions #
    #---------------------------#
    def _decide_node(self, token: str | None, action: AccessAction, key: NodeKey) -> AccessDecision:
        """Consult the gateway (mode-dependent) and log the decision."""
        return self._decide(token, action, node_key=key.to_tuple())

    def _decide_edge(self, token: str | None, action: AccessAction, key: EdgeKey) -> AccessDecision:
        """Consult the gateway (mode-dependent) and log the decision."""
        return self._decide(token, action, edge_key=key.to_tuple())

    def _decide(self, token: str | None, action: AccessAction,
                node_key: tuple | None = None,
                edge_key: tuple | None = None) -> AccessDecision:
        """Single decision path: logging, fail-closed handling, mode shortcuts."""
        key_repr = ":".join(str(part) for part in (node_key or edge_key or []))
        kind     = "node" if node_key is not None else "edge"

        # OFF mode: pre-integration behaviour, no calls, everything allowed.
        if self.mode is EnforcementMode.OFF:
            return AccessDecision(allowed=True)

        # Enforce mode with no token: hard 401-equivalent handled by the
        # entrypoint; here treat as a deny decision for completeness.
        if not token:
            decision = AccessDecision(allowed=False, reason="no bearer token")
        else:
            decision = self._gateway.verify(access_token=token, action=action,
                                            node_key=node_key, edge_key=edge_key) \
                if self._gateway is not None else \
                AccessDecision(allowed=False, reason="no gateway configured")

        if self.mode is EnforcementMode.LOG:
            logger.info(
                "[access:dry-run] %s %s %s principal=%s reason=%s",
                decision.allowed and "ALLOW" or "DENY ",
                action.value, f"{kind} {key_repr}",
                decision.principal or "?", decision.reason or "-",
            )
        else:
            logger.info(
                "[access] %s %s %s principal=%s reason=%s",
                decision.allowed and "ALLOW" or "DENY ",
                action.value, f"{kind} {key_repr}",
                decision.principal or "?", decision.reason or "-",
            )
        return decision
