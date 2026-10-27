# graphregistry/application/ports/gateways/prt_access.py
"""Port: access-control verification against GraphAuth.

Implemented by the GraphAuth gateway adapter. The port intentionally
carries no HTTP types and no "check on behalf of someone else" — the
decision always concerns the principal of the presented token.
"""
from __future__ import annotations

from typing import Protocol

from graphregistry.domain.models.values.mdl_access import AccessAction, AccessDecision


#==================#
# Class Definition #
#==================#
class AccessGateway(Protocol):
    """Port for authorization decisions (GraphAuth /v1/verify)."""

    def verify(self,
               access_token: str,
               action      : AccessAction,
               node_key    : tuple[str, str] | None = None,
               edge_key    : tuple[str, str, str, str, str] | None = None) -> AccessDecision:
        """Verify whether the token's principal may perform the action."""
        ...
