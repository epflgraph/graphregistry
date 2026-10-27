# graphregistry/adapters/gateways/graphauth/agt_auth.py
"""GraphAuth gateway adapter: authorization decisions over HTTP.

Calls POST {url}/v1/verify with the caller's presented bearer token and maps
the response to an AccessDecision. Never raises for denied authorization:
fail-closed denials (including GraphAuth being unreachable) are returned as
allowed=False with a reason, so the application policy stays in charge.
"""
from __future__ import annotations

import requests

from graphregistry.application.ports.gateways.prt_access import AccessGateway
from graphregistry.domain.models.values.mdl_access import AccessAction, AccessDecision


#==================#
# Class Definition #
#==================#
class GraphAuthAccessGateway:
    """Implements AccessGateway against a GraphAuth deployment."""

    #--------------------#
    # Internal variables #
    #--------------------#
    _url    : str
    _timeout: float

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, url: str, timeout: float = 3.0) -> None:
        self._url     = url.rstrip("/")
        self._timeout = timeout

    #----------------#
    # Public methods #
    #----------------#
    def verify(self,
               access_token: str,
               action      : AccessAction,
               node_key    : tuple[str, str] | None = None,
               edge_key    : tuple[str, str, str, str, str] | None = None) -> AccessDecision:
        """Ask GraphAuth whether the token's principal may perform the action."""
        # Build exactly one of node/edge, as required by the GraphAuth schema.
        body: dict = {"action": action.value}
        if node_key is not None:
            body["node"] = [str(part) for part in node_key]
        elif edge_key is not None:
            body["edge"] = [str(part) for part in edge_key]

        try:
            response = requests.post(
                f"{self._url}/v1/verify",
                json     = body,
                headers  = {"Authorization": f"Bearer {access_token}"},
                timeout  = self._timeout,
            )
        except requests.RequestException as exc:
            # Fail closed: an unreachable authorization service denies access.
            return AccessDecision(allowed=False, reason=f"graphauth unreachable: {exc}")

        # Invalid or inactive key: explicit denial with a stable reason.
        if response.status_code == 401:
            return AccessDecision(allowed=False, reason="invalid or inactive key")

        # Any other non-200 is an unexpected GraphAuth answer: fail closed.
        if response.status_code != 200:
            return AccessDecision(allowed=False,
                                  reason=f"unexpected graphauth status {response.status_code}")

        data = response.json()
        return AccessDecision(
            allowed  = data.get("decision") == "allow",
            principal= data.get("principal"),
            reason   = None if data.get("decision") == "allow" else "denied by grants",
        )
