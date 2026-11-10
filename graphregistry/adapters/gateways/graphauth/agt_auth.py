# graphregistry/adapters/gateways/graphauth/agt_auth.py
"""GraphAuth gateway adapter: authorization decisions over HTTP.

Calls POST {url}/v1/verify with the caller's presented bearer token and maps
the response to an AccessDecision. Never raises for denied authorization:
fail-closed denials (including GraphAuth being unreachable) are returned as
allowed=False with a reason, so the application policy stays in charge.

FROZEN CONTRACT (Guy's integration brief 2026-11-09; KB authentication
collection: 'Human access: IdP tokens on /v1/verify'):
- The caller's bearer credential (gk- key or IdP access token) is forwarded
  VERBATIM — no parsing, no prefix filtering, no logging of token material
  on this side. Token validation is GraphAuth's job (RS256 via JWKS,
  iss/aud/exp, sub ⟷ principals.idp_sub, fail-closed).
- If token validation ever moves into THIS service (it currently must not):
  the issuer and audience come from GraphAuth's live configuration
  (GRAPHAUTH_VERIFY_TOKEN_ISSUER / _AUDIENCES in the GraphAuth stack).
  Two invariant gotchas: the issuer must be compared BYTE-EXACT as
  published by the IdP's discovery document (it carries a trailing slash —
  never normalize), and Authentik API tokens are NOT valid credentials —
  reject them fail-closed. Endpoint coordinates live in deployment
  configuration, never in source.
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

        # Invalid or inactive key: AUTHENTICATION failure (not a denial).
        # Carried as auth_failed so enforce mode surfaces it as 401.
        if response.status_code == 401:
            return AccessDecision(allowed=False, auth_failed=True,
                                  reason="invalid or inactive key")

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
