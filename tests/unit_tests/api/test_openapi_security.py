# tests/unit_tests/api/test_openapi_security.py
"""Unit tests for the OpenAPI security schemes (Swagger Authorize dialog)."""
import pytest
from fastapi.testclient import TestClient

from graphregistry.entrypoints.api.main import create_app


def test_paste_token_scheme_always_present():
    """The advanced paste-token option exists even without OIDC config."""
    schema = create_app().openapi()
    schemes = schema["components"]["securitySchemes"]
    assert "2. Paste a token (advanced)" in schemes
    assert schemes["2. Paste a token (advanced)"]["scheme"] == "bearer"
    assert "1. SSO login (recommended)" not in schemes          # not configured
    # /api data endpoints declare security; system endpoints do not.
    assert schema["paths"]["/api/nodes/get"]["post"]["security"] == [{"2. Paste a token (advanced)": []}]
    assert "security" not in schema["paths"]["/health"]["get"]


def test_sso_scheme_listed_first_when_configured(monkeypatch):
    """Scheme order == recommended usage order: SSO first, paste-token second."""
    monkeypatch.setenv("GRAPHREGISTRY_OIDC_ISSUER", "https://idp.example/application/o/graph-registry/")
    monkeypatch.setenv("GRAPHREGISTRY_OIDC_CLIENT_ID", "test-public-client")

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "authorization_endpoint": "https://idp.example/application/o/authorize/",
                "token_endpoint": "https://idp.example/application/o/token/",
            }

    def fake_get(url, timeout=5):
        assert "well-known/openid-configuration" in url
        return FakeResponse()

    monkeypatch.setattr("graphregistry.entrypoints.api.main.requests.get", fake_get)

    schema = create_app().openapi()
    assert list(schema["components"]["securitySchemes"]) == [
        "1. SSO login (recommended)", "2. Paste a token (advanced)",
    ]
    flow = schema["components"]["securitySchemes"]["1. SSO login (recommended)"][
        "flows"]["authorizationCode"]
    assert flow["authorizationUrl"] == "https://idp.example/application/o/authorize/"
    assert flow["tokenUrl"] == "https://idp.example/application/o/token/"
    assert flow["x-usePkce"] == "SHA-256"
    # /api operations accept EITHER the browser SSO flow or a pasted credential.
    assert schema["paths"]["/api/nodes/get"]["post"]["security"] == [
        {"1. SSO login (recommended)": ["openid", "profile"]},
        {"2. Paste a token (advanced)": []},
    ]


def test_discovery_failure_falls_back_to_paste_token(monkeypatch):
    monkeypatch.setenv("GRAPHREGISTRY_OIDC_ISSUER", "https://idp.example/application/o/graph-registry/")
    monkeypatch.setenv("GRAPHREGISTRY_OIDC_CLIENT_ID", "test-public-client")

    import requests

    def boom(url, timeout=5):
        raise requests.ConnectionError("idp unreachable")

    monkeypatch.setattr("graphregistry.entrypoints.api.main.requests.get", boom)

    schema = create_app().openapi()
    assert "1. SSO login (recommended)" not in schema["components"]["securitySchemes"]
    assert "2. Paste a token (advanced)" in schema["components"]["securitySchemes"]


#----------------------------------------------------------#
# Custom Swagger UI route                                  #
#----------------------------------------------------------#
def test_docs_page_preconfigures_pkce_and_client_id(monkeypatch):
    monkeypatch.setenv("GRAPHREGISTRY_OIDC_CLIENT_ID", "test-public-client")
    client = TestClient(create_app())

    r = client.get("/docs")
    assert r.status_code == 200
    assert "test-public-client" in r.text                      # client id pre-filled
    assert "usePkceWithAuthorizationCodeGrant" in r.text       # PKCE automatic
    assert "/docs/oauth2-redirect" in r.text                   # back-channel wired
    assert "persistAuthorization" in r.text                    # survives page reloads


def test_oauth2_redirect_backchannel_served():
    client = TestClient(create_app())
    r = client.get("/docs/oauth2-redirect")
    assert r.status_code == 200
