# tests/unit_tests/api/test_allowed_types.py
"""Tests for the config-based allowed-type validation on save endpoints.

These tests use the real ``AllowedTypesValidator`` (loaded from
``config/config_api.json``) and fake repositories so no database is required.
"""
from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient


class TestNodeAllowedTypes:
    """Validate allowed node object types on /api/nodes/save and /api/nodes/save_many."""

    def test_nodes_save_allows_configured_type(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "node": {
                "type": "Course",
                "id": "CS-433",
                "title": "Machine Learning",
            }
        }
        response = api_client.post("/api/nodes/save", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["saved_key"] == {"type": "Course", "id": "CS-433"}

    def test_nodes_save_rejects_unconfigured_type(self, api_client: TestClient) -> None:
        # "Slide" is a valid ObjectType but not present in the allowed-types list.
        payload: dict[str, Any] = {
            "node": {
                "type": "Slide",
                "id": "slide-1",
                "title": "Intro Slide",
            }
        }
        response = api_client.post("/api/nodes/save", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()

    def test_nodes_save_rejects_unknown_type_with_unified_message(
        self, api_client: TestClient
    ) -> None:
        # Lowercase "course" is not a known ObjectType, so Pydantic rejects it
        # before the allowed-types validator runs. The exception handler now
        # converts this into the same unified message.
        payload: dict[str, Any] = {
            "node": {
                "type": "course",
                "id": "cs-433",
                "title": "Machine Learning",
            }
        }
        response = api_client.post("/api/nodes/save", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()

    def test_nodes_save_many_allows_configured_types(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "node_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Person", "id": "p-1"},
            ]
        }
        response = api_client.post("/api/nodes/save_many", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] == 2

    def test_nodes_save_many_rejects_mixed_unconfigured_type(
        self, api_client: TestClient
    ) -> None:
        payload: dict[str, Any] = {
            "node_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Slide", "id": "slide-1"},
            ]
        }
        response = api_client.post("/api/nodes/save_many", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()


class TestOpenAPIExamples:
    """Swagger/OpenAPI examples should reflect the allowed-type configuration."""

    def test_openapi_uses_first_allowed_type_as_example(
        self, api_client: TestClient
    ) -> None:
        response = api_client.get("/openapi.json")
        assert response.status_code == 200
        schemas = response.json()["components"]["schemas"]

        # First allowed node type in config_api.json is "Course".
        assert schemas["NodeSpec"]["properties"]["type"]["example"] == "Course"
        assert schemas["NodeKeySpec"]["properties"]["type"]["example"] == "Course"

        # First allowed edge tuple in config_api.json is ("Course", "Person", "teacher").
        assert schemas["EdgeSpec"]["properties"]["from_type"]["example"] == "Course"
        assert schemas["EdgeSpec"]["properties"]["to_type"]["example"] == "Person"
        assert schemas["EdgeSpec"]["properties"]["context"]["example"] == "teacher"
        assert schemas["EdgeKeySpec"]["properties"]["from_type"]["example"] == "Course"
        assert schemas["EdgeKeySpec"]["properties"]["to_type"]["example"] == "Person"
        assert schemas["EdgeKeySpec"]["properties"]["context"]["example"] == "teacher"

        # Request-level examples must also use the first allowed values so the
        # Swagger UI request body preview does not fall back to the field default.
        node_save_example = schemas["APINodesSaveRequest"]["example"]["node"]
        assert node_save_example["type"] == "Course"
        assert node_save_example["title"] == "string"
        assert node_save_example["custom_fields"][0]["field_name"] == "string"
        node_save_many_example = schemas["APINodesSaveManyRequest"]["example"]["node_list"][0]
        assert node_save_many_example["type"] == "Course"
        assert node_save_many_example["title"] == "string"
        assert schemas["APIEdgesSaveRequest"]["example"]["edge"]["context"] == "teacher"
        assert schemas["APIEdgesSaveManyRequest"]["example"]["edge_list"][0]["context"] == "teacher"


class TestEdgeAllowedTypes:
    """Validate allowed edge tuples on /api/edges/save and /api/edges/save_many."""

    def test_edges_save_allows_configured_tuple(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "edge": {
                "from_type": "Course",
                "from_id": "CS-433",
                "to_type": "Person",
                "to_id": "p-1",
                "context": "teacher",
            }
        }
        response = api_client.post("/api/edges/save", json=payload)
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_edges_save_rejects_unconfigured_tuple(self, api_client: TestClient) -> None:
        # Course -> Person exists, but the "taught_by" context is not allowed.
        payload: dict[str, Any] = {
            "edge": {
                "from_type": "Course",
                "from_id": "CS-433",
                "to_type": "Person",
                "to_id": "p-1",
                "context": "taught_by",
            }
        }
        response = api_client.post("/api/edges/save", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()

    def test_edges_save_rejects_unknown_type_with_unified_message(
        self, api_client: TestClient
    ) -> None:
        # Lowercase "course" is not a known ObjectType. The handler should
        # return the same unified "not an allowed type" message.
        payload: dict[str, Any] = {
            "edge": {
                "from_type": "course",
                "from_id": "cs-433",
                "to_type": "Person",
                "to_id": "p-1",
                "context": "teacher",
            }
        }
        response = api_client.post("/api/edges/save", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()

    def test_edges_save_many_allows_configured_tuples(
        self, api_client: TestClient
    ) -> None:
        payload: dict[str, Any] = {
            "edge_list": [
                {
                    "from_type": "Course",
                    "from_id": "CS-433",
                    "to_type": "Person",
                    "to_id": "p-1",
                    "context": "teacher",
                },
                {
                    "from_type": "Lecture",
                    "from_id": "lec-1",
                    "to_type": "Course",
                    "to_id": "CS-433",
                    "context": "part of",
                },
            ]
        }
        response = api_client.post("/api/edges/save_many", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] == 2

    def test_edges_save_many_rejects_mixed_unconfigured_tuple(
        self, api_client: TestClient
    ) -> None:
        payload: dict[str, Any] = {
            "edge_list": [
                {
                    "from_type": "Course",
                    "from_id": "CS-433",
                    "to_type": "Person",
                    "to_id": "p-1",
                    "context": "teacher",
                },
                {
                    "from_type": "Course",
                    "from_id": "CS-433",
                    "to_type": "Person",
                    "to_id": "p-2",
                    "context": "taught_by",
                },
            ]
        }
        response = api_client.post("/api/edges/save_many", json=payload)
        assert response.status_code == 400
        assert "not an allowed type" in response.json()["detail"].lower()
