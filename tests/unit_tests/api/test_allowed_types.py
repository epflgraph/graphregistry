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
