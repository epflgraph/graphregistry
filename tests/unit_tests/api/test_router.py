# tests/unit_tests/api/test_router.py
"""Integration tests for the FastAPI router using TestClient and fake operations.

These tests exercise the API entrypoint layer without a running MySQL server by
overriding the FastAPI dependencies that build NodeOperations/EdgeOperations.
"""
from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient


class TestStatusEndpoint:
    def test_status(self, api_client: TestClient) -> None:
        response = api_client.get("/api")
        assert response.status_code == 200
        assert response.json()["success"] is True


class TestNodeEndpoints:
    def test_nodes_save(self, api_client: TestClient) -> None:
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

    def test_nodes_exists(self, api_client: TestClient) -> None:
        # Pre-populate through the API
        api_client.post("/api/nodes/save", json={"node": {"type": "Course", "id": "CS-433"}})

        response = api_client.post("/api/nodes/exists", json={"key": {"type": "Course", "id": "CS-433"}})
        assert response.status_code == 200
        assert response.json()["exists"] is True

    def test_nodes_get_not_found(self, api_client: TestClient) -> None:
        response = api_client.post("/api/nodes/get", json={"key": {"type": "Course", "id": "MISSING"}})
        assert response.status_code == 200
        assert response.json()["found"] is False

    def test_nodes_get_found(self, api_client: TestClient) -> None:
        api_client.post(
            "/api/nodes/save",
            json={"node": {"type": "Course", "id": "CS-433", "title": "Machine Learning"}},
        )
        response = api_client.post("/api/nodes/get", json={"key": {"type": "Course", "id": "CS-433"}})
        assert response.status_code == 200
        data = response.json()
        assert data["found"] is True
        assert data["node"]["id"] == "CS-433"

    def test_nodes_save_many(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "node_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Course", "id": "MATH-203"},
            ]
        }
        response = api_client.post("/api/nodes/save_many", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] == 2

    def test_nodes_delete(self, api_client: TestClient) -> None:
        api_client.post("/api/nodes/save", json={"node": {"type": "Course", "id": "CS-433"}})
        response = api_client.post("/api/nodes/delete", json={"key": {"type": "Course", "id": "CS-433"}})
        assert response.status_code == 200
        assert response.json()["success"] is True

        exists = api_client.post("/api/nodes/exists", json={"key": {"type": "Course", "id": "CS-433"}})
        assert exists.json()["exists"] is False

    def test_nodes_list(self, api_client: TestClient) -> None:
        api_client.post("/api/nodes/save", json={"node": {"type": "Course", "id": "CS-433"}})
        api_client.post("/api/nodes/save", json={"node": {"type": "Course", "id": "CS-250"}})
        response = api_client.post("/api/nodes/list", json={"type": "Course"})
        assert response.status_code == 200
        assert response.json()["count"] == 2

    def test_nodes_save_rejects_non_string_custom_field_value(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "node": {
                "type": "Course",
                "id": "CS-433",
                "custom_fields": [
                    {"field_name": "credits", "field_value": 2},
                ],
            }
        }
        response = api_client.post("/api/nodes/save", json=payload)
        assert response.status_code == 422
        assert any(
            error["loc"] == ["body", "node", "custom_fields", 0, "field_value"]
            for error in response.json()["detail"]
        )


class TestEdgeEndpoints:
    def test_edges_save(self, api_client: TestClient) -> None:
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

    def test_edges_save_defaults_context_to_part_of(self, api_client: TestClient) -> None:
        # "Lecture" -> "Course" with context "part of" is an allowed edge type.
        payload: dict[str, Any] = {
            "edge": {
                "from_type": "Lecture",
                "from_id": "lec-1",
                "to_type": "Course",
                "to_id": "CS-433",
            }
        }
        response = api_client.post("/api/edges/save", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["saved_key"]["context"] == "part of"

    def test_edges_exists(self, api_client: TestClient) -> None:
        api_client.post("/api/edges/save", json={
            "edge": {
                "from_type": "Course", "from_id": "CS-433",
                "to_type": "Person", "to_id": "p-1",
                "context": "teacher",
            }
        })
        response = api_client.post("/api/edges/exists", json={
            "key": {
                "from_type": "Course", "from_id": "CS-433",
                "to_type": "Person", "to_id": "p-1",
                "context": "teacher",
            }
        })
        assert response.status_code == 200
        assert response.json()["exists"] is True

    def test_edges_delete_many(self, api_client: TestClient) -> None:
        api_client.post("/api/edges/save", json={
            "edge": {
                "from_type": "Course", "from_id": "CS-433",
                "to_type": "Person", "to_id": "p-1",
                "context": "teacher",
            }
        })
        response = api_client.post("/api/edges/delete_many", json={
            "key_list": [{
                "from_type": "Course", "from_id": "CS-433",
                "to_type": "Person", "to_id": "p-1",
                "context": "teacher",
            }]
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["n_deleted"] == 1

    def test_edges_save_rejects_non_string_custom_field_value(self, api_client: TestClient) -> None:
        payload: dict[str, Any] = {
            "edge": {
                "from_type": "Course",
                "from_id": "CS-433",
                "to_type": "Person",
                "to_id": "p-1",
                "context": "teacher",
                "custom_fields": [
                    {"field_name": "credits", "field_value": 2},
                ],
            }
        }
        response = api_client.post("/api/edges/save", json=payload)
        assert response.status_code == 422
        assert any(
            error["loc"] == ["body", "edge", "custom_fields", 0, "field_value"]
            for error in response.json()["detail"]
        )
