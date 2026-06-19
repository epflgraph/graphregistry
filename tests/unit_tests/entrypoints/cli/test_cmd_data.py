# tests/unit_tests/entrypoints/cli/test_cmd_data.py
"""Unit tests for CLI data command dispatch without a real database.

These tests verify that the CLI command functions parse input, delegate to the
right application operation methods, and handle wrapper-key normalization.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.entrypoints.cli.cmd_data import cmd_data_exists, cmd_data_save


def _args(**overrides: Any) -> SimpleNamespace:
    """Build a minimal CLI args namespace with all data subcommand options."""
    defaults: dict[str, Any] = {
        "ctx": SimpleNamespace(db=MagicMock(), global_config=MagicMock()),
        "env": "xaas_coresrv",
        # Input options
        "node": None,
        "edge": None,
        "node_list": None,
        "edge_list": None,
        "node_key": None,
        "edge_key": None,
        "node_key_list": None,
        "edge_key_list": None,
        "node_request": None,
        "edge_request": None,
        "actions": "commit",
        # Other
        "input_file": None,
        "import_method": None,
        "detect_concepts": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestCmdDataSave:
    @patch("graphregistry.entrypoints.cli.cmd_data.build_registry_operations_from_args")
    def test_save_node(self, mock_build_registry_ops: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_build_registry_ops.return_value = (mock_ops, MagicMock())

        payload: dict[str, Any] = {"type": "Course", "id": "CS-433", "title": "ML"}
        args = _args(node="/tmp/node.json")

        with patch("graphregistry.entrypoints.cli.cmd_data._load_json_input", return_value=payload):
            cmd_data_save(args)

        mock_ops.save.assert_called_once()
        saved_node: Node = mock_ops.save.call_args[0][0]
        assert saved_node.key.object_id == "CS-433"

    @patch("graphregistry.entrypoints.cli.cmd_data.build_registry_operations_from_args")
    def test_save_node_list(self, mock_build_registry_ops: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_build_registry_ops.return_value = (mock_ops, MagicMock())

        payload: dict[str, Any] = {
            "node_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Course", "id": "MATH-203"},
            ]
        }
        args = _args(node_list="/tmp/nodes.json")

        with patch("graphregistry.entrypoints.cli.cmd_data._load_json_input", return_value=payload):
            cmd_data_save(args)

        mock_ops.save_many.assert_called_once()
        saved_list = mock_ops.save_many.call_args[0][0]
        assert len(saved_list.item_list) == 2


class TestCmdDataExists:
    @patch("graphregistry.entrypoints.cli.cmd_data.build_registry_operations_from_args")
    def test_exists_node(self, mock_build_registry_ops: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_ops.exists.return_value = True
        mock_build_registry_ops.return_value = (mock_ops, MagicMock())

        payload: dict[str, Any] = {"type": "Course", "id": "CS-433"}
        args = _args(node_key="/tmp/key.json")

        with patch("graphregistry.entrypoints.cli.cmd_data._load_json_input", return_value=payload):
            with patch("graphregistry.entrypoints.cli.cmd_data.rich.print_json") as mock_print:
                cmd_data_exists(args)

        mock_ops.exists.assert_called_once()
        key: NodeKey = mock_ops.exists.call_args[0][0]
        assert key.object_id == "CS-433"
        mock_print.assert_called_once()
        assert mock_print.call_args[1]["data"]["exists"] is True

    @patch("graphregistry.entrypoints.cli.cmd_data.build_registry_operations_from_args")
    def test_exists_node_list(self, mock_build_registry_ops: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_ops.exists_many.return_value = [True, False]
        mock_build_registry_ops.return_value = (mock_ops, MagicMock())

        payload: dict[str, Any] = {
            "key_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Course", "id": "MATH-203"},
            ]
        }
        args = _args(node_key_list="/tmp/keys.json")

        with patch("graphregistry.entrypoints.cli.cmd_data._load_json_input", return_value=payload):
            with patch("graphregistry.entrypoints.cli.cmd_data.rich.print_json") as mock_print:
                cmd_data_exists(args)

        mock_ops.exists_many.assert_called_once()
        result = mock_print.call_args[1]["data"]
        assert result["exist_keys"] == [True, False]
        assert result["count"] == 2
