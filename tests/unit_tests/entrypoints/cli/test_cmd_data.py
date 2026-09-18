# tests/unit_tests/entrypoints/cli/test_cmd_data.py
"""Unit tests for CLI data command dispatch without a real database.

These tests verify that the CLI command functions parse input, delegate to the
right application operation methods, and handle wrapper-key normalization.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.entrypoints.cli.commands.cmd_data import cmd_data_exists, cmd_data_save


def _make_ctx() -> Any:
    """Build a minimal Typer context mock for unit tests."""
    ctx = MagicMock()
    ctx.obj = MagicMock()
    return ctx


class TestCmdDataSave:
    @patch("graphregistry.entrypoints.cli.commands.cmd_data.build_registry_operations_from_cli")
    @patch("graphregistry.entrypoints.cli.commands.cmd_data._load_json_input")
    def test_save_node(self, mock_load: MagicMock, mock_build: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_build.return_value = (mock_ops, MagicMock())

        payload = {"type": "Course", "id": "CS-433", "title": "ML"}
        mock_load.return_value = payload

        cmd_data_save(_make_ctx(), json_file="/tmp/node.json")

        mock_ops.save.assert_called_once()
        saved_node: Node = mock_ops.save.call_args[0][0]
        assert saved_node.key.object_id == "CS-433"

    @patch("graphregistry.entrypoints.cli.commands.cmd_data.build_registry_operations_from_cli")
    @patch("graphregistry.entrypoints.cli.commands.cmd_data._load_json_input")
    def test_save_node_list(self, mock_load: MagicMock, mock_build: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_build.return_value = (mock_ops, MagicMock())

        payload = {
            "node_list": [
                {"type": "Course", "id": "CS-433", "title": "ML"},
                {"type": "Course", "id": "MATH-203", "title": "Math"},
            ]
        }
        mock_load.return_value = payload

        cmd_data_save(_make_ctx(), json_file="/tmp/nodes.json")

        mock_ops.save_many.assert_called_once()
        saved_list = mock_ops.save_many.call_args[0][0]
        assert len(saved_list.item_list) == 2


class TestCmdDataExists:
    @patch("graphregistry.entrypoints.cli.commands.cmd_data.build_registry_operations_from_cli")
    @patch("graphregistry.entrypoints.cli.commands.cmd_data._load_json_input")
    def test_exists_node(self, mock_load: MagicMock, mock_build: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_ops.exists.return_value = True
        mock_build.return_value = (mock_ops, MagicMock())

        payload = {"type": "Course", "id": "CS-433"}
        mock_load.return_value = payload

        with patch("graphregistry.entrypoints.cli.commands.cmd_data.rich.print_json") as mock_print:
            cmd_data_exists(_make_ctx(), json_file="/tmp/key.json")

        mock_ops.exists.assert_called_once()
        key: NodeKey = mock_ops.exists.call_args[0][0]
        assert key.object_id == "CS-433"
        mock_print.assert_called_once()
        assert mock_print.call_args[1]["data"]["exists"] is True

    @patch("graphregistry.entrypoints.cli.commands.cmd_data.build_registry_operations_from_cli")
    @patch("graphregistry.entrypoints.cli.commands.cmd_data._load_json_input")
    def test_exists_node_list(self, mock_load: MagicMock, mock_build: MagicMock) -> None:
        mock_ops = MagicMock()
        mock_ops.exists_many.return_value = [True, False]
        mock_build.return_value = (mock_ops, MagicMock())

        payload = {
            "key_list": [
                {"type": "Course", "id": "CS-433"},
                {"type": "Course", "id": "MATH-203"},
            ]
        }
        mock_load.return_value = payload

        with patch("graphregistry.entrypoints.cli.commands.cmd_data.rich.print_json") as mock_print:
            cmd_data_exists(_make_ctx(), json_file="/tmp/keys.json")

        mock_ops.exists_many.assert_called_once()
        result = mock_print.call_args[1]["data"]
        assert result["exist_keys"] == [True, False]
        assert result["count"] == 2
