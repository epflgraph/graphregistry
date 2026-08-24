# tests/unit_tests/application/test_ops_lecture.py
"""Unit tests for LectureOperations gateway wiring."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.domain.models.entities.mdl_base import NodeKey


class FakeLectureRepo:
    def get_video_download_task_id(self, key: NodeKey) -> str:
        return "task-123"


def test_explicit_video_gateway_is_used() -> None:
    repo = FakeLectureRepo()
    video_gtw = MagicMock()
    video_gtw.get_video_download_result.return_value = {"token": "vid-1"}

    ops = LectureOperations(
        repo=repo,
        processing_state=repo,
        video_processing_gateway=video_gtw,
    )
    result = ops.get_video_download_result(NodeKey(object_type="Lecture", object_id="L-1"))

    assert result == {"token": "vid-1"}
    video_gtw.get_video_download_result.assert_called_once_with(task_id="task-123")


def test_missing_video_gateway_raises() -> None:
    repo = FakeLectureRepo()
    ops = LectureOperations(repo=repo, processing_state=repo)
    with pytest.raises(ValueError, match="Missing gateway: video_processing"):
        ops.get_video_download_result(NodeKey(object_type="Lecture", object_id="L-1"))
