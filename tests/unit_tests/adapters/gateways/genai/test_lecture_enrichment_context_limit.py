# tests/unit_tests/adapters/gateways/genai/test_lecture_enrichment_context_limit.py
"""Tests for the GenAI lecture enrichment gateway context-length safeguard."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from graphregistry.adapters.gateways.genai.agt_lectureenrich import (
    GenAILectureEnrichmentGateway,
)
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureConceptTitleList,
    LectureEnrichmentResult,
    LectureEnrichmentTask,
    LectureKeyframeOCTandConcepts,
)


def _make_task(ocr_content: str) -> LectureEnrichmentTask:
    return LectureEnrichmentTask(
        lecture_id="0_toobig",
        keyframes=[
            LectureKeyframeOCTandConcepts(
                keyframe_id="kf-1",
                ocr_content=ocr_content,
                concepts=LectureConceptTitleList(raw_unrefined_list=["concept"]),
            )
        ],
    )


def test_oversized_prompt_is_skipped_before_request(tmp_path: Path) -> None:
    """When the estimated token count exceeds the safe limit, no LLM call is made."""
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("You are enriching metadata for one lecture.")

    gateway = GenAILectureEnrichmentGateway(
        prompt_path=prompt_path,
        max_context_length=1_024,
        context_length_margin=0,
        token_estimate_ratio=3.0,
    )

    # 10_000 ASCII characters / 3 bytes-per-token ratio ~= 3_333 tokens,
    # which is above the 1_024 context limit.
    task = _make_task(ocr_content="x" * 10_000)

    with patch("graphregistry.adapters.gateways.genai.agt_lectureenrich.send_llm_request") as mock_send:
        result = gateway.enrich(task)

    assert result is None
    mock_send.assert_not_called()


def test_fitting_prompt_is_sent_normally(tmp_path: Path) -> None:
    """When the prompt fits, the gateway forwards the request to the LLM client."""
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("short prompt")

    gateway = GenAILectureEnrichmentGateway(
        prompt_path=prompt_path,
        max_context_length=1_024,
        context_length_margin=0,
        token_estimate_ratio=3.0,
    )

    task = _make_task(ocr_content="small")

    fake_result = LectureEnrichmentResult(
        lecture_id=task.lecture_id,
        title="Title",
        long_description="Long description.",
        medium_description="Medium description.",
        short_description="Short description.",
        top_concepts=LectureConceptTitleList(),
        keyframes=[],
    )

    with patch("graphregistry.adapters.gateways.genai.agt_lectureenrich.send_llm_request") as mock_send:
        mock_send.return_value = fake_result
        result = gateway.enrich(task)

    mock_send.assert_called_once()
    assert result is not None
    assert result.lecture_id == task.lecture_id


def test_default_context_limit_matches_gpt_oss_120b() -> None:
    gateway = GenAILectureEnrichmentGateway()
    assert gateway.max_context_length == 131_072
