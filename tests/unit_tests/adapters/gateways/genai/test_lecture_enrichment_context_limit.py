# tests/unit_tests/adapters/gateways/genai/test_lecture_enrichment_context_limit.py
"""Tests for the GenAI lecture enrichment gateway context-length safeguard."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from graphregistry.adapters.gateways.genai.gtw_lectureenrich import (
    GenAILectureEnrichmentGateway,
)
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentChunkResult,
    LectureEnrichmentResult,
    LectureEnrichmentTask,
    LectureKeyframe,
    LectureKeyframeConceptList,
    LectureKeyframeRefinedConcepts,
    LectureTopConceptList,
)


def _make_task(ocr_content: str) -> LectureEnrichmentTask:
    # Tasks carry only the keyframe ids and their OCR text; the concepts are
    # detected by the map calls and returned in the result, keyed by id.
    return LectureEnrichmentTask(
        lecture_id="0_toobig",
        keyframes=[
            LectureKeyframe(
                keyframe_id="kf-1",
                ocr_content=ocr_content,
            )
        ],
    )


def test_oversized_prompt_is_skipped_before_request(tmp_path: Path) -> None:
    """When the estimated token count exceeds the safe limit, no LLM call is made."""
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("You are enriching metadata for one lecture.")

    gateway = GenAILectureEnrichmentGateway(
        map_prompt_path=prompt_path,
        reduce_prompt_path=prompt_path,
        max_context_length=1_024,
        context_length_margin=0,
        token_estimate_ratio=3.0,
    )

    # 10_000 ASCII characters / 3 bytes-per-token ratio ~= 3_333 tokens,
    # which is above the 1_024 context limit. The oversized keyframe cannot
    # be split, so its chunk is skipped pre-flight and the lecture is
    # dropped without any LLM call.
    task = _make_task(ocr_content="x" * 10_000)

    with patch("graphregistry.adapters.gateways.genai.gtw_lectureenrich.send_llm_request") as mock_send:
        result = gateway.enrich(task)

    assert result is None
    mock_send.assert_not_called()


def test_fitting_prompt_is_sent_normally(tmp_path: Path) -> None:
    """When the prompt fits, the gateway sends one map call per chunk and one reduce call."""
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("short prompt")

    gateway = GenAILectureEnrichmentGateway(
        map_prompt_path=prompt_path,
        reduce_prompt_path=prompt_path,
        max_context_length=1_024,
        context_length_margin=0,
        token_estimate_ratio=3.0,
    )

    task = _make_task(ocr_content="small")

    fake_map_result = LectureEnrichmentChunkResult(
        lecture_id=task.lecture_id,
        chunk_index=0,
        chunk_count=1,
        keyframes=[
            LectureKeyframeRefinedConcepts(
                keyframe_id="kf-1",
                refined_concepts=LectureKeyframeConceptList(ai_extracted_keywords=["concept"]),
            )
        ],
        chunk_title_draft="Draft title.",
        chunk_top_concepts=["concept"],
    )
    fake_reduce_result = LectureEnrichmentResult(
        lecture_id=task.lecture_id,
        title="Title",
        long_description="Long description.",
        medium_description="Medium description.",
        short_description="Short description.",
        top_concepts=LectureTopConceptList(ai_top_keywords=["concept"]),
        keyframes=[],
    )

    with patch("graphregistry.adapters.gateways.genai.gtw_lectureenrich.send_llm_request") as mock_send:
        # One map call for the single chunk, then one reduce call.
        mock_send.side_effect = [fake_map_result, fake_reduce_result]
        result = gateway.enrich(task)

    assert mock_send.call_count == 2
    assert result is not None
    assert result.lecture_id == task.lecture_id
    assert result.title == "Title"


def test_default_context_limit_matches_gpt_oss_120b() -> None:
    gateway = GenAILectureEnrichmentGateway()
    assert gateway.max_context_length == 131_072
