# graphregistry/adapters/gateways/genai/gtw_lectureenrich.py
"""Concrete gateway that enriches lectures through chunked map/reduce LLM calls."""
from __future__ import annotations
import json
import math
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from loguru import logger as sysmsg
from openai import OpenAIError
from graphregistry.adapters.clients.rcp_models import send_llm_request
from graphregistry.adapters.gateways.mappers.agm_lectureenrich import GenAILectureEnrichmentMapper
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentChunkResult,
    LectureEnrichmentResult,
    LectureEnrichmentTask,
    LectureKeyframe,
    LectureKeyframeRefinedConcepts,
)

#==================#
# Class Definition #
#==================#
class GenAILectureEnrichmentGateway:
    """Concrete gateway that enriches one lecture through chunked map/reduce calls.

    Per-keyframe concept detection is a parallel map: one structured call per
    chunk of keyframes, each also producing a small chunk report. The model
    detects concepts from the OCR text alone; no candidate concepts are sent.
    One reduce call then merges the reports into the lecture-level metadata,
    so the reduce model never sees the raw OCR. Chunk boundaries follow a
    token budget so no single call exceeds the model's context window, and
    reassembly is canonically ordered so the concept-to-keyframe mapping stays
    total even when individual chunks fail (they degrade to empty refined
    lists). The estimate is intentionally conservative because no tokenizer is
    bundled.
    """

    # Default limit for the gpt-oss-120b model currently used through RCP.
    DEFAULT_MAX_CONTEXT_LENGTH = 131_072

    # Default per-chunk keyframe cap: moderate chunks keep structured output
    # reliable, since the output scales with the chunk size.
    DEFAULT_MAX_KEYFRAMES_PER_CHUNK = 50

    # Public Method: Initialise the gateway with its prompts, chunking budget and concurrency.
    def __init__(
        self,
        map_prompt_path         : Path = Path("prompts/genai/lecture_enrich_map.txt"),
        reduce_prompt_path      : Path = Path("prompts/genai/lecture_enrich_reduce.txt"),
        timeout                 : int  = 3600,
        max_context_length      : int | None = None,
        context_length_margin   : int  = 8_192,
        token_estimate_ratio    : float = 3.0,
        max_keyframes_per_chunk : int | None = None,
        max_concurrent_requests : int  = 4,
        llm_model               : str | None = None,
    ) -> None:
        self.map_prompt_path = map_prompt_path
        self.reduce_prompt_path = reduce_prompt_path
        self.timeout = timeout
        self.max_context_length = (
            max_context_length or self.DEFAULT_MAX_CONTEXT_LENGTH
        )
        self.context_length_margin = context_length_margin
        self.token_estimate_ratio = token_estimate_ratio
        self.max_keyframes_per_chunk = (
            max_keyframes_per_chunk or self.DEFAULT_MAX_KEYFRAMES_PER_CHUNK
        )
        self.max_concurrent_requests = max_concurrent_requests
        self.llm_model = llm_model

    # Public Method: Enrich one lecture through parallel chunk map calls and one reduce call.
    def enrich(
        self,
        task: LectureEnrichmentTask,
        verbose: bool = False,
    ) -> LectureEnrichmentResult | None:

        # Load both prompt templates once per lecture; every chunk call and the
        # reduce call reuse them.
        map_template = self.map_prompt_path.read_text(encoding="utf-8")
        reduce_template = self.reduce_prompt_path.read_text(encoding="utf-8")

        # A task without keyframes has nothing to refine and no reports to merge.
        if not task.keyframes:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={}: the task carries no keyframes.",
                task.lecture_id,
            )
            return None

        # Plan canonically ordered, token-budgeted chunks over the task keyframes.
        chunk_plans = self._plan_chunks(task, map_template)
        chunk_count = len(chunk_plans)
        sysmsg.info(
            "Lecture enrichment for lecture_id={} planned in {} chunk(s).",
            task.lecture_id,
            chunk_count,
        )

        # Run one structured map call per chunk, up to the concurrency cap.
        successful: dict[int, LectureEnrichmentChunkResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as pool:
            futures = {
                pool.submit(
                    self._enrich_chunk,
                    task,
                    chunk,
                    chunk_index,
                    chunk_count,
                    map_template,
                    verbose,
                ): chunk_index
                for chunk_index, chunk in enumerate(chunk_plans)
            }
            for future, chunk_index in futures.items():
                chunk_result = future.result()
                if chunk_result is not None:
                    successful[chunk_index] = (
                        GenAILectureEnrichmentMapper.normalize_chunk_result(chunk_result)
                    )

        # All chunks failed: skip the lecture entirely so batch processing stays alive.
        failed = [index for index in range(chunk_count) if index not in successful]
        if not successful:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={}: all {} chunk call(s) failed; "
                "failed chunks marked for reprocessing: {}.",
                task.lecture_id,
                chunk_count,
                failed,
            )
            return None
        if failed:
            sysmsg.warning(
                "Lecture enrichment for lecture_id={}: chunk(s) {} failed and degraded to "
                "empty refined lists (marked for reprocessing).",
                task.lecture_id,
                failed,
            )

        # Reassemble the refined keyframes in canonical order; totality holds.
        refined_keyframes = self._reassemble_keyframes(chunk_plans, successful, task)

        # Merge the chunk reports into lecture-level metadata with one small
        # reduce call; the reduce model never sees the raw OCR.
        reduce_payload = GenAILectureEnrichmentMapper.to_reduce_prompt_dict(
            task.lecture_id,
            successful,
        )
        reduce_prompt = (
            reduce_template
            + "\n\nHere is the lecture data:\n"
            + json.dumps(reduce_payload, ensure_ascii=False, indent=2)
        )
        if verbose:
            print(reduce_prompt)

        # A lecture cut into very many chunks can still grow the report payload;
        # keep the same pre-flight guard as the map calls.
        estimated_tokens = self._estimate_prompt_tokens(reduce_prompt)
        max_input_tokens = self.max_context_length - self.context_length_margin
        if estimated_tokens > max_input_tokens:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={}: estimated reduce prompt "
                "length ({} tokens) exceeds safe context limit ({} tokens).",
                task.lecture_id,
                estimated_tokens,
                max_input_tokens,
            )
            return None

        # Send the reduce request; any failure skips the lecture gracefully so
        # batch processing stays alive.
        try:
            reduced = send_llm_request(
                timeout            = self.timeout,
                llm_model          = self.llm_model,
                response_llm_model = LectureEnrichmentResult,
                messages           = [
                    {
                        "role"    : "system",
                        "content" : (
                            "You produce structured lecture-enrichment metadata. "
                            "Follow the provided response schema exactly. "
                            "Return no prose outside the structured response."
                        ),
                    },
                    {
                        "role"    : "user",
                        "content" : reduce_prompt,
                    },
                ],
            )
        except ValueError as exc:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={} due to invalid reduce response: {}",
                task.lecture_id,
                exc,
            )
            return None
        except OpenAIError as exc:
            # Keep batch processing alive when the reduce call fails.
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={} due to LLM API error: {}",
                task.lecture_id,
                exc,
            )
            return None

        # Clean the reduce response before assembling the final result.
        reduced = GenAILectureEnrichmentMapper.normalize(reduced)

        # Build the final result: reduce metadata plus the canonically ordered
        # refined keyframes; the task id stays authoritative for the mapping.
        return LectureEnrichmentResult(
            lecture_id         = task.lecture_id,
            title              = reduced.title,
            long_description   = reduced.long_description,
            medium_description = reduced.medium_description,
            short_description  = reduced.short_description,
            top_concepts       = reduced.top_concepts,
            keyframes          = refined_keyframes,
        )

    # Internal Function: Estimate token count from prompt text using a conservative byte-length heuristic.
    def _estimate_prompt_tokens(self, prompt: str) -> int:
        """Estimate token count from prompt text using a conservative heuristic.

        No tokenizer is declared as a project dependency, so we approximate by
        dividing the UTF-8 byte length by a configurable ratio. Using bytes is
        safer than character count for multilingual/OCR payloads.
        """
        byte_length = len(prompt.encode("utf-8"))
        return math.ceil(byte_length / max(self.token_estimate_ratio, 0.1))

    # Internal Function: Canonical keyframe order: numeric id suffix ascending, id as tiebreaker.
    @staticmethod
    def _keyframe_sort_key(keyframe: LectureKeyframe) -> tuple[int, str]:
        match = re.search(r"(\d+)$", keyframe.keyframe_id)
        suffix = int(match.group(1)) if match else -1
        return (suffix, keyframe.keyframe_id)

    # Internal Function: Estimate the json payload cost of one keyframe in bytes.
    @staticmethod
    def _keyframe_payload_bytes(keyframe: LectureKeyframe) -> int:
        payload = {
            "keyframe_id" : keyframe.keyframe_id,
            "ocr_content" : keyframe.ocr_content,
        }
        return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    # Internal Function: Split the canonically ordered keyframes into token-budgeted chunks.
    def _plan_chunks(
        self,
        task: LectureEnrichmentTask,
        map_template: str,
    ) -> list[list[LectureKeyframe]]:
        # Fixed overhead per chunk: the map template, the payload wrapper and
        # the chunk meta fields (lecture_id, chunk_index, chunk_count).
        overhead_bytes = len(
            (map_template + "\n\nHere is the lecture data:\n").encode("utf-8")
        ) + 256
        budget_bytes = (
            (self.max_context_length - self.context_length_margin)
            * self.token_estimate_ratio
        ) - overhead_bytes

        # Greedy packing over the canonical order: chunks are cut only at
        # budget or keyframe-cap boundaries, never inside a keyframe.
        chunks: list[list[LectureKeyframe]] = []
        current: list[LectureKeyframe] = []
        current_bytes = 0
        for keyframe in sorted(task.keyframes, key=self._keyframe_sort_key):
            cost = self._keyframe_payload_bytes(keyframe)
            if current and (
                current_bytes + cost > budget_bytes
                or len(current) >= self.max_keyframes_per_chunk
            ):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(keyframe)
            current_bytes += cost
        if current:
            chunks.append(current)
        return chunks

    # Internal Function: Enrich one chunk through a single structured map call.
    def _enrich_chunk(
        self,
        task: LectureEnrichmentTask,
        chunk_keyframes: list[LectureKeyframe],
        chunk_index: int,
        chunk_count: int,
        map_template: str,
        verbose: bool = False,
    ) -> LectureEnrichmentChunkResult | None:

        # Build the chunk payload through the mapper: keyframe ids and OCR text
        # only, no lecture-level context in the first pass.
        payload = GenAILectureEnrichmentMapper.to_chunk_prompt_dict(
            task.lecture_id,
            chunk_keyframes,
            chunk_index,
            chunk_count,
        )
        llm_prompt = (
            map_template
            + "\n\nHere is the lecture data:\n"
            + json.dumps(payload, ensure_ascii=False, indent=2)
        )
        if verbose:
            print(llm_prompt)

        # Pre-flight check per chunk: the budget makes overflow unlikely, but a
        # single oversized keyframe can still exceed the context window.
        estimated_tokens = self._estimate_prompt_tokens(llm_prompt)
        max_input_tokens = self.max_context_length - self.context_length_margin
        if estimated_tokens > max_input_tokens:
            sysmsg.warning(
                "Skipping chunk {} of lecture_id={}: estimated prompt length "
                "({} tokens) exceeds safe context limit ({} tokens).",
                chunk_index,
                task.lecture_id,
                estimated_tokens,
                max_input_tokens,
            )
            return None

        # Send the structured map request for this chunk; a failure returns
        # None so the chunk degrades to empty refined lists during reassembly.
        try:
            chunk_result = send_llm_request(
                timeout            = self.timeout,
                llm_model          = self.llm_model,
                response_llm_model = LectureEnrichmentChunkResult,
                messages           = [
                    {
                        "role"    : "system",
                        "content" : (
                            "You produce structured lecture-enrichment metadata. "
                            "Follow the provided response schema exactly. "
                            "Return no prose outside the structured response."
                        ),
                    },
                    {
                        "role"    : "user",
                        "content" : llm_prompt,
                    },
                ],
            )
        except ValueError as exc:
            sysmsg.warning(
                "Chunk {} of lecture_id={} failed due to invalid LLM response: {}",
                chunk_index,
                task.lecture_id,
                exc,
            )
            return None
        except OpenAIError as exc:
            # Keep the remaining chunks alive when one chunk call fails.
            sysmsg.warning(
                "Chunk {} of lecture_id={} failed due to LLM API error: {}",
                chunk_index,
                task.lecture_id,
                exc,
            )
            return None

        # Surface what the model actually returned: a chunk result with fewer
        # keyframes than planned means the model skipped some, and reassembly
        # will degrade them silently unless we log it here.
        sysmsg.info(
            "Chunk {} of lecture_id={}: map returned {} of {} planned keyframe(s).",
            chunk_index,
            task.lecture_id,
            len(chunk_result.keyframes),
            len(chunk_keyframes),
        )
        if verbose:
            print(chunk_result)
        return chunk_result

    # Internal Function: Reassemble refined keyframes in canonical order with totality guaranteed.
    def _reassemble_keyframes(
        self,
        chunk_plans: list[list[LectureKeyframe]],
        successful: dict[int, LectureEnrichmentChunkResult],
        task: LectureEnrichmentTask,
    ) -> list[LectureKeyframeRefinedConcepts]:
        refined: list[LectureKeyframeRefinedConcepts] = []

        # Iterate the chunk plans in canonical order, never the completion
        # order: parallel map calls return scrambled.
        for chunk_index, chunk in enumerate(chunk_plans):
            chunk_result = successful.get(chunk_index)
            returned = (
                {kf.keyframe_id: kf for kf in chunk_result.keyframes}
                if chunk_result is not None
                else {}
            )
            input_ids = {keyframe.keyframe_id for keyframe in chunk}
            extra = set(returned) - input_ids
            if extra:
                sysmsg.warning(
                    "Dropping {} keyframe(s) returned for chunk {} of lecture_id={} "
                    "that are not in the input.",
                    len(extra),
                    chunk_index,
                    task.lecture_id,
                )

            # Keyframes missing from a chunk result (a failed chunk, or the
            # model skipping one) degrade to an EMPTY refined list, so every
            # input keyframe appears exactly once in the result.
            missing = 0
            for keyframe in chunk:
                refined_keyframe = returned.get(keyframe.keyframe_id)
                if refined_keyframe is not None:
                    refined.append(refined_keyframe)
                else:
                    refined.append(
                        GenAILectureEnrichmentMapper.degraded_refined_concepts(keyframe)
                    )
                    missing += 1
            # Degradation must never be silent: unrefined keyframes hide LLM
            # skips and make runs look successful when they were not.
            if missing:
                sysmsg.warning(
                    "Chunk {} of lecture_id={}: {} of {} keyframe(s) missing from the map "
                    "result and degraded to empty refined lists (marked for reprocessing).",
                    chunk_index,
                    task.lecture_id,
                    missing,
                    len(chunk),
                )
        return refined
