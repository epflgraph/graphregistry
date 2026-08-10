# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations

import json
import math
from pathlib import Path

from loguru import logger as sysmsg
from openai import OpenAIError

from graphregistry.adapters.clients.rcp_models import send_llm_request
from graphregistry.adapters.gateways.mappers.agm_lectureenrich import GenAILectureEnrichmentMapper
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentResult,
    LectureEnrichmentTask,
)


class GenAILectureEnrichmentGateway:
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models.

    Performs a lightweight pre-flight context-length check so that requests that
    are likely to exceed the model's context window are skipped before being sent.
    The estimate is intentionally conservative because no tokenizer is bundled.
    """

    # Default limit for the gpt-oss-120b model currently used through RCP.
    DEFAULT_MAX_CONTEXT_LENGTH = 131_072

    def __init__(
        self,
        prompt_path: Path = Path("prompts/lecture_description_and_concepts_v2.txt"),
        timeout: int = 3600,
        max_context_length: int | None = None,
        context_length_margin: int = 8_192,
        token_estimate_ratio: float = 3.0,
    ) -> None:
        self.prompt_path = prompt_path
        self.timeout = timeout
        self.max_context_length = (
            max_context_length or self.DEFAULT_MAX_CONTEXT_LENGTH
        )
        self.context_length_margin = context_length_margin
        self.token_estimate_ratio = token_estimate_ratio

    def _estimate_prompt_tokens(self, prompt: str) -> int:
        """Estimate token count from prompt text using a conservative heuristic.

        No tokenizer is declared as a project dependency, so we approximate by
        dividing the UTF-8 byte length by a configurable ratio. Using bytes is
        safer than character count for multilingual/OCR payloads.
        """
        byte_length = len(prompt.encode("utf-8"))
        return math.ceil(byte_length / max(self.token_estimate_ratio, 0.1))

    def enrich(
        self,
        task: LectureEnrichmentTask,
        verbose: bool = False,
    ) -> LectureEnrichmentResult | None:

        prompt_template = self.prompt_path.read_text(encoding="utf-8")
        task_payload = GenAILectureEnrichmentMapper.to_prompt_dict(task)

        llm_prompt = (
            prompt_template
            + "\n\nHere is the lecture data:\n"
            + json.dumps(task_payload, ensure_ascii=False, indent=2)
        )

        if verbose:
            print(llm_prompt)

        estimated_tokens = self._estimate_prompt_tokens(llm_prompt)
        max_input_tokens = self.max_context_length - self.context_length_margin
        if estimated_tokens > max_input_tokens:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={}: "
                "estimated prompt length ({} tokens) exceeds safe context limit ({} tokens). "
                "max_context_length={}",
                task.lecture_id,
                estimated_tokens,
                max_input_tokens,
                self.max_context_length,
            )
            return None

        try:
            result = send_llm_request(
                timeout=self.timeout,
                response_llm_model=LectureEnrichmentResult,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You produce structured lecture-enrichment metadata. "
                            "Follow the provided response schema exactly. "
                            "Return no prose outside the structured response."
                        ),
                    },
                    {
                        "role": "user",
                        "content": llm_prompt,
                    },
                ],
            )
        except ValueError as exc:
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={} due to invalid LLM response: {}",
                task.lecture_id,
                exc,
            )
            return None
        except OpenAIError as exc:
            # Keep batch processing alive when one lecture request fails (e.g., context overflow).
            sysmsg.warning(
                "Skipping lecture enrichment for lecture_id={} due to LLM API error: {}",
                task.lecture_id,
                exc,
            )
            return None

        return GenAILectureEnrichmentMapper.normalize(result)
