# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations

import json
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
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models."""

    def __init__(
        self,
        prompt_path: Path = Path("prompts/lecture_description_and_concepts_v2.txt"),
        timeout: int = 3600,
    ) -> None:
        self.prompt_path = prompt_path
        self.timeout = timeout

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
