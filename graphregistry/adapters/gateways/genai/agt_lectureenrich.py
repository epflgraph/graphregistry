# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations

import json
from pathlib import Path

from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentResult,
    LectureEnrichmentTask,
)
from graphregistry.adapters.clients.rcp_models import send_llm_request
from graphregistry.adapters.gateways.mappers.agm_lectureenrich import GenAILectureEnrichmentMapper


class GenAILectureEnrichmentGateway:
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models."""

    def __init__(
        self,
        prompt_path: Path = Path("prompts/lecture_description_and_concepts_v2.txt"),
        timeout: int = 3600,
    ) -> None:
        self.prompt_path = prompt_path
        self.timeout = timeout

    def enrich(self, task: LectureEnrichmentTask) -> LectureEnrichmentResult | None:
        prompt_template = self.prompt_path.read_text(encoding="utf-8")

        task_payload = GenAILectureEnrichmentMapper.to_prompt_dict(task)

        llm_prompt = (
            prompt_template
            + "\n\nHere is the lecture data:\n"
            + json.dumps(task_payload, ensure_ascii=False, indent=2)
        )

        print(llm_prompt)

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

        if result is None:
            return None

        return GenAILectureEnrichmentMapper.normalize(result)