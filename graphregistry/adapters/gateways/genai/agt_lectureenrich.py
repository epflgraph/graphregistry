# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations

import json

from graphregistry.adapters.clients.rcp_models import send_llm_request
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentResult,
    LectureEnrichmentTask,
)
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import (
    MySQLLectureEnrichmentTaskMapper,
)


def parse_llm_json(s: str):
    s = s.strip()

    # remove accidental outer quotes
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]

    # unescape inner quotes/newlines
    s = s.encode("utf-8").decode("unicode_escape")

    return json.loads(s)


class GenAILectureEnrichmentGateway:
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models."""

    def enrich(self, task: LectureEnrichmentTask) -> LectureEnrichmentResult | None:
        # Load prompt template
        with open("prompts/lecture_description_and_concepts.txt", "r") as f:
            prompt_template = f.read()

        # Serialize the enrichment task to a dictionary format suitable for LLM processing
        task_dict = MySQLLectureEnrichmentTaskMapper.to_dict(task)

        # Append the serialized task payload; do not use str.format because the prompt contains JSON braces.
        llm_prompt = f"{prompt_template}\n\n{json.dumps(task_dict, ensure_ascii=False, indent=2)}"

        # Print the generated prompt for debugging purposes
        # print("Generated LLM Prompt:")
        # print(llm_prompt)

        # Send the enrichment task to the LLM and receive the enrichment result
        llm_response = send_llm_request(
            timeout  = 3600,
            messages = [
                {
                    "role": "user",
                    "content": llm_prompt,
                }
            ]
        )

        data = parse_llm_json(llm_response)

        import rich
        rich.print("LLM Response:")
        rich.print_json(data=data)
        return None
