# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations
import json
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.clients.rcp_models import send_llm_request

class GenAILectureEnrichmentGateway:
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models."""

    def enrich(self, task: LectureEnrichmentTask) -> LectureEnrichmentResult | None:
        # Load prompt template
        with open("prompts/lecture_description_and_concepts_v2.txt", "r") as f:
            prompt_template = f.read()

        # Serialize the enrichment task to a dictionary format suitable for LLM processing
        task_dict = MySQLLectureEnrichmentTaskMapper.to_dict(task)

        # Append the serialized task payload; do not use str.format because the prompt contains JSON braces.
        llm_prompt = f"{prompt_template}\n\n{json.dumps(task_dict, ensure_ascii=False, indent=2)}"

        # Send the enrichment task to the LLM and receive the enrichment result
        llm_response = send_llm_request(
            timeout=3600,
            response_llm_model=LectureEnrichmentResult,
            messages=[
                {
                    "role": "system",
                    "content": "Return only valid JSON that matches the requested schema.",
                },
                {
                    "role": "user",
                    "content": llm_prompt,
                },
            ],
        )

        # Return the enrichment result as a domain model instance
        return llm_response
