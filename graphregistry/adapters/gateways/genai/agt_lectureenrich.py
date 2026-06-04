# graphregistry/adapters/gateways/genai/agt_lectureenrich.py
from __future__ import annotations
import json
from pathlib import Path
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.adapters.clients.rcp_models import send_llm_request
from graphregistry.adapters.gateways.mappers.agm_lectureenrich import GenAILectureEnrichmentMapper

# Gateway class
class GenAILectureEnrichmentGateway:
    """Concrete gateway that prepares lecture enrichment prompts for GenAI models."""

    def __init__(
        self,
        prompt_path: Path = Path("prompts/lecture_description_and_concepts_v2.txt"),
        timeout: int = 3600,
    ) -> None:
        self.prompt_path = prompt_path
        self.timeout = timeout

    # The enrich method takes a LectureEnrichmentTask, constructs a prompt, sends it to the LLM, and returns a LectureEnrichmentResult
    def enrich(self, task: LectureEnrichmentTask, verbose: bool = False) -> LectureEnrichmentResult | None:

        # Read the prompt template from the file system
        prompt_template = self.prompt_path.read_text(encoding="utf-8")

        # Convert the LectureEnrichmentTask into a dict format suitable for JSON serialization and LLM prompting
        task_payload = GenAILectureEnrichmentMapper.to_prompt_dict(task)

        # Construct the full prompt by combining the template with the JSON-serialized task payload
        llm_prompt = (
            prompt_template
            + "\n\nHere is the lecture data:\n"
            + json.dumps(task_payload, ensure_ascii=False, indent=2)
        )

        # Optionally print the prompt for debugging
        if verbose:
            print(llm_prompt)

        # Send the prompt to the LLM and get the response, which should conform to the LectureEnrichmentResult schema
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

        # Return the normalized result, which will convert dicts to dataclass instances and perform any necessary transformations
        return GenAILectureEnrichmentMapper.normalize(result)
