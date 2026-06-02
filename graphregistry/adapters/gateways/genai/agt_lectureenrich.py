# graphregistry/application/gateways/gtw_lectureenrich.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.adapters.persistence.mysql.mappers.amp_lecture import MySQLLectureEnrichmentTaskMapper
from graphregistry.adapters.clients.rcp_models import send_llm_request

# Model definition
class LectureEnrichmentGateway(Protocol):
    """Gateway protocol for lecture enrichment operations
    """

    def enrich(self, task: LectureEnrichmentTask) -> LectureEnrichmentResult | None:
        """Enriches a lecture based on the provided enrichment task.
        """

        # Load prompt template
        with open('prompts/lecture_enrichment_prompt.txt', 'r') as f:
            prompt_template = f.read()

        # Serialize the enrichment task to a dictionary format suitable for LLM processing
        task_dict = MySQLLectureEnrichmentTaskMapper.to_dict(task)

        # Generate the prompt by filling in the template with the task data
        llm_prompt = prompt_template.format(**task_dict)

        # Print the generated prompt for debugging purposes
        print("Generated LLM Prompt:")
        print(llm_prompt)

        return None

        # # Send the enrichment task to the LLM and receive the enrichment result
        # llm_response = send_llm_request(
        #     timeout  = 3600,
        #     messages = [
        #         {
        #             "role": "user",
        #             "content": llm_prompt,
        #         }
        #     ]
        # )
        # return None
