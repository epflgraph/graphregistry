# graphregistry/adapters/gateways/graphai/agt_conceptdet.py
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from requests import post

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionTask


class GraphAIConceptDetectionGateway(GraphAIBaseGateway, ConceptDetectionGateway):
    """GraphAI adapter for concept detection and Wikipedia search."""

    def wiki_search(self, search_term: str) -> list[dict[str, Any]]:
        login_info = self._ensure_login_info()

        url = login_info["host"] + "/text/wiki_search"
        payload: dict[str, Any] = {"search_term": search_term}

        response = self._request(
            url=url,
            login_info=login_info,
            request_func=post,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=payload,
            timeout=900,
            max_tries=5,
        )

        data = response.json()

        if not isinstance(data, list):
            raise ValueError(
                f"Unexpected /text/wiki_search response shape: expected list, got {type(data).__name__}"
            )

        return [item for item in data if isinstance(item, dict)]

    def extract_keywords(
        self,
        text: str,
        *,
        use_nltk: bool = False,
        max_tries: int = 5,
        timeout: int = 900,
    ) -> list[str]:
        """Extract keywords from raw text using the GraphAI /text/keywords endpoint."""
        login_info = self._ensure_login_info()

        url = f'{login_info["host"]}/text/keywords?use_nltk={"true" if use_nltk else "false"}'
        response = self._request(
            url=url,
            login_info=login_info,
            request_func=post,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={"raw_text": text},
            timeout=timeout,
            max_tries=max_tries,
        )

        data = response.json()
        if not isinstance(data, list):
            raise ValueError(
                f"Unexpected /text/keywords response shape: expected list, got {type(data).__name__}"
            )

        return [str(item) for item in data]

    def detect_concepts(self, text: str | list[str]) -> ScoredConceptList:
        """Detect concepts from raw text or a list of keywords."""
        if isinstance(text, str):
            task = ConceptDetectionTask(text=text)
        else:
            task = ConceptDetectionTask(keywords=text)
        return self.detect_concepts_with_task(task)

    def detect_concepts_with_task(self, task: ConceptDetectionTask) -> ScoredConceptList:
        """Detect concepts using a fully configured ConceptDetectionTask."""
        login_info = self._ensure_login_info()

        params = task.get_url_safe_params_dict()
        payload = task.get_payload_dict()

        url = login_info["host"] + "/text/wikify?" + urlencode(params)

        response = self._request(
            url=url,
            login_info=login_info,
            request_func=post,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=payload,
            timeout=900,
            max_tries=5,
        )

        data = response.json()

        if not isinstance(data, list):
            raise ValueError(
                f"Unexpected /text/wikify response shape: expected list, got {type(data).__name__}"
            )

        return ScoredConceptList(
            item_list=[
                self._to_detected_concept(item)
                for item in data
                if isinstance(item, dict)
            ]
        )

    @staticmethod
    def _to_detected_concept(item: dict[str, Any]) -> ScoredConcept:
        return ScoredConcept(
            concept=Concept(
                id=str(item["concept_id"]),
                name=str(item["concept_name"]),
            ),
            score=float(item.get("mixed_score") or item.get("score") or 0.0),
        )
