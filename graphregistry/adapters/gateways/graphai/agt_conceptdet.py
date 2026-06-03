# graphregistry/adapters/gateways/graphai/agt_conceptdet.py
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.domain.models.tasks.mdl_conceptdet import (
    ConceptDetectionTask,
    ConceptDetectionResult,
    ConceptDetectionResultList,
)
from requests import post


class GraphAIConceptDetectionGateway(GraphAIBaseGateway, ConceptDetectionGateway):

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

    def extract_keywords(self, text: str) -> list[str]:
        raise NotImplementedError("Keyword extraction is not implemented for GraphAIConceptDetectionGateway")

    def detect_concepts(self, text: str | list[str]) -> ConceptDetectionResultList:
        login_info = self._ensure_login_info()

        if isinstance(text, str):
            task = ConceptDetectionTask(text=text)
            params = task.get_params_dict()
            payload: dict[str, Any] = task.get_payload_dict()
        else:
            task = ConceptDetectionTask()
            params = task.get_params_dict()
            payload = {"keywords": text}

        url = (
            login_info["host"]
            + "/text/wikify?"
            + urlencode(params)
        )

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

        return ConceptDetectionResultList(
            item_list=[
                self._to_detected_concept(item)
                for item in data
                if isinstance(item, dict)
            ]
        )

    @staticmethod
    def _to_detected_concept(item: dict[str, Any]) -> ConceptDetectionResult:
        return ConceptDetectionResult(
            concept_id=str(item["concept_id"]),
            concept_name=str(item["concept_name"]),
            score=float(item.get("mixed_score") or item.get("score") or 0.0),
        )
