# graphregistry/adapters/gateways/graphai/agt_conceptdet.py
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
from graphregistry.domain.models.mdl_concept import (
    ConceptExtractionTask,
    DetectedConcept,
    DetectedConceptList,
)
from requests import post


class GraphAIConceptGateway(GraphAIBaseGateway, ConceptGateway):
    def detect_concepts(self, text: str) -> DetectedConceptList:
        if not text or not text.strip():
            return DetectedConceptList()

        login_info = self._ensure_login_info()

        task = ConceptExtractionTask(text=text)

        url = (
            login_info["host"]
            + "/text/wikify?"
            + urlencode(task.to_wikify_query_params())
        )

        response = self._request(
            url=url,
            login_info=login_info,
            request_func=post,
            headers={"Content-Type": "application/json"},
            json=task.to_wikify_payload(),
            timeout=900,
            max_tries=15,
        )

        data = response.json()

        return DetectedConceptList(
            concept_list=[
                self._to_detected_concept(item)
                for item in data
                if isinstance(item, dict)
            ]
        )

    @staticmethod
    def _to_detected_concept(item: dict[str, Any]) -> DetectedConcept:
        return DetectedConcept(
            concept_id=str(item.get("concept_id") or item.get("id") or ""),
            text_source=str(item.get("concept_name")) if item.get("concept_name") is not None else None,
            score=float(item.get("mixed_score", 0.0)),
        )
