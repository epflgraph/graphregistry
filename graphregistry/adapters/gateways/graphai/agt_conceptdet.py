# graphregistry/adapters/gateways/graphai/agt_conceptdet.py
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
from graphregistry.domain.models.tasks.mdl_conceptdet import (
    ConceptDetectionTask,
    ConceptDetectionResult,
    ConceptDetectionResultList,
)
from requests import post


class GraphAIConceptGateway(GraphAIBaseGateway, ConceptGateway):
    def detect_concepts(self, text: str) -> ConceptDetectionResultList:
        if not text or not text.strip():
            return ConceptDetectionResultList()

        login_info = self._ensure_login_info()

        task = ConceptDetectionTask(text=text)

        url = (
            login_info["host"]
            + "/text/wikify?"
            + urlencode(task.get_params_dict())
        )

        response = self._request(
            url=url,
            login_info=login_info,
            request_func=post,
            headers={"Content-Type": "application/json"},
            json=task.get_payload_dict(),
            timeout=900,
            max_tries=5,
        )

        data = response.json()

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
            concept_id=str(item.get("concept_id") or item.get("id") or ""),
            concept_name=str(item.get("concept_name") or item.get("name") or ""),
            score=float(item.get("mixed_score", 0.0)),
        )
