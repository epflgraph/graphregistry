# graphregistry/adapters/graphai/adp_conceptgatw.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Callable
from graphregistry.common.config import GlobalConfig, REPO_ROOT
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
from graphregistry.domain.models.mdl_concept import DetectedConcept, DetectedConceptList

# Type aliases for better readability
LoginFn = Callable[[str], dict[str, Any]]
ExtractFn = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

# Class definition
class GraphAIConceptGateway(ConceptGateway):
    """
    Gateway adapter that bridges the domain `ConceptGateway` protocol to GraphAI.
    """

    def __init__(
        self,
        graph_api_json: str | Path | None = None,
        login_info: dict[str, Any] | None = None,
        login_fn: LoginFn | None = None,
        extract_fn: ExtractFn | None = None,
    ) -> None:
        self.graph_api_json = self._resolve_graph_api_json(graph_api_json)
        self._login_info = login_info
        self._login_fn = login_fn
        self._extract_fn = extract_fn

    def detect_concepts(self, text: str) -> DetectedConceptList:
        if not text or not text.strip():
            return DetectedConceptList()

        login_info = self._ensure_login_info()
        raw_results = self._get_extract_fn()(text, login_info)
        concept_list = [self._to_detected_concept(item) for item in raw_results if isinstance(item, dict)]
        return DetectedConceptList(concept_list=[c for c in concept_list if c is not None])

    def _ensure_login_info(self) -> dict[str, Any]:
        if self._login_info is not None:
            return self._login_info

        login_info = self._get_login_fn()(str(self.graph_api_json))
        if not isinstance(login_info, dict) or not login_info:
            raise ValueError("Failed to obtain valid GraphAI login info.")
        self._login_info = login_info
        return login_info

    def _get_login_fn(self) -> LoginFn:
        if self._login_fn is not None:
            return self._login_fn

        from graphai_client.client import login as graphai_login

        self._login_fn = graphai_login
        return self._login_fn

    def _get_extract_fn(self) -> ExtractFn:
        if self._extract_fn is not None:
            return self._extract_fn

        from graphai_client.client_api.text import extract_concepts_from_text

        self._extract_fn = extract_concepts_from_text
        return self._extract_fn

    @staticmethod
    def _resolve_graph_api_json(graph_api_json: str | Path | None) -> Path:
        if graph_api_json is None:
            glbcfg = GlobalConfig()
            graph_api_json = glbcfg.settings["graphai"]["client_config_file"]

        config_path = Path(graph_api_json)
        if config_path.is_absolute():
            return config_path
        return REPO_ROOT / config_path

    @staticmethod
    def _to_detected_concept(item: dict[str, Any]) -> DetectedConcept | None:
        concept_id = item.get("concept_id") or item.get("id") or item.get("concept")
        if concept_id is None:
            return None

        score_raw = item.get("score", item.get("confidence", 1.0))
        try:
            score = float(score_raw)
        except (TypeError, ValueError):
            score = 0.0

        text_source_raw = item.get("text_source")
        text_source = str(text_source_raw) if text_source_raw is not None else None

        return DetectedConcept(
            concept_id=str(concept_id),
            score=score,
            text_source=text_source,
        )
