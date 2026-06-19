# graphregistry/adapters/gateways/graphai/agt_image.py
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway


class GraphAIImageGateway(GraphAIBaseGateway):
    def extract_text_from_slide(
        self,
        slide_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 600,
        ocr_model: str = "google",
        google_api_token: str | None = None,
        launch_only: bool = False,
    ) -> dict[str, str] | str | None:
        login_info = self._ensure_login_info()

        task_result = self._call_async_endpoint(
            endpoint="/image/extract_text",
            payload={
                "token": slide_token,
                "method": ocr_model,
                "force": force,
                "google_api_token": google_api_token,
            },
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        assert isinstance(task_result, dict)
        language = str(task_result.get("language", ""))
        result_items = task_result.get("result")
        if not isinstance(result_items, list) or not result_items:
            return None

        preferred = next(
            (
                item
                for item in result_items
                if isinstance(item, dict)
                and item.get("method") in {"ocr_google_1_token", "ocr_google_1_results"}
            ),
            None,
        )
        chosen = preferred if preferred is not None else result_items[0]

        if not isinstance(chosen, dict):
            return None

        return {
            "text": str(chosen.get("text", "")),
            "language": language,
        }

    def calculate_fingerprint(
        self,
        slide_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 120,
        launch_only: bool = False,
    ) -> str | None:
        login_info = self._ensure_login_info()

        task_result = self._call_async_endpoint(
            endpoint="/image/calculate_fingerprint",
            payload={"token": slide_token, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        assert isinstance(task_result, dict)
        result = task_result.get("result")
        return str(result) if result is not None else None
