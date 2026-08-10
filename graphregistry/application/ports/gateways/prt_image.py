# graphregistry/application/ports/gateways/prt_image.py
from __future__ import annotations
from typing import Protocol


# Model definition
class ImageProcessingGateway(Protocol):

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
        ...

    def calculate_fingerprint(
        self,
        slide_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 120,
        launch_only: bool = False,
    ) -> str | None:
        ...
