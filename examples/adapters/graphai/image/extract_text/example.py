#!/usr/bin/env python3
"""Example: extract text from a slide/image token using OCR.

Reads the Google Cloud API key from the ``GOOGLE_CLOUD_API_KEY`` variable in
``.env`` at the repo root.
"""
from __future__ import annotations

import os
from pathlib import Path

from graphregistry.adapters.gateways.graphai.agt_image import GraphAIImageGateway


def _load_google_api_key() -> str | None:
    """Return GOOGLE_CLOUD_API_KEY from the environment or from ``.env``."""
    env_key = os.environ.get("GOOGLE_CLOUD_API_KEY")
    if env_key:
        return env_key

    # python-dotenv is not a dependency, so parse .env manually.
    env_path = Path(__file__).resolve().parents[5] / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == "GOOGLE_CLOUD_API_KEY":
            return value.strip().strip('"').strip("'")

    return None


def main() -> None:
    # Replace with a real image/slide token returned by GraphAIVideoGateway.extract_slides().
    slide_token = "175887418976912501285267.mp4_slides/frame-000062.png"

    google_api_token = _load_google_api_key()
    if google_api_token is None:
        print("GOOGLE_CLOUD_API_KEY not found in environment or .env")
        raise SystemExit(1)

    gateway = GraphAIImageGateway()
    result = gateway.extract_text_from_slide(
        slide_token=slide_token,
        google_api_token=google_api_token,
    )

    if result is None:
        print("OCR failed.")
        return

    if isinstance(result, str):
        print(f"Async task launched; task_id={result}")
        return

    print(f"Detected language: {result.get('language')}")
    print(f"Extracted text:\n{result.get('text')}")


if __name__ == "__main__":
    main()
