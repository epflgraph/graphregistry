#!/usr/bin/env python3
"""Example: extract text from a slide/image token using OCR."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_image import GraphAIImageGateway


def main() -> None:
    # Replace with a real image/slide token returned by GraphAIVideoGateway.extract_slides().
    slide_token = "SLIDE-TOKEN-PLACEHOLDER"

    gateway = GraphAIImageGateway()
    result = gateway.extract_text_from_slide(slide_token=slide_token)

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
