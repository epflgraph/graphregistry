#!/usr/bin/env python3
"""Example: calculate a fingerprint for a slide/image token."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_image import GraphAIImageGateway


def main() -> None:
    # Replace with a real image/slide token returned by GraphAIVideoGateway.extract_slides().
    slide_token = "175887418976912501285267.mp4_slides/frame-000062.png"

    gateway = GraphAIImageGateway()
    fingerprint = gateway.calculate_fingerprint(slide_token=slide_token)

    if fingerprint is None:
        print("Slide fingerprint calculation failed.")
        return

    print(f"Slide fingerprint: {fingerprint}")


if __name__ == "__main__":
    main()
