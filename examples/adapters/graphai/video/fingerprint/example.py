#!/usr/bin/env python3
"""Example: calculate a fingerprint for a video token."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video token returned by get_video().
    video_token = "177581471705447505981400.mp4"

    gateway = GraphAIVideoGateway()
    fingerprint = gateway.fingerprint(video_token=video_token)

    if fingerprint is None:
        print("Fingerprint calculation failed.")
        return

    print(f"Video fingerprint: {fingerprint}")


if __name__ == "__main__":
    main()
