#!/usr/bin/env python3
"""Example: download the video file bytes for a video token.

The output file path is required so the downloaded bytes are always saved to
a known location, even when the transfer is very slow.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import faulthandler

from graphregistry.adapters.gateways.graphai.gtw_video import GraphAIVideoGateway

faulthandler.enable()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download a video file from GraphAI by token."
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        type=Path,
        help="Output file path where the downloaded video will be saved.",
    )
    parser.add_argument(
        "--token",
        "-t",
        default="177581471705447505981400.mp4",
        help="Video token returned by get_video() (default: %(default)s).",
    )
    args = parser.parse_args()

    output_path: Path = args.output
    video_token: str = args.token

    gateway = GraphAIVideoGateway()
    result = gateway.download_file(token=video_token, file_path=output_path)

    if result is None:
        print("Download failed.")
        raise SystemExit(1)

    print(f"Downloaded video to {result} ({result.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
