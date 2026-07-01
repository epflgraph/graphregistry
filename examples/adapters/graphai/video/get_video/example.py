#!/usr/bin/env python3
"""Example: download a video from a URL and obtain a GraphAI video token."""
from __future__ import annotations

from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway


def main() -> None:
    # Replace with a real video URL GraphAI can reach.
    video_url = "https://raw.githubusercontent.com/epflgraph/graphregistry/master/scripts/init/sample_sets/MATH-132_Lecture_01.mp4"

    gateway = GraphAIVideoGateway()
    video = gateway.get_video(file_url=video_url)

    if video is None:
        print("Video download failed or returned no result.")
        return

    if isinstance(video, str):
        print(f"Async task launched; task_id={video}")
        return

    print("Video downloaded successfully:")
    print(f"  token      = {video.token}")
    print(f"  file_url   = {video.file_url}")
    print(f"  fingerprint= {video.fingerprint}")
    print(f"  duration   = {video.duration}")
    print(f"  resolution = {video.resolution}")


if __name__ == "__main__":
    main()
