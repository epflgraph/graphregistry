#!/usr/bin/env python3
"""Example: compute an embedding vector for a single string."""
from __future__ import annotations

import time
import types

from graphregistry.adapters.gateways.graphai.agt_embedding import GraphAIEmbeddingGateway


def main() -> None:
    text = "Machine learning is a branch of artificial intelligence."

    gateway = GraphAIEmbeddingGateway()

    # Temporary timing instrumentation to diagnose slow cached runs.
    original_request = gateway._request.__func__

    def timed_request(self, url, login_info, request_func=None, *, max_tries=5, timeout=600, **kwargs):
        t0 = time.monotonic()
        try:
            result = original_request(self, url, login_info, request_func, max_tries=max_tries, timeout=timeout, **kwargs)
            print(f"  [timing] {request_func.__name__.upper()} {url} OK {time.monotonic() - t0:.2f}s")
            return result
        except Exception as e:
            print(f"  [timing] {request_func.__name__.upper()} {url} FAIL {time.monotonic() - t0:.2f}s: {e}")
            raise

    gateway._request = types.MethodType(timed_request, gateway)

    # Use max_tries=1 in the example so an unreachable GraphAI service fails
    # fast rather than retrying for minutes. Remove it in production code.
    t0 = time.monotonic()
    embedding = gateway.embed_text_str(text=text, max_tries=1)
    print(f"  [timing] total {time.monotonic() - t0:.2f}s")

    if embedding is None:
        print("Embedding failed.")
        return

    if isinstance(embedding, str):
        print(f"Async task launched; task_id={embedding}")
        return

    print(f"Embedding dimension: {len(embedding)}")
    print(f"First 5 values: {embedding[:5]}")


if __name__ == "__main__":
    main()
