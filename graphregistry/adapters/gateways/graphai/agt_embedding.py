# graphregistry/adapters/gateways/graphai/agt_embedding.py
from __future__ import annotations

from json import JSONDecodeError, loads
from typing import Any

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway

MIN_TEXT_LENGTH = 128
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 600
STEP_AUTO_DECREASE_TEXT_LENGTH = 100


class GraphAIEmbeddingGateway(GraphAIBaseGateway):
    def embed_text(
        self,
        text: str | list[str | None],
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_tries: int = 5,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " ", "$"),
        no_cache: bool = False,
    ) -> list[float] | list[list[float] | None] | None:
        if isinstance(text, str):
            return self.embed_text_str(
                text=text,
                model=model,
                force=force,
                max_text_length=max_text_length,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                split_characters=split_characters,
                no_cache=no_cache,
            )

        return self.embed_text_list(
            list_of_texts=text,
            model=model,
            force=force,
            max_text_length=max_text_length,
            max_tries=max_tries,
            max_processing_time_s=max_processing_time_s,
            split_characters=split_characters,
            no_cache=no_cache,
        )

    def embed_text_str(
        self,
        text: str,
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_tries: int = 5,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " ", "$"),
        no_cache: bool = False,
    ) -> list[float] | None:
        if max_text_length and len(text) > max_text_length:
            chunks = self._split_text(text, max_text_length=max_text_length, split_characters=split_characters)
            chunk_embeddings = self.embed_text_list(
                list_of_texts=chunks,
                model=model,
                force=force,
                max_text_length=max_text_length,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                split_characters=split_characters,
                no_cache=no_cache,
            )
            if not isinstance(chunk_embeddings, list) or not chunk_embeddings:
                return None
            valid_chunks = [item for item in chunk_embeddings if isinstance(item, list)]
            if not valid_chunks:
                return None
            return self._weighted_average_embeddings(valid_chunks, [len(c) for c in chunks[: len(valid_chunks)]])

        login_info = self._ensure_login_info()
        payload: dict[str, Any] = {"text": text, "force": force, "no_cache": no_cache}
        if model:
            payload["model_type"] = model

        task_result = self._call_async_endpoint(
            endpoint="/embedding/embed",
            payload=payload,
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
        )
        if task_result is None:
            return None

        if task_result.get("text_too_large", False):
            next_len = self._next_text_length_for_split(len(text), previous_text_length=max_text_length)
            if next_len is None:
                return None
            return self.embed_text_str(
                text=text,
                model=model,
                force=force,
                max_text_length=next_len,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                split_characters=split_characters,
                no_cache=no_cache,
            )

        return self._parse_embedding_payload(task_result.get("result"))

    def embed_text_list(
        self,
        list_of_texts: list[str | None],
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_tries: int = 5,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " "),
        no_cache: bool = False,
    ) -> list[list[float] | None] | None:
        results: list[list[float] | None] = []
        for text in list_of_texts:
            if text is None:
                results.append(None)
                continue
            results.append(
                self.embed_text_str(
                    text=text,
                    model=model,
                    force=force,
                    max_text_length=max_text_length,
                    max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s,
                    split_characters=split_characters,
                    no_cache=no_cache,
                )
            )
        return results

    @staticmethod
    def _parse_embedding_payload(value: Any) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, list):
            try:
                return [float(item) for item in value]
            except (TypeError, ValueError):
                return None
        if isinstance(value, str):
            try:
                parsed = loads(value)
            except JSONDecodeError:
                return None
            if isinstance(parsed, list):
                try:
                    return [float(item) for item in parsed]
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def _next_text_length_for_split(text_length: int, previous_text_length: int | None) -> int | None:
        if previous_text_length is None:
            candidate = min(DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG, text_length)
        else:
            candidate = previous_text_length - STEP_AUTO_DECREASE_TEXT_LENGTH
        if candidate < MIN_TEXT_LENGTH:
            return None
        return candidate

    @staticmethod
    def _split_text(
        text: str,
        *,
        max_text_length: int,
        split_characters: tuple[str, ...],
    ) -> list[str]:
        if len(text) <= max_text_length:
            return [text]

        pieces: list[str] = []
        remaining = text
        while len(remaining) > max_text_length:
            split_index = -1
            for split_char in split_characters:
                idx = remaining.rfind(split_char, 0, max_text_length + 1)
                if idx > split_index:
                    split_index = idx
            if split_index <= 0:
                split_index = max_text_length
            head = remaining[:split_index].strip()
            if head:
                pieces.append(head)
            remaining = remaining[split_index:].strip()

        if remaining:
            pieces.append(remaining)

        return pieces or [text]

    @staticmethod
    def _weighted_average_embeddings(
        embeddings: list[list[float]],
        weights: list[int],
    ) -> list[float]:
        vector_len = len(embeddings[0])
        weighted_sum = [0.0] * vector_len
        total_weight = 0.0

        for embedding, weight in zip(embeddings, weights):
            if len(embedding) != vector_len:
                continue
            weight_float = float(max(weight, 1))
            total_weight += weight_float
            for idx, value in enumerate(embedding):
                weighted_sum[idx] += float(value) * weight_float

        if total_weight == 0:
            return embeddings[0]

        return [value / total_weight for value in weighted_sum]
