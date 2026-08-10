# graphregistry/adapters/gateways/graphai/gtw_embedding.py
from __future__ import annotations

from json import JSONDecodeError, loads
from math import sqrt
from typing import Any, cast

from graphregistry.adapters.gateways.graphai.gtw_base import GraphAIBaseGateway

MIN_TEXT_LENGTH = 128
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 600
STEP_AUTO_DECREASE_TEXT_LENGTH = 100
DEFAULT_MAX_TEXT_LIST_LENGTH = 20000


class GraphAIEmbeddingGateway(GraphAIBaseGateway):
    def embed_text(
        self,
        text: str | list[str | None],
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_tries: int = 2,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " ", "$"),
        no_cache: bool = False,
        launch_only: bool = False,
    ) -> list[float] | list[list[float] | None] | str | list[str | None] | None:
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
                launch_only=launch_only,
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
            launch_only=launch_only,
        )

    def embed_text_str(
        self,
        text: str,
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_tries: int = 2,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " ", "$"),
        no_cache: bool = False,
        launch_only: bool = False,
    ) -> list[float] | str | None:
        if not launch_only and max_text_length and len(text) > max_text_length:
            chunks = self._split_text(text, max_text_length=max_text_length, split_characters=split_characters)
            chunk_embeddings = self.embed_text_list(
                list_of_texts=cast(list[str | None], chunks),
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
            wait_for_result=not launch_only,
        )
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        assert isinstance(task_result, dict)
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
        max_text_list_length: int = DEFAULT_MAX_TEXT_LIST_LENGTH,
        max_tries: int = 2,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " "),
        no_cache: bool = False,
        launch_only: bool = False,
    ) -> list[list[float] | None] | list[str | None] | None:
        """
        Embed a list of texts, with total-length batching and per-item chunking.

        None / empty placeholders are preserved. Long items are split, batched,
        and recombined into a single embedding per original item.
        """
        if launch_only:
            # launch_only is not supported for the high-level list API because a
            # single call may spawn many async tasks.
            return None

        # 1. Clean None / empty entries and remember where they came from.
        cleaned_texts: list[str] = []
        cleaned_to_original: dict[int, int] = {}
        for idx, value in enumerate(list_of_texts):
            if value is not None and isinstance(value, str) and len(value) > 0:
                cleaned_to_original[len(cleaned_texts)] = idx
                cleaned_texts.append(value)

        if not cleaned_texts:
            return cast(list[list[float] | None], [None] * len(list_of_texts))

        # 2. Split any items that exceed max_text_length.
        split_texts: list[str] = []
        split_to_original: dict[int, int] = {}
        split_weights: list[int] = []
        for cleaned_idx, text in enumerate(cleaned_texts):
            original_idx = cleaned_to_original[cleaned_idx]
            if max_text_length is not None and len(text) > max_text_length:
                chunks = self._split_text(
                    text,
                    max_text_length=max_text_length,
                    split_characters=split_characters,
                )
                for chunk in chunks:
                    split_to_original[len(split_texts)] = original_idx
                    split_texts.append(chunk)
                    split_weights.append(len(chunk))
            else:
                split_to_original[len(split_texts)] = original_idx
                split_texts.append(text)
                split_weights.append(len(text))

        # 3. Embed split texts in batches by total length.
        split_embeddings: list[list[float] | None] = [None] * len(split_texts)
        idx_start = 0
        sum_length = 0
        n_splits = len(split_texts)

        for idx in range(n_splits):
            text_length = len(split_texts[idx])
            next_length = len(split_texts[idx + 1]) if idx + 1 < n_splits else 0

            if (
                max_text_list_length is not None
                and sum_length + text_length + next_length > max_text_list_length
                and idx_start < idx
            ):
                self._embed_batch(
                    split_texts=split_texts,
                    start=idx_start,
                    end=idx,
                    result_slot=split_embeddings,
                    model=model,
                    force=force,
                    max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length,
                    max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s,
                    split_characters=split_characters,
                    no_cache=no_cache,
                )
                idx_start = idx
                sum_length = 0

            sum_length += text_length

        if idx_start < n_splits:
            self._embed_batch(
                split_texts=split_texts,
                start=idx_start,
                end=n_splits,
                result_slot=split_embeddings,
                model=model,
                force=force,
                max_text_length=max_text_length,
                max_text_list_length=max_text_list_length,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                split_characters=split_characters,
                no_cache=no_cache,
            )

        # 4. Recombine split embeddings per original item.
        recombined = self._recombine_embeddings(
            split_embeddings,
            split_to_original,
            output_length=len(list_of_texts),
            weights=split_weights,
        )

        # 5. Put back cleaned placeholders as None.
        return [recombined[i] if i in cleaned_to_original.values() else None for i in range(len(list_of_texts))]

    def _embed_batch(
        self,
        *,
        split_texts: list[str],
        start: int,
        end: int,
        result_slot: list[list[float] | None],
        model: str | None,
        force: bool,
        max_text_length: int | None,
        max_text_list_length: int,
        max_tries: int,
        max_processing_time_s: int,
        split_characters: tuple[str, ...],
        no_cache: bool,
    ) -> None:
        """Embed one batch of split texts and write the results into result_slot."""
        batch = split_texts[start:end]
        if not batch:
            return

        login_info = self._ensure_login_info()
        payload: dict[str, Any] = {"text": batch, "force": force, "no_cache": no_cache}
        if model:
            payload["model_type"] = model

        task_result = self._call_async_endpoint(
            endpoint="/embedding/embed",
            payload=payload,
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=True,
        )
        if task_result is None:
            raise RuntimeError(f"Failed to embed text batch: {batch}")

        if isinstance(task_result, dict):
            raw_results, is_too_large = self._parse_batch_result_dict(task_result)
            if is_too_large:
                length_too_long = len(max(batch, key=len))
                next_max_text_length = self._next_text_length_for_split(
                    text_length=length_too_long,
                    previous_text_length=max_text_length,
                )
                if next_max_text_length is None:
                    raise RuntimeError(f"Text too large and cannot split further: {batch}")

                # Re-process this batch with a smaller chunk size.
                sub_result = self.embed_text_list(
                    cast(list[str | None], batch),
                    model=model,
                    force=force,
                    max_text_length=next_max_text_length,
                    max_text_list_length=max_text_list_length,
                    max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s,
                    split_characters=split_characters,
                    no_cache=no_cache,
                )
                if not isinstance(sub_result, list):
                    raise RuntimeError(f"Failed to embed split batch: {batch}")
                for i, value in enumerate(sub_result):
                    result_slot[start + i] = cast(list[float] | None, value)
                return

            if not isinstance(raw_results, list) or len(raw_results) != len(batch):
                raise RuntimeError(
                    f"Invalid embedding result for batch (expected {len(batch)} embeddings): {raw_results}"
                )

            for i, raw in enumerate(raw_results):
                parsed = self._parse_embedding_payload(raw)
                result_slot[start + i] = parsed
            return

        if isinstance(task_result, list):
            if len(task_result) != len(batch):
                raise RuntimeError(
                    f"Invalid embedding result for batch (expected {len(batch)} embeddings): {task_result}"
                )

            # Check for per-item errors / text_too_large signals.
            any_too_large = any(
                isinstance(item, dict) and item.get("text_too_large", False)
                for item in task_result
            )
            if any_too_large:
                length_too_long = len(max(batch, key=len))
                next_max_text_length = self._next_text_length_for_split(
                    text_length=length_too_long,
                    previous_text_length=max_text_length,
                )
                if next_max_text_length is None:
                    raise RuntimeError(f"Text too large and cannot split further: {batch}")

                sub_result = self.embed_text_list(
                    cast(list[str | None], batch),
                    model=model,
                    force=force,
                    max_text_length=next_max_text_length,
                    max_text_list_length=max_text_list_length,
                    max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s,
                    split_characters=split_characters,
                    no_cache=no_cache,
                )
                if not isinstance(sub_result, list):
                    raise RuntimeError(f"Failed to embed split batch: {batch}")
                for i, value in enumerate(sub_result):
                    result_slot[start + i] = cast(list[float] | None, value)
                return

            for i, item in enumerate(task_result):
                if isinstance(item, dict) and not item.get("successful", True):
                    raise RuntimeError(f"GraphAI embedding failed for item {i}: {item}")
                raw = item.get("result") if isinstance(item, dict) else item
                parsed = self._parse_embedding_payload(raw)
                result_slot[start + i] = parsed
            return

        raise RuntimeError(f"Unexpected embedding result type: {type(task_result)}")

    @staticmethod
    def _parse_batch_result_dict(task_result: dict[str, Any]) -> tuple[Any, bool]:
        """Extract raw results and text_too_large flag from a dict-style batch result."""
        if task_result.get("text_too_large", False):
            return None, True
        return task_result.get("result"), False

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
        """
        Compute a length-weighted average of embeddings and L2-normalize it.

        This matches the legacy client's recombination behavior.
        """
        if not embeddings:
            raise ValueError("Cannot average empty embedding list")

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
            averaged = embeddings[0]
        else:
            averaged = [value / total_weight for value in weighted_sum]

        norm = sqrt(sum(value * value for value in averaged))
        if norm == 0:
            return averaged
        return [value / norm for value in averaged]

    @staticmethod
    def _recombine_embeddings(
        embeddings: list[list[float] | None],
        mapping_from_split_to_original: dict[int, int],
        *,
        output_length: int,
        weights: list[int],
    ) -> list[list[float] | None]:
        """
        Recombine split embeddings back into one embedding per original item.

        embeddings[i] belongs to original index mapping_from_split_to_original[i].
        """
        grouped: list[list[list[float]]] = [[] for _ in range(output_length)]
        grouped_weights: list[list[int]] = [[] for _ in range(output_length)]

        for split_idx, embedding in enumerate(embeddings):
            if embedding is None:
                continue
            original_idx = mapping_from_split_to_original[split_idx]
            grouped[original_idx].append(embedding)
            grouped_weights[original_idx].append(weights[split_idx])

        result: list[list[float] | None] = [None] * output_length
        for original_idx in range(output_length):
            group = grouped[original_idx]
            if not group:
                continue
            result[original_idx] = GraphAIEmbeddingGateway._weighted_average_embeddings(
                group,
                grouped_weights[original_idx],
            )

        return result
