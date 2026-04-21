# graphregistry/adapters/gateways/graphai/agt_translation.py
from __future__ import annotations
from datetime import datetime, timedelta
from json import load as load_json
from pathlib import Path
from time import sleep
from typing import Any, Callable, cast
from requests import Response, get, post
from graphregistry.common.config import GlobalConfig, REPO_ROOT
from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.domain.interfaces.gateways.gtw_translation import TextTranslationGateway
from graphregistry.domain.models.entities.mdl_text import LanguageCode, MultilingualText
from graphregistry.domain.models.tasks.mdl_translation import TranslationTask

DIRECT_TRANSLATION_PAIRS: set[tuple[LanguageCode, LanguageCode]] = {
    ("fr", "en"),
    ("en", "fr"),
    ("de", "en"),
    ("en", "de"),
    ("it", "en"),
    ("en", "it"),
}

# --------------------------------------------------------------------------------------
# Translation chunking / retry configuration
# --------------------------------------------------------------------------------------
# If GraphAI says a text is too large, we progressively reduce the chunk size until
# the request becomes acceptable.
MIN_TEXT_LENGTH = 400
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 4000
STEP_AUTO_DECREASE_TEXT_LENGTH = 800

# Supported application languages in the registry
SUPPORTED_LANGUAGES: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")


class GraphAITextTranslationGateway(GraphAIBaseGateway, TextTranslationGateway):
    """
    Concrete adapter for the GraphAI translation API.

    Responsibilities:
    - authenticate against GraphAI
    - submit translation jobs
    - poll async job status
    - retry transient failures
    - split overly large texts into smaller chunks
    - provide a simple domain-friendly interface to the rest of the app

    Non-responsibilities:
    - deciding *when* translation should happen
    - storing translation results
    - mutating domain objects outside the returned payload
    """

    # ----------------------------------------------------------------------------------
    # Construction / setup
    # ----------------------------------------------------------------------------------

    def __init__(
        self,
        graph_api_json: str | Path | None = None,
        login_info: dict[str, Any] | None = None,
        debug: bool = False,
        ) -> None:
        """
        Args:
            graph_api_json:
                Path to the GraphAI client config JSON.
                If omitted, falls back to GlobalConfig().
            login_info:
                Optional already-authenticated login payload.
                Useful for tests or reused sessions.
            debug:
                If True, prints more request-level information.
        """
        self.graph_api_json = self._resolve_graph_api_json(graph_api_json)
        self._login_info = login_info
        self.debug = debug

    # ----------------------------------------------------------------------------------
    # Public gateway API
    # ----------------------------------------------------------------------------------

    def translate_text(
        self,
        text: str,
        source_language: LanguageCode,
        target_language: LanguageCode,
        *,
        force: bool = False,
        no_cache: bool = False,
        skip_segmentation: bool = False,
        clean: bool = False,
        ) -> str:
        """
        Translate one text string.

        Returns:
            The translated text.
            If translation failed: no string result returned.

        Notes:
            - Same-language requests return the original text unchanged.
            - Empty text returns immediately.
            - This method intentionally returns a string instead of raising on normal
              translation failure, because the surrounding app often wants to degrade
              gracefully.
        """
        if not text or source_language == target_language:
            return text

        login_info = self._ensure_login_info()

        result = self._translate_any(
            text=text,
            source_language=source_language,
            target_language=target_language,
            login_info=login_info,
            force=force,
            no_cache=no_cache,
            skip_segmentation=skip_segmentation,
            clean=clean,
            debug=self.debug,
        )

        if not isinstance(result, str):
            raise RuntimeError("Translation failed: no string result returned.")
        return result

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: LanguageCode,
        target_languages: tuple[LanguageCode, ...] = SUPPORTED_LANGUAGES,
        ) -> MultilingualText:
        """
        Translate a multilingual container by filling only missing target fields.

        Behavior:
        - copies the input object
        - reads source text from `source_language`
        - translates only into empty target language fields
        - never overwrites existing non-empty target text
        """
        out = text.model_copy(deep=True)
        source_value = getattr(text, source_language, "").strip()

        if not source_value:
            return out

        for lang in target_languages:
            if lang == source_language:
                continue

            existing_value = getattr(out, lang, "").strip()
            if existing_value:
                continue

            translated = self.translate_text(
                text=source_value,
                source_language=source_language,
                target_language=lang,
            )

            if translated:
                setattr(out, lang, translated)

        return out

    # ----------------------------------------------------------------------------------
    # Core translation dispatcher
    # ----------------------------------------------------------------------------------

    def _translate_any(
        self,
        text: str | list[str | None],
        source_language: LanguageCode,
        target_language: LanguageCode,
        login_info: dict[str, Any],
        **kwargs: Any,
        ) -> str | list[str | None] | None:
        """
        Internal generic translation dispatcher.

        Handles:
        - same-language short-circuit
        - intermediary English routing for unsupported direct pairs
        - dispatch to str vs list translation implementations
        """
        if source_language == target_language:
            return text



        if (source_language, target_language) not in DIRECT_TRANSLATION_PAIRS:
            translated_to_en = self._translate_any(
                text=text,
                source_language=source_language,
                target_language="en",
                login_info=login_info,
                **kwargs,
            )
            if translated_to_en is None:
                return None

            return self._translate_any(
                text=translated_to_en,
                source_language="en",
                target_language=target_language,
                login_info=login_info,
                **kwargs,
            )






        if isinstance(text, str):
            return self._translate_string(
                text=text,
                source_language=source_language,
                target_language=target_language,
                login_info=login_info,
                **kwargs,
            )

        return self._translate_list(
            texts=text,
            source_language=source_language,
            target_language=target_language,
            login_info=login_info,
            **kwargs,
        )

    # ----------------------------------------------------------------------------------
    # String translation
    # ----------------------------------------------------------------------------------

    def _translate_string(
        self,
        text: str,
        source_language: LanguageCode,
        target_language: LanguageCode,
        login_info: dict[str, Any],
        *,
        force: bool = False,
        no_cache: bool = False,
        skip_segmentation: bool = False,
        clean: bool = False,
        debug: bool = False,
        max_text_length: int | None = None,
        max_text_list_length: int = 20000,
        max_tries: int = 5,
        max_processing_time_s: int = 3600,
        ) -> str | None:
        """
        Translate one string.

        If the text is too large:
        - either pre-split if max_text_length is already known
        - or let GraphAI reject it once, then retry with smaller chunks
        """
        if not text:
            return text

        # Pre-split if caller already knows the chunk size limit.
        if max_text_length is not None and len(text) > max_text_length:
            chunks = self._split_text(text, max_text_length)
            translated_chunks = self._translate_list(
                texts = cast(list[str | None], chunks),
                source_language=source_language,
                target_language=target_language,
                login_info=login_info,
                force=force,
                no_cache=no_cache,
                skip_segmentation=skip_segmentation,
                clean=clean,
                debug=debug,
                max_text_length=max_text_length,
                max_text_list_length=max_text_list_length,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                mapping_from_input_to_original={i: 0 for i in range(len(chunks))},
            )
            return "".join(chunk or "" for chunk in (translated_chunks or []))

        task = TranslationTask(
            text=text,
            source=source_language,
            target=target_language,
            force=force,
            no_cache=no_cache,
            skip_segmentation=skip_segmentation,
            clean=clean,
        )
        payload = task.to_api_payload()

        task_result = self._call_async_endpoint(
            endpoint="/translation/translate",
            payload=payload,
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
        )

        if task_result is None:
            return None

        # GraphAI tells us this text is too large -> retry with chunking.
        if task_result.get("text_too_large", False):
            next_max_text_length = self._get_next_text_length_for_split(
                text_length=len(text),
                previous_text_length=max_text_length,
            )
            return self._translate_string(
                text=text,
                source_language=source_language,
                target_language=target_language,
                login_info=login_info,
                force=force,
                no_cache=no_cache,
                skip_segmentation=skip_segmentation,
                clean=clean,
                debug=debug,
                max_text_length=next_max_text_length,
                max_text_list_length=max_text_list_length,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
            )

        result = task_result.get("result")
        return str(result).strip() if isinstance(result, str) else None

    # ----------------------------------------------------------------------------------
    # List translation
    # ----------------------------------------------------------------------------------

    def _translate_list(
        self,
        texts: list[str | None],
        source_language: LanguageCode,
        target_language: LanguageCode,
        login_info: dict[str, Any],
        *,
        force: bool = False,
        no_cache: bool = False,
        skip_segmentation: bool = False,
        clean: bool = False,
        debug: bool = False,
        max_text_length: int | None = None,
        max_text_list_length: int = 20000,
        max_tries: int = 5,
        max_processing_time_s: int = 3600,
        mapping_from_input_to_original: dict[int, int] | None = None,
        num_output: int | None = None,
        ) -> list[str | None] | None:
        """
        Translate a list of strings while preserving original list shape.

        Important behavior:
        - removes None / empty items before sending to GraphAI
        - optionally splits overly long individual elements
        - after translation, reconstructs the original output layout
        """
        if mapping_from_input_to_original is None and num_output is None:
            num_output = len(texts)

        cleaned_texts, cleaned_to_original_mapping = self._clean_list_of_texts(
            texts,
            mapping_from_input_to_original,
        )

        if not cleaned_texts:
            return cast(list[str | None], cleaned_texts)

        if all(not line or not line.strip() for line in cleaned_texts):
            return cast(list[str | None], cleaned_texts)

        split_texts, split_to_original_mapping = self._limit_length_list_of_texts(
            cast(list[str | None], cleaned_texts),
            max_text_length=max_text_length,
            mapping_from_split_to_original=cleaned_to_original_mapping,
        )

        task = TranslationTask(
            text=split_texts,
            source=source_language,
            target=target_language,
            force=force,
            no_cache=no_cache,
            skip_segmentation=skip_segmentation,
            clean=clean,
        )

        payload = task.to_api_payload()

        task_result = self._call_async_endpoint(
            endpoint="/translation/translate",
            payload=payload,
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
        )

        if task_result is None:
            return None

        result = task_result.get("result")
        if not isinstance(result, list):
            return None

        # Stitch split chunks back together so the output shape matches the original.
        return self._recombine_split_list_of_texts(
            list_of_texts_split=result,
            mapping_from_split_to_original=split_to_original_mapping,
            output_length=num_output,
        )

    # ----------------------------------------------------------------------------------
    # Text list cleanup helpers
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _clean_list_of_texts(
        list_of_texts: list[str | None],
        mapping_from_input_to_original: dict[int, int] | None = None,
        ) -> tuple[list[str], dict[int, int]]:
        """
        Remove None / empty entries before calling the API.

        Returns:
            cleaned_texts
            mapping from cleaned index -> original logical index
        """
        if mapping_from_input_to_original is None:
            mapping_from_input_to_original = {i: i for i in range(len(list_of_texts))}

        kept_indices = [
            idx
            for idx, value in enumerate(list_of_texts)
            if value is not None and isinstance(value, str) and len(value) > 0
        ]

        cleaned_texts = [list_of_texts[idx] for idx in kept_indices]
        cleaned_to_original_mapping = {
            new_idx: mapping_from_input_to_original[old_idx]
            for new_idx, old_idx in enumerate(kept_indices)
        }

        return cast(list[str], cleaned_texts), cleaned_to_original_mapping

    @staticmethod
    def _recombine_split_list_of_texts(
        list_of_texts_split: list[str | None],
        mapping_from_split_to_original: dict[int, int] | None,
        output_length: int | None = None,
        ) -> list[str | None]:
        """
        Recombine split translated chunks back into their original logical items.

        Example:
            original line 0 got split into 3 chunks
            translated chunk 0,1,2 are concatenated back into output[0]
        """
        if mapping_from_split_to_original is None:
            mapping_from_split_to_original = {i: i for i in range(len(list_of_texts_split))}

        if output_length is None:
            output_length = max(mapping_from_split_to_original.values()) + 1

        output: list[str | None] = [None] * output_length

        for split_idx, translated_chunk in enumerate(list_of_texts_split):
            original_idx = mapping_from_split_to_original[split_idx]

            if output[original_idx] is None:
                output[original_idx] = translated_chunk
            else:
                output[original_idx] = (output[original_idx] or "") + (translated_chunk or "")

        return cast(list[str | None], output)

    # ----------------------------------------------------------------------------------
    # Chunking helpers
    # ----------------------------------------------------------------------------------

    def _limit_length_list_of_texts(
        self,
        list_of_texts: list[str | None],
        max_text_length: int | None = None,
        mapping_from_split_to_original: dict[int, int] | None = None,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " "),
        ) -> tuple[list[str], dict[int, int]]:
        """
        Ensure that no individual text item exceeds `max_text_length`.

        If an item is too long:
        - reassemble all chunks belonging to the same original logical item
        - split it again more carefully using `_split_text()`

        Returns:
            split_texts
            mapping from split index -> original logical index
        """
        if mapping_from_split_to_original is None:
            mapping_from_split_to_original = {i: i for i in range(len(list_of_texts))}

        if max_text_length is None:
            return cast(list[str], list_of_texts), mapping_from_split_to_original

        split_texts: list[str] = []
        split_to_original_mapping: dict[int, int] = {}

        for original_idx in sorted(set(mapping_from_split_to_original.values())):
            current_split_indices = sorted(
                split_idx
                for split_idx, orig_idx in mapping_from_split_to_original.items()
                if orig_idx == original_idx
            )

            is_too_long = any(
                len(cast(str, list_of_texts[split_idx])) > max_text_length
                for split_idx in current_split_indices
            )

            if is_too_long:
                full_text = "".join(cast(str, list_of_texts[split_idx]) for split_idx in current_split_indices)
                portions = self._split_text(
                    full_text,
                    max_length=max_text_length,
                    split_characters=split_characters,
                )
                for portion in portions:
                    split_to_original_mapping[len(split_texts)] = original_idx
                    split_texts.append(portion)
            else:
                for split_idx in current_split_indices:
                    split_to_original_mapping[len(split_texts)] = original_idx
                    split_texts.append(cast(str, list_of_texts[split_idx]))

        return split_texts, split_to_original_mapping

    @staticmethod
    def _get_next_text_length_for_split(
        text_length: int,
        previous_text_length: int | None = None,
        text_length_min: int = MIN_TEXT_LENGTH,
        max_text_length_default: int = DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG,
        text_length_steps: int = STEP_AUTO_DECREASE_TEXT_LENGTH,
        ) -> int:
        """
        Compute the next chunk size when GraphAI says the text is too large.
        """
        if previous_text_length == text_length_min:
            raise ValueError("text_too_long while already at minimum chunk size")

        if previous_text_length is None:
            return min(
                max_text_length_default,
                max(text_length_min, text_length - text_length_steps),
            )

        return max(previous_text_length - text_length_steps, text_length_min)

    @staticmethod
    def _split_text(
        text: str,
        max_length: int,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " "),
        ) -> list[str]:
        """
        Split text into chunks no longer than `max_length`.

        Strategy:
        - try splitting at a "nice" boundary first
        - if no good boundary exists, hard-cut at max_length
        """
        chunks: list[str] = []

        while len(text) > max_length:
            split_position = -1

            for split_char in split_characters:
                split_position = text[:max_length].rfind(split_char)
                if split_position > 0:
                    break

            if split_position > 0:
                chunks.append(text[: split_position + 1])
                text = text[split_position + 1 :]
            else:
                chunks.append(text[:max_length])
                text = text[max_length:]

        if text:
            chunks.append(text)

        return chunks