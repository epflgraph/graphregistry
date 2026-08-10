# graphregistry/application/ports/gateways/prt_embedding.py
from __future__ import annotations
from typing import Protocol


# Model definition
class TextEmbeddingGateway(Protocol):

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
        launch_only: bool = False,
    ) -> list[float] | list[list[float] | None] | str | list[str | None] | None:
        ...

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
        launch_only: bool = False,
    ) -> list[float] | str | None:
        ...

    def embed_text_list(
        self,
        list_of_texts: list[str | None],
        *,
        model: str | None = None,
        force: bool = False,
        max_text_length: int | None = None,
        max_text_list_length: int = 20000,
        max_tries: int = 5,
        max_processing_time_s: int = 600,
        split_characters: tuple[str, ...] = ("\n", ".", ";", ",", " "),
        no_cache: bool = False,
        launch_only: bool = False,
    ) -> list[list[float] | None] | list[str | None] | None:
        ...
