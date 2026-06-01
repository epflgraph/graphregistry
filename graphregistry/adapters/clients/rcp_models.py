# graphregistry/adapters/clients/rcp_llm_models.py
from __future__ import annotations

from typing import Any, Mapping, Sequence, TypeVar, overload

from loguru import logger as sysmsg
from openai import OpenAI, OpenAIError
from pydantic import BaseModel, ValidationError

from graphregistry.common.config import GlobalConfig



PydanticModelT = TypeVar("PydanticModelT", bound=BaseModel)

ChatMessage = Mapping[str, Any]


class RCPModelsClient:
    """Client for RCP-hosted OpenAI-compatible chat llm_models."""

    def __init__(
        self,
        *,
        llm_base_url: str | None = None,
        llm_api_key: str | None = None,
        llm_model: str | None = None,
        timeout: float = 30.0,
        config: GlobalConfig | None = None,
    ) -> None:
        cfg = config or GlobalConfig()
        rcp_cfg = cfg.settings.get("llm", {}).get("rcp", {})

        self.llm_base_url = llm_base_url or rcp_cfg.get("llm_base_url")
        self.llm_api_key = llm_api_key or rcp_cfg.get("llm_api_key")
        self.llm_model = llm_model or rcp_cfg.get("llm_model")
        self.timeout = timeout

        missing = [
            name
            for name, value in {
                "llm_base_url": self.llm_base_url,
                "llm_api_key": self.llm_api_key,
                "llm_model": self.llm_model,
            }.items()
            if not value
        ]

        if missing:
            raise ValueError(
                "Missing RCP LLM configuration: "
                + ", ".join(missing)
                + ". Expected config_global.yaml -> llm.rcp.{llm_base_url, llm_api_key, llm_model}, "
                "or pass values explicitly."
            )

        self.client = OpenAI(
            base_url=self.llm_base_url,
            api_key=self.llm_api_key,
            timeout=self.timeout,
        )

    @staticmethod
    def _response_format(
        llm_model_cls: type[BaseModel] | None,
    ) -> dict[str, Any] | None:
        if llm_model_cls is None:
            return None

        return {
            "type": "json_schema",
            "json_schema": {
                "name": llm_model_cls.__name__,
                "schema": llm_model_cls.llm_model_json_schema(),
                "strict": True,
            },
        }

    @overload
    def chat(
        self,
        messages: Sequence[ChatMessage],
        *,
        response_llm_model: type[PydanticModelT],
        llm_model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout: float | None = None,
    ) -> PydanticModelT: ...

    @overload
    def chat(
        self,
        messages: Sequence[ChatMessage],
        *,
        response_llm_model: None = None,
        llm_model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout: float | None = None,
    ) -> str: ...

    def chat(
        self,
        messages: Sequence[ChatMessage],
        *,
        response_llm_model: type[PydanticModelT] | None = None,
        llm_model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        timeout: float | None = None,
    ) -> str | PydanticModelT:
        request: dict[str, Any] = {
            "llm_model": llm_model or self.llm_model,
            "messages": list(messages),
        }

        if max_tokens is not None:
            request["max_tokens"] = max_tokens

        if temperature is not None:
            request["temperature"] = temperature

        if timeout is not None:
            request["timeout"] = timeout

        response_format = self._response_format(response_llm_model)
        if response_format is not None:
            request["response_format"] = response_format

        try:
            response = self.client.chat.completions.create(**request)
        except OpenAIError:
            sysmsg.exception("RCP LLM request failed.")
            raise

        content = response.choices[0].message.content

        if not content:
            raise ValueError("RCP LLM returned empty content.")

        content = content.strip()

        if response_llm_model is None:
            return content

        try:
            return response_llm_model.llm_model_validate_json(content)
        except ValidationError as exc:
            raise ValueError(
                f"RCP LLM response did not match {response_llm_model.__name__}: {content}"
            ) from exc


@overload
def send_llm_request(
    messages: Sequence[ChatMessage],
    *,
    response_llm_model: type[PydanticModelT],
    max_tokens: int | None = None,
    timeout: float = 30.0,
    llm_model: str | None = None,
    llm_base_url: str | None = None,
    llm_api_key: str | None = None,
    temperature: float | None = None,
) -> PydanticModelT: ...


@overload
def send_llm_request(
    messages: Sequence[ChatMessage],
    *,
    response_llm_model: None = None,
    max_tokens: int | None = None,
    timeout: float = 30.0,
    llm_model: str | None = None,
    llm_base_url: str | None = None,
    llm_api_key: str | None = None,
    temperature: float | None = None,
) -> str: ...


def send_llm_request(
    messages: Sequence[ChatMessage],
    *,
    response_llm_model: type[PydanticModelT] | None = None,
    max_tokens: int | None = None,
    timeout: float = 30.0,
    llm_model: str | None = None,
    llm_base_url: str | None = None,
    llm_api_key: str | None = None,
    temperature: float | None = None,
) -> str | PydanticModelT:
    client = RCPModelsClient(
        llm_base_url=llm_base_url,
        llm_api_key=llm_api_key,
        llm_model=llm_model,
        timeout=timeout,
    )

    return client.chat(
        messages,
        response_llm_model=response_llm_model,
        max_tokens=max_tokens,
        temperature=temperature,
    )
