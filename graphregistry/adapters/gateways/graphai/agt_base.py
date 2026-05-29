# graphregistry/adapters/gateways/graphai/agt_base.py
from __future__ import annotations

from datetime import datetime, timedelta
from json import load as load_json
from pathlib import Path
from random import uniform
from time import sleep
from typing import Any, Callable, cast

from requests import Response, get, post

from graphregistry.common.config import GlobalConfig, REPO_ROOT


class GraphAIBaseGateway:
    def __init__(
        self,
        graph_api_json: str | Path | None = None,
        login_info: dict[str, Any] | None = None,
        debug: bool = False,
    ) -> None:
        self.graph_api_json = self._resolve_graph_api_json(graph_api_json)
        self._login_info = login_info
        self.debug = debug

    # ----------------------------------------------------------------------------------
    # Authentication / config helpers
    # ----------------------------------------------------------------------------------

    def _ensure_login_info(self) -> dict[str, Any]:
        """
        Lazily authenticate if needed.
        """
        if self._login_info is None or "token" not in self._login_info:
            self._login_info = self._login(str(self.graph_api_json))
        return self._login_info

    @staticmethod
    def _resolve_graph_api_json(graph_api_json: str | Path | None) -> Path:
        """
        Resolve the config file path, either from the explicit argument or from
        GlobalConfig.
        """
        if graph_api_json is None:
            glbcfg = GlobalConfig()
            graph_api_json = glbcfg.settings["graphai"]["client_config_file"]

        path = Path(cast(str, graph_api_json))
        return path if path.is_absolute() else (REPO_ROOT / path)

    def _login(self, graph_api_json: str, max_tries: int = 5) -> dict[str, Any]:
        """
        Authenticate against GraphAI and return the login payload with bearer token.
        """
        with open(graph_api_json) as fp:
            cfg = load_json(fp)

        login_info: dict[str, Any] = {
            "user": cfg["user"],
            "host": f'{cfg["host"]}:{cfg["port"]}',
            "graph_api_json": graph_api_json,
        }

        response = self._request(
            url="/token",
            login_info=login_info,
            request_func=post,
            data={"username": cfg["user"], "password": cfg["password"]},
            max_tries=max_tries,
        )

        login_info["token"] = response.json()["access_token"]
        return login_info

    # ----------------------------------------------------------------------------------
    # Low-level HTTP request helper
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _backoff_seconds(attempt: int) -> int:
        """
        Retry delays: 1, 2, 4, 8, 16, 32, ...
        """
        return min(2 ** (attempt - 1), 32)

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 425, 429, 500, 502, 503, 504}

    @staticmethod
    def _is_server_error(status_code: int) -> bool:
        return 500 <= status_code <= 599

    @staticmethod
    def _jittered_delay(delay_seconds: int) -> float:
        # Keep jitter moderate to avoid synchronized retries against GraphAI.
        return max(0.0, delay_seconds * uniform(0.8, 1.2))

    def _log(self, message: str) -> None:
        if self.debug:
            print(f"[GraphAI] {message}")

    def _request(
        self,
        url: str,
        login_info: dict[str, Any],
        request_func: Callable[..., Response] = get,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        *,
        max_tries: int = 5,
        timeout: int = 600,
    ) -> Response:
        """
        Execute one HTTP request with retry logic and automatic token refresh on 401.
        """
        if not url.startswith("http"):
            url = login_info["host"] + url

        request_headers = dict(headers or {})
        if "token" in login_info:
            request_headers["Authorization"] = f'Bearer {login_info["token"]}'

        for attempt in range(1, max_tries + 1):
            try:
                response = request_func(
                    url,
                    headers=request_headers,
                    json=json,
                    data=data,
                    timeout=timeout,
                )
            except Exception:
                if attempt == max_tries:
                    raise
                delay = self._backoff_seconds(attempt)
                self._log(
                    f"{request_func.__name__.upper()} failed "
                    f"(attempt {attempt}/{max_tries}), retrying in {delay}s..."
                )
                sleep(delay)
                continue

            if response.ok:
                return response

            if response.status_code == 401:
                if "/token" in url:
                    raise RuntimeError("GraphAI authentication failed: invalid credentials")
                self._log("Token expired, refreshing...")
                new_token = self._login(login_info["graph_api_json"])["token"]
                login_info["token"] = new_token
                request_headers["Authorization"] = f"Bearer {new_token}"
                continue

            retryable_status = self._is_retryable_status(response.status_code)

            # Fail faster for unstable auth infrastructure instead of hammering /token.
            if (
                "/token" in url
                and self._is_server_error(response.status_code)
                and attempt >= 2
            ):
                raise RuntimeError(
                    f'HTTP {response.status_code} while calling "{url}": '
                    f'{response.reason} (auth endpoint unstable; aborted early)'
                )

            if retryable_status and attempt < max_tries:
                delay = self._backoff_seconds(attempt)
                jittered_delay = self._jittered_delay(delay)
                self._log(
                    f"{request_func.__name__.upper()} {url} returned "
                    f"{response.status_code}, retrying in {jittered_delay:.2f}s..."
                )
                sleep(jittered_delay)
                continue

            raise RuntimeError(
                f'HTTP {response.status_code} while calling "{url}": {response.reason}'
            )

        raise RuntimeError(f'Failed to call "{url}" after {max_tries} attempts.')

    # ----------------------------------------------------------------------------------
    # Async GraphAI task helper
    # ----------------------------------------------------------------------------------

    def _call_async_endpoint(
        self,
        endpoint: str,
        payload: dict[str, Any],
        login_info: dict[str, Any],
        *,
        max_processing_time_s: int = 6000,
        max_tries: int = 5,
        wait_for_result: bool = True,
    ) -> dict[str, Any] | str | None:
        """
        Submit an async GraphAI job and optionally poll until completion.
        """
        for attempt in range(1, max_tries + 1):
            submit_response = self._request(
                url=endpoint,
                login_info=login_info,
                request_func=post,
                headers={"Content-Type": "application/json"},
                json=payload,
                max_tries=max_tries,
                timeout=60,
            )
            self._log(f"Submitted async task to {endpoint}")

            task_id = submit_response.json()["task_id"]
            if not wait_for_result:
                return str(task_id)

            deadline = datetime.now() + timedelta(seconds=max_processing_time_s)

            while datetime.now() < deadline:
                status_response = self._request(
                    url=f"{endpoint}/status/{task_id}",
                    login_info=login_info,
                    request_func=get,
                    headers={"Content-Type": "application/json"},
                    max_tries=max_tries,
                    timeout=60,
                )

                status_payload = status_response.json()
                task_status = status_payload["task_status"]
                self._log(f"Task {task_id}: {task_status}")

                if task_status in ("PENDING", "STARTED"):
                    sleep(1)
                    continue

                if task_status == "SUCCESS":
                    task_result = status_payload.get("task_result")

                    if not isinstance(task_result, dict):
                        break

                    if not task_result.get("successful", True):
                        if task_result.get("text_too_large", False):
                            return task_result

                        # failure_reason is currently provided by /video/retrieve_url only.
                        if endpoint == "/video/retrieve_url":
                            failure_reason = task_result.get("failure_reason")
                            if isinstance(failure_reason, str) and failure_reason.strip():
                                raise RuntimeError(failure_reason)

                        result_message = task_result.get("result")
                        if isinstance(result_message, str) and result_message.strip():
                            raise RuntimeError(result_message)

                        raise RuntimeError("Unknown GraphAI task error")

                    return task_result

                if task_status == "FAILURE":
                    break

                raise ValueError(f"Unexpected task status: {task_status}")

            if attempt < max_tries:
                delay = self._backoff_seconds(attempt)
                self._log(
                    f"Async task failed (attempt {attempt}/{max_tries}), retrying in {delay}s..."
                )
                sleep(delay)

        return None
