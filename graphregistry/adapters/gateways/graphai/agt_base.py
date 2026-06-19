# graphregistry/adapters/gateways/graphai/agt_base.py
from __future__ import annotations

from datetime import datetime, timedelta
from json import dump as dump_json, load as load_json
from os import chmod
from pathlib import Path
from random import uniform
from time import sleep
from typing import Any, Callable, ClassVar, Literal, cast, overload

from requests import Response, get, post

from graphregistry.common.config import GlobalConfig, REPO_ROOT


class GraphAIBaseGateway:
    # Module-level cache of login_info keyed by resolved config path. This avoids
    # re-authenticating against GraphAI for every new gateway instance; the
    # /token endpoint can be very slow (10-20s). Token refresh on 401 is still
    # handled transparently by _request().
    _login_cache: ClassVar[dict[str, dict[str, Any]]] = {}
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

        Reuses a module-level cache and an on-disk token cache keyed by config
        path so multiple gateway instances and process runs sharing the same
        GraphAI credentials don't each pay the (often slow) /token cost.
        """
        cache_key = str(self.graph_api_json)
        if self._login_info is None or "token" not in self._login_info:
            self._login_info = self._login_cache.get(cache_key)
        if self._login_info is None or "token" not in self._login_info:
            self._login_info = self._load_cached_login_info(cache_key)
        if self._login_info is None or "token" not in self._login_info:
            self._login_info = self._login(cache_key)
            self._login_cache[cache_key] = self._login_info
            self._save_cached_login_info(self._login_info)
        return self._login_info

    @staticmethod
    def _get_token_cache_path() -> Path:
        """Return the path to the persisted token cache file."""
        cache_dir = Path.home() / ".cache" / "graphregistry"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "graphai_tokens.json"

    @classmethod
    def _load_cached_login_info(cls, graph_api_json: str) -> dict[str, Any] | None:
        """Load a previously cached login_info from disk, if present and valid."""
        cache_path = cls._get_token_cache_path()
        if not cache_path.exists():
            return None
        try:
            with open(cache_path) as fp:
                cache = load_json(fp)
        except Exception:
            return None

        if not isinstance(cache, dict):
            return None
        login_info = cache.get(graph_api_json)
        if not isinstance(login_info, dict) or "token" not in login_info:
            return None
        # Validate that the cached entry matches the expected shape.
        if "host" not in login_info or "user" not in login_info:
            return None
        return login_info

    @classmethod
    def _save_cached_login_info(cls, login_info: dict[str, Any]) -> None:
        """Persist login_info to disk, keyed by config path."""
        cache_path = cls._get_token_cache_path()
        cache: dict[str, Any] = {}
        if cache_path.exists():
            try:
                with open(cache_path) as fp:
                    cache = load_json(fp)
            except Exception:
                cache = {}
        if not isinstance(cache, dict):
            cache = {}

        graph_api_json = login_info.get("graph_api_json")
        if not isinstance(graph_api_json, str):
            return

        cache[graph_api_json] = login_info
        tmp_path = cache_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w") as fp:
                dump_json(cache, fp, indent=2)
            tmp_path.replace(cache_path)
            chmod(cache_path, 0o600)
        except Exception:
            # Best-effort cache persistence; authentication still works.
            pass

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
            timeout=30,
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
    def _jittered_delay(delay_seconds: float) -> float:
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
                new_login_info = self._login(login_info["graph_api_json"])
                new_token = new_login_info["token"]
                login_info["token"] = new_token
                request_headers["Authorization"] = f"Bearer {new_token}"
                # Keep both in-memory and on-disk caches up to date.
                cache_key = login_info["graph_api_json"]
                self._login_cache[cache_key] = new_login_info
                self._save_cached_login_info(new_login_info)
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

    def get_async_task_result(
        self,
        endpoint: str,
        task_id: str,
        login_info: dict[str, Any] | None = None,
        *,
        wait_for_result: bool = True,
        return_status_payload: bool = False,
        max_processing_time_s: int = 6000,
        max_tries: int = 2,
        status_timeout: int = 5,
    ) -> dict[str, Any] | list[Any] | None:
        """
        Get the result of an already-submitted async GraphAI task.

        If wait_for_result is False, this performs one status check and returns:
        - status_payload when return_status_payload is True
        - task_result when task_status is SUCCESS
        - None when task_status is PENDING/STARTED/FAILURE or result is malformed

        status_timeout controls how long a single lightweight /status/{task_id}
        request may take. It defaults to 5s so callers fail fast when GraphAI is
        unreachable; the long wait for actual processing is governed by
        max_processing_time_s.
        """
        resolved_login_info = login_info or self._ensure_login_info()

        # Status checks are lightweight; don't multiply wait times when GraphAI
        # is unreachable.
        status_http_retries = 2

        def _status_check_once() -> tuple[str, dict[str, Any] | list[Any] | None]:
            status_response = self._request(
                url=f"{endpoint}/status/{task_id}",
                login_info=resolved_login_info,
                request_func=get,
                headers={"Content-Type": "application/json"},
                max_tries=status_http_retries,
                timeout=status_timeout,
            )

            status_payload = status_response.json()
            task_status = status_payload["task_status"]
            self._log(f"Task {task_id}: {task_status}")

            if task_status in ("PENDING", "STARTED"):
                return task_status, status_payload if return_status_payload else None

            if task_status == "SUCCESS":
                if return_status_payload:
                    return "SUCCESS", status_payload

                task_result = status_payload.get("task_result")

                # Batch endpoints (e.g., /embedding/embed, /translation/translate)
                # return a list of per-item result dicts. Return the list directly
                # and let the caller validate each item.
                if isinstance(task_result, list):
                    return "SUCCESS", task_result

                if not isinstance(task_result, dict):
                    return "SUCCESS", None

                if not task_result.get("successful", True):
                    if task_result.get("text_too_large", False):
                        return "SUCCESS", task_result

                    # Explicit missing-file signal added by GraphAI status responses.
                    # `None` means unknown/not-applicable and must not be treated as missing.
                    file_found = task_result.get("file_found")
                    if file_found is False:
                        raise RuntimeError(
                            f'GraphAI source file not found for task "{task_id}" at "{endpoint}"'
                        )

                    # failure_reason is currently provided by /video/retrieve_url only.
                    if endpoint == "/video/retrieve_url":
                        failure_reason = task_result.get("failure_reason")
                        if isinstance(failure_reason, str) and failure_reason.strip():
                            raise RuntimeError(failure_reason)

                    result_message = task_result.get("result")
                    if isinstance(result_message, str) and result_message.strip():
                        raise RuntimeError(result_message)

                    raise RuntimeError("Unknown GraphAI task error")

                return "SUCCESS", task_result

            if task_status == "FAILURE":
                return "FAILURE", status_payload if return_status_payload else None

            raise ValueError(f"Unexpected task status: {task_status}")

        if not wait_for_result:
            _status, result = _status_check_once()
            return result

        # Single global deadline. The previous implementation reset the deadline
        # on every outer retry attempt, allowing max_tries * max_processing_time_s
        # total wait time.
        deadline = datetime.now() + timedelta(seconds=max_processing_time_s)

        # Exponential backoff for polling, starting fast so cached tasks return
        # quickly and backing off so long-running tasks don't hammer the server.
        poll_delay = 0.1
        max_poll_delay = 2.0

        status_check_errors = 0
        while datetime.now() < deadline:
            try:
                status, result = _status_check_once()
            except Exception as exc:
                status_check_errors += 1
                self._log(
                    f"Status check failed ({status_check_errors}/{max_tries}): {exc}"
                )
                if status_check_errors >= max_tries:
                    raise RuntimeError(
                        f"Failed to get status for task {task_id} after {max_tries} attempts"
                    ) from exc
                sleep(min(poll_delay, max_poll_delay))
                poll_delay *= 2
                continue

            if status in ("PENDING", "STARTED"):
                jittered_delay = self._jittered_delay(poll_delay)
                remaining = (deadline - datetime.now()).total_seconds()
                sleep(max(0.0, min(jittered_delay, remaining)))
                poll_delay = min(poll_delay * 2, max_poll_delay)
                continue

            if status == "SUCCESS":
                return result

            if status == "FAILURE":
                return result

        return None

    @staticmethod
    def _requires_media_retry(
        task_result: dict[str, Any] | None,
        *,
        media_label: str,
        force: bool = False,
        recalculate_cached: bool = False,
        retry_mode: str = "force",
    ) -> tuple[bool, dict[str, Any]]:
        """
        Inspect a media task result and decide whether the caller must retry.

        Returns a tuple (must_retry, retry_kwargs):
        - must_retry: True if the underlying file/token is missing or inactive.
        - retry_kwargs: payload overrides for the retry call.

        retry_mode controls the retry strategy:
        - "force": retry with force=True (used for video downloads).
        - "recalculate": retry with recalculate_cached=True (used for audio/slides).

        Raises RuntimeError when a fresh or forced result still lacks the file,
        because that indicates a real GraphAI failure rather than a stale cache.
        """
        if not isinstance(task_result, dict):
            return False, {}

        token_status = task_result.get("token_status")
        if not isinstance(token_status, dict):
            # No token_status at all: treat as missing if not fresh/forced.
            if task_result.get("fresh") or force:
                raise RuntimeError(
                    f"Missing downloaded file for {media_label} while fresh or forced"
                )
            retry_kwargs: dict[str, Any] = (
                {"force": True} if retry_mode == "force" else {"recalculate_cached": True}
            )
            return True, retry_kwargs

        active = token_status.get("active")
        fingerprinted = token_status.get("fingerprinted")

        if active or fingerprinted:
            return False, {}

        if task_result.get("fresh") or force:
            raise RuntimeError(
                f"Missing downloaded file for {media_label} while fresh or forced"
            )

        retry_kwargs = (
            {"force": True} if retry_mode == "force" else {"recalculate_cached": True}
        )
        return True, retry_kwargs

    @overload
    def _call_async_endpoint(
        self,
        endpoint: str,
        payload: dict[str, Any],
        login_info: dict[str, Any],
        *,
        max_processing_time_s: int = 6000,
        max_tries: int = 2,
        submit_timeout: int = 10,
        wait_for_result: Literal[True] = True,
    ) -> dict[str, Any] | list[Any] | None: ...

    @overload
    def _call_async_endpoint(
        self,
        endpoint: str,
        payload: dict[str, Any],
        login_info: dict[str, Any],
        *,
        max_processing_time_s: int = 6000,
        max_tries: int = 2,
        submit_timeout: int = 10,
        wait_for_result: Literal[False],
    ) -> str | None: ...

    def _call_async_endpoint(
        self,
        endpoint: str,
        payload: dict[str, Any],
        login_info: dict[str, Any],
        *,
        max_processing_time_s: int = 6000,
        max_tries: int = 2,
        submit_timeout: int = 10,
        wait_for_result: bool = True,
    ) -> dict[str, Any] | list[Any] | str | None:
        """
        Submit an async GraphAI job and optionally poll until completion.

        The submit_timeout controls how long we wait for GraphAI to accept the
        task submission. It should be short (default 10s) so callers fail fast
        when the service is unreachable. The long processing wait happens during
        polling, governed by max_processing_time_s.
        """
        # Use a small, fixed number of HTTP retries for the lightweight submit
        # call. The outer loop handles task-level retries; combining both would
        # multiply wait times unacceptably when GraphAI is unreachable.
        submit_http_retries = 2
        for attempt in range(1, max_tries + 1):
            submit_response = self._request(
                url=endpoint,
                login_info=login_info,
                request_func=post,
                headers={"Content-Type": "application/json"},
                json=payload,
                max_tries=submit_http_retries,
                timeout=submit_timeout,
            )
            self._log(f"Submitted async task to {endpoint}")

            task_id = submit_response.json()["task_id"]
            if not wait_for_result:
                return str(task_id)

            task_result = self.get_async_task_result(
                endpoint=endpoint,
                task_id=str(task_id),
                login_info=login_info,
                max_processing_time_s=max_processing_time_s,
                max_tries=max_tries,
                status_timeout=submit_timeout,
            )
            if task_result is not None:
                return task_result

            if attempt < max_tries:
                delay = self._backoff_seconds(attempt)
                self._log(
                    f"Async task failed (attempt {attempt}/{max_tries}), retrying in {delay}s..."
                )
                sleep(delay)

        return None
