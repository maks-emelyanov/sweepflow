"""Alpaca's paper-only Trading API; credentials never leave its fixed origin.

Order endpoints follow https://docs.alpaca.markets/us/reference/getallorders-1.
Read-only requests retry transient failures with a short, bounded backoff.
Mutations are sent once: callers must reconcile an ambiguous submission using
its client order ID before attempting another order.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
TERMINAL_ORDER_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "replaced"})
_KEY_NAMES = ("ALPACA_API_KEY_ID", "ALPACA_API_KEY", "APCA_API_KEY_ID")
_SECRET_NAMES = (
    "ALPACA_API_SECRET_KEY",
    "ALPACA_API_SECRET",
    "ALPACA_SECRET_KEY",
    "APCA_API_SECRET_KEY",
)
_URL_NAMES = ("ALPACA_BASE_URL", "APCA_API_BASE_URL", "ALPACA_PAPER_BASE_URL")
_ENV_NAMES = frozenset((*_KEY_NAMES, *_SECRET_NAMES, *_URL_NAMES))
_MAX_RESPONSE_BYTES = 20 * 1024 * 1024
_MAX_RETRY_DELAY = 2.0
_SAFE_METHODS = frozenset({"GET", "POST", "DELETE"})
_SAFE_REASONS = frozenset(
    {
        "api_error",
        "http_error",
        "rate_limited",
        "server_error",
        "authentication_error",
        "transport_error",
        "timeout",
        "invalid_json",
        "response_size_limit",
        "invalid_object",
        "invalid_list",
        "retry_deferred",
    }
)


def _safe_endpoint(path: str) -> str:
    """Allow only static endpoint names and templates, never IDs or query values."""
    if path in {
        "/v2/account",
        "/v2/clock",
        "/v2/positions",
        "/v2/orders",
        "/v2/orders:by_client_order_id",
    }:
        return path
    if path.startswith("/v2/orders/"):
        return "/v2/orders/{order_id}"
    if path.startswith("/v2/assets/"):
        return "/v2/assets/{symbol}"
    return "other"


class AlpacaAPIError(RuntimeError):
    """Sanitized API failure; ``status=None`` includes ambiguous transport errors."""

    def __init__(
        self,
        message: str,
        status: int | None = None,
        *,
        method: str | None = None,
        endpoint: str | None = None,
        reason: str = "api_error",
        attempts: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.method = method if method in _SAFE_METHODS else None
        self.endpoint = _safe_endpoint(endpoint) if isinstance(endpoint, str) else None
        self.reason = reason if reason in _SAFE_REASONS else "api_error"
        self.attempts = attempts if type(attempts) is int and 0 <= attempts <= 5 else None
        self.retry_after_seconds = (
            retry_after_seconds
            if type(retry_after_seconds) in (int, float)
            and math.isfinite(retry_after_seconds)
            and retry_after_seconds >= 0
            else None
        )

    def safe_fields(self) -> dict[str, Any]:
        """Structured diagnostics that omit upstream text, credentials and IDs."""
        fields: dict[str, Any] = {
            "http_status": (
                self.status if type(self.status) is int and 100 <= self.status < 600 else None
            ),
            "reason": self.reason,
        }
        for name in ("method", "endpoint", "attempts", "retry_after_seconds"):
            value = getattr(self, name)
            if value is not None:
                fields[name] = value
        return fields


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib otherwise copies API key headers into the redirected request.
        return None


def _read_dotenv(path: str | Path) -> dict[str, str]:
    """Read only Alpaca settings, without evaluation or global environment changes."""
    try:
        contents = Path(path).read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return {}
    values: dict[str, str] = {}
    for number, line in enumerate(contents.splitlines(), 1):
        line = line.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, raw = line.partition("=")
        name = name.strip()
        if not separator or name not in _ENV_NAMES:
            continue
        raw = raw.strip()
        try:
            if raw.startswith(("'", '"')):
                parts = shlex.split(raw, comments=True, posix=True)
                if len(parts) != 1:
                    raise ValueError
                value = parts[0]
            else:
                value = re.split(r"\s+#", raw, maxsplit=1)[0].strip()
        except ValueError:
            raise ValueError(f"Invalid Alpaca setting in .env at line {number}") from None
        values[name] = value
    return values


def _setting(names: tuple[str, ...], file_values: Mapping[str, str]) -> str | None:
    # Environment aliases also outrank canonical names from the .env file.
    for source in (os.environ, file_values):
        for name in names:
            if name in source:
                return source[name]
    return None


def _component(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or value in (".", ".."):
        raise ValueError("A nonempty Alpaca resource identifier is required")
    return quote(value, safe="")


def _http_reason(status: int) -> str:
    if status == 429:
        return "rate_limited"
    if 500 <= status < 600:
        return "server_error"
    if status in (401, 403):
        return "authentication_error"
    return "http_error"


def _retry_after_seconds(headers: Any) -> float | None:
    """Read a server delay without exposing the header in diagnostics.

    Retry-After permits seconds or an HTTP date:
    https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after.
    """
    raw = headers.get("Retry-After") if headers is not None else None
    if not isinstance(raw, str) or not raw.strip() or len(raw) > 128:
        return None
    raw = raw.strip()
    try:
        if re.fullmatch(r"[0-9]+", raw):
            requested = float(raw)
        else:
            date = parsedate_to_datetime(raw)
            if date.tzinfo is None:
                return None
            requested = max(0.0, date.timestamp() - time.time())
    except ValueError, TypeError, OverflowError, OSError:
        return None
    return requested


class AlpacaPaperClient:
    """Synchronous Trading API client restricted to Alpaca's HTTPS paper origin."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = ALPACA_PAPER_BASE_URL,
        timeout: float = 30.0,
        opener: Any = None,
        max_read_attempts: int = 3,
    ) -> None:
        if not isinstance(base_url, str) or base_url.rstrip("/") != ALPACA_PAPER_BASE_URL:
            raise ValueError("Only https://paper-api.alpaca.markets is allowed for paper trading")
        for credential in (api_key, api_secret):
            if (
                not isinstance(credential, str)
                or not credential.strip()
                or any(ord(character) < 33 or ord(character) > 126 for character in credential)
            ):
                raise ValueError("Alpaca API key and secret must be nonempty printable credentials")
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Alpaca request timeout must be finite and positive")
        if type(max_read_attempts) is not int or not 1 <= max_read_attempts <= 5:
            raise ValueError("Alpaca read attempts must be an integer from 1 to 5")
        self.timeout = timeout
        self.max_read_attempts = max_read_attempts
        self._read_not_before = 0.0
        self._read_retry_status: int | None = None
        self._api_key = api_key
        self._api_secret = api_secret
        self._opener = opener if opener is not None else build_opener(_NoRedirect())

    @property
    def base_url(self) -> str:
        return ALPACA_PAPER_BASE_URL

    @classmethod
    def from_env(
        cls, *, env_file: str | Path = Path(".env"), timeout: float = 30.0
    ) -> AlpacaPaperClient:
        values = _read_dotenv(env_file)
        key = _setting(_KEY_NAMES, values)
        secret = _setting(_SECRET_NAMES, values)
        if not key or not secret:
            raise ValueError("Missing Alpaca API key or secret in the environment or .env")
        return cls(
            key,
            secret,
            base_url=_setting(_URL_NAMES, values) or ALPACA_PAPER_BASE_URL,
            timeout=timeout,
        )

    def __enter__(self) -> AlpacaPaperClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._opener.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        remaining = self._read_not_before - time.monotonic()
        if method == "GET" and remaining > 0:
            raise AlpacaAPIError(
                "Alpaca paper read deferred until the server's retry delay expires",
                self._read_retry_status,
                method=method,
                endpoint=path,
                reason="retry_deferred",
                attempts=0,
                retry_after_seconds=remaining,
            )
        url = self.base_url + path
        if params:
            url += "?" + urlencode(params)
        data = None if payload is None else json.dumps(payload, allow_nan=False).encode("utf-8")
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "APCA-API-KEY-ID": self._api_key,
                "APCA-API-SECRET-KEY": self._api_secret,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "sweepflow/0.1 AlpacaPaperClient",
            },
        )
        max_attempts = self.max_read_attempts if method == "GET" else 1
        read_deadline = time.monotonic() + self.timeout
        last_error: AlpacaAPIError | None = None
        for attempt in range(1, max_attempts + 1):
            headers = None
            status = None
            request_timeout = self.timeout
            if attempt > 1:
                request_timeout = min(self.timeout, read_deadline - time.monotonic())
                if request_timeout <= 0:
                    assert last_error is not None
                    raise last_error from None
            try:
                with self._opener.open(request, timeout=request_timeout) as response:
                    status = response.status
                    if 200 <= status < 300:
                        if status == 204:
                            return None
                        body = response.read(_MAX_RESPONSE_BYTES + 1)
                        if method == "GET":
                            self._read_not_before = 0.0
                        break
                    headers = getattr(response, "headers", None)
            except HTTPError as exc:
                status = exc.code
                headers = exc.headers
                exc.close()
            except (URLError, OSError, HTTPException) as exc:
                # Neither exception strings nor upstream bodies are safe to log:
                # servers/proxies can echo request credentials into either one.
                cause = exc.reason if isinstance(exc, URLError) else exc
                status = None
                error = AlpacaAPIError(
                    "Alpaca paper request failed during transport; "
                    "reconcile before retrying an order",
                    method=method,
                    endpoint=path,
                    reason="timeout" if isinstance(cause, TimeoutError) else "transport_error",
                    attempts=attempt,
                )
            if status is not None:
                try:
                    phrase = HTTPStatus(status).phrase
                except ValueError:
                    phrase = "Request failed"
                error = AlpacaAPIError(
                    f"Alpaca paper request failed (HTTP {status}: {phrase})",
                    status,
                    method=method,
                    endpoint=path,
                    reason=_http_reason(status),
                    attempts=attempt,
                )
            transient = status is None or status == 429 or 500 <= status < 600
            requested = _retry_after_seconds(headers) if transient else None
            if method == "GET" and requested is not None and requested > 0:
                self._read_not_before = time.monotonic() + requested
                self._read_retry_status = status
                error.retry_after_seconds = requested
            delay = min(0.5 * 2 ** (attempt - 1), _MAX_RETRY_DELAY)
            if requested is not None:
                delay = max(delay, requested)
            if (
                not transient
                or attempt >= max_attempts
                or delay > _MAX_RETRY_DELAY
                or time.monotonic() + delay >= read_deadline
            ):
                delay = None
            if delay is None:
                raise error from None
            last_error = error
            time.sleep(delay)
        if len(body) > _MAX_RESPONSE_BYTES:
            raise AlpacaAPIError(
                "Alpaca paper response exceeded the size limit",
                status,
                method=method,
                endpoint=path,
                reason="response_size_limit",
                attempts=attempt,
            )
        try:
            return json.loads(body)
        except ValueError, UnicodeError:
            raise AlpacaAPIError(
                "Alpaca paper returned an invalid JSON response",
                status,
                method=method,
                endpoint=path,
                reason="invalid_json",
                attempts=attempt,
            ) from None

    @staticmethod
    def _object(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise AlpacaAPIError(
                "Alpaca paper returned an invalid object response", reason="invalid_object"
            )
        return value

    @staticmethod
    def _records(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise AlpacaAPIError(
                "Alpaca paper returned an invalid list response", reason="invalid_list"
            )
        return value

    def get_account(self) -> dict[str, Any]:
        return self._object(self._request("GET", "/v2/account"))

    def get_clock(self) -> dict[str, Any]:
        return self._object(self._request("GET", "/v2/clock"))

    def get_positions(self) -> list[dict[str, Any]]:
        return self._records(self._request("GET", "/v2/positions"))

    def get_open_orders(self, *, page_size: int = 500) -> list[dict[str, Any]]:
        """Read every open order with exit legs, rejecting stalled pagination.

        Alpaca's exclusive order-ID cursor avoids losing orders that share the
        same submission timestamp at a page boundary.
        """
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or not 1 <= page_size <= 500
        ):
            raise ValueError("Alpaca order page size must be an integer from 1 to 500")
        params: dict[str, Any] = {
            "status": "open",
            "nested": "true",
            "direction": "desc",
            "limit": page_size,
        }
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        for _ in range(10000):
            page = self._records(self._request("GET", "/v2/orders", params=params))
            if len(page) > page_size:
                raise AlpacaAPIError("Alpaca order page exceeded the requested limit")
            for row in page:
                order_id = row.get("id")
                if not isinstance(order_id, str) or not order_id or order_id in seen:
                    raise AlpacaAPIError("Alpaca order pagination returned missing or repeated IDs")
                seen.add(order_id)
                records.append(row)
            if len(page) < page_size:
                return records
            params["before_order_id"] = page[-1]["id"]
        raise AlpacaAPIError("Alpaca order pagination exceeded its safety limit")

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._object(
            self._request("GET", f"/v2/orders/{_component(order_id)}", params={"nested": "true"})
        )

    def get_order_by_client_id(self, client_order_id: str) -> dict[str, Any] | None:
        _component(client_order_id)
        try:
            order = self._object(
                self._request(
                    "GET",
                    "/v2/orders:by_client_order_id",
                    params={"client_order_id": client_order_id},
                )
            )
        except AlpacaAPIError as exc:
            if exc.status == 404:
                return None
            raise
        order_id = order.get("id")
        if not isinstance(order_id, str) or not order_id:
            raise AlpacaAPIError("Alpaca client-order lookup returned no order ID")
        # The client-ID endpoint does not document nested=true. Fetch by broker ID
        # so a recovered bracket always includes its stop and target children.
        return self.get_order(order_id)

    def get_asset(self, symbol: str) -> dict[str, Any]:
        return self._object(self._request("GET", f"/v2/assets/{_component(symbol)}"))

    def submit_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise TypeError("Alpaca order payload must be a dictionary")
        return self._object(self._request("POST", "/v2/orders", payload=payload))

    def cancel_order(self, order_id: str) -> None:
        self._request("DELETE", f"/v2/orders/{_component(order_id)}")
