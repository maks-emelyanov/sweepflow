from __future__ import annotations

import io
import json
from email.utils import formatdate
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest

from sweepflow.alpaca import (
    _ENV_NAMES,
    ALPACA_PAPER_BASE_URL,
    AlpacaAPIError,
    AlpacaPaperClient,
    _NoRedirect,
)


class Response:
    def __init__(self, value=None, *, status=200, body=None, headers=None):
        self.status = status
        self.body = json.dumps(value).encode() if body is None else body
        self.headers = headers or {}
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def read(self, limit):
        return self.body[:limit]


class Opener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    def close(self):
        self.closed = True


def client(*responses):
    opener = Opener(*responses)
    return AlpacaPaperClient("test-key", "test-secret", opener=opener), opener


def http_error(status, *, headers=None):
    return HTTPError(
        ALPACA_PAPER_BASE_URL + "/v2/orders",
        status,
        "private: test-secret",
        {"Location": "https://example.test", **(headers or {})},
        io.BytesIO(b'{"message":"test-key test-secret"}'),
    )


@pytest.fixture
def retry_waits(monkeypatch):
    waits = []
    monkeypatch.setattr("sweepflow.alpaca.time.sleep", waits.append)
    return waits


@pytest.fixture
def empty_env(monkeypatch):
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://paper-api.alpaca.markets",
        "https://api.alpaca.markets",
        "https://paper-api.alpaca.markets.attacker.example",
        "https://paper-api.alpaca.markets@attacker.example",
        "https://paper-api.alpaca.markets:443",
        "https://paper-api.alpaca.markets/v2",
        "https://paper-api.alpaca.markets?redirect=evil",
        "https://paper-api.alpaca.markets#ignored",
        "https://paper-api.alpaca.markets\n",
    ],
)
def test_rejects_every_origin_except_paper_before_transport(base_url):
    with pytest.raises(ValueError, match="Only https://paper-api"):
        AlpacaPaperClient("test-key", "test-secret", base_url=base_url)


def test_default_transport_forbids_redirects_and_client_context_closes():
    with AlpacaPaperClient("test-key", "test-secret") as api:
        guards = [handler for handler in api._opener.handlers if isinstance(handler, _NoRedirect)]
        assert len(guards) == 1
        assert guards[0].redirect_request(None, None, 302, "", {}, "https://evil.test") is None
    api, opener = client(Response({"id": "account"}))
    with api:
        assert api.get_account() == {"id": "account"}
    assert opener.closed


@pytest.mark.parametrize(
    ("key", "secret"),
    [("", "secret"), ("key", ""), ("key\n", "secret"), ("key", "secret\rleak"), (" ", "s")],
)
def test_invalid_credentials_rejected_without_echoing_values(key, secret):
    with pytest.raises(ValueError, match="nonempty printable credentials"):
        AlpacaPaperClient(key, secret)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_invalid_timeout_rejected(timeout):
    with pytest.raises(ValueError, match="timeout"):
        AlpacaPaperClient("key", "secret", timeout=timeout)


@pytest.mark.parametrize("attempts", [0, -1, 6, 1.5, True, "3"])
def test_invalid_read_attempt_limit_rejected(attempts):
    with pytest.raises(ValueError, match="read attempts"):
        AlpacaPaperClient("key", "secret", max_read_attempts=attempts)


@pytest.mark.usefixtures("empty_env")
@pytest.mark.parametrize(
    ("key_name", "secret_name"),
    [
        ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"),
        ("ALPACA_API_KEY", "ALPACA_API_SECRET"),
        ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
        ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"),
    ],
)
def test_dotenv_supports_aliases_quotes_comments_and_export(tmp_path, key_name, secret_name):
    env = tmp_path / ".env"
    env.write_text(
        f'export {key_name}="file-key" # comment\n{secret_name}=file-secret#suffix\n'
        "UNRELATED='unclosed\n",
        encoding="utf-8",
    )
    with AlpacaPaperClient.from_env(env_file=env) as api:
        assert api._api_key == "file-key"
        assert api._api_secret == "file-secret#suffix"
        assert api.base_url == ALPACA_PAPER_BASE_URL


@pytest.mark.usefixtures("empty_env")
def test_environment_aliases_override_file_canonical_names(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("ALPACA_API_KEY_ID=file-key\nALPACA_API_SECRET_KEY=file-secret\n")
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "env-secret")
    with AlpacaPaperClient.from_env(env_file=env) as api:
        assert api._api_key == "env-key"
        assert api._api_secret == "env-secret"
    import os

    assert "ALPACA_API_KEY_ID" not in os.environ


@pytest.mark.usefixtures("empty_env")
def test_environment_can_work_without_dotenv_file(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "env-secret")
    with AlpacaPaperClient.from_env(env_file=tmp_path / "missing.env") as api:
        assert api._api_key == "env-key"


@pytest.mark.usefixtures("empty_env")
def test_missing_credentials_fail_without_request(tmp_path):
    with pytest.raises(ValueError, match="Missing Alpaca"):
        AlpacaPaperClient.from_env(env_file=tmp_path / "missing.env")


@pytest.mark.usefixtures("empty_env")
def test_bad_quoted_secret_error_omits_secret(tmp_path):
    env = tmp_path / ".env"
    env.write_text("ALPACA_API_SECRET='do-not-show-this\n")
    with pytest.raises(ValueError, match="line 1") as caught:
        AlpacaPaperClient.from_env(env_file=env)
    assert "do-not-show" not in str(caught.value)


@pytest.mark.usefixtures("empty_env")
def test_live_base_url_from_env_is_rejected(tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        "ALPACA_API_KEY=key\nALPACA_API_SECRET=secret\n"
        "APCA_API_BASE_URL=https://api.alpaca.markets\n"
    )
    with pytest.raises(ValueError, match="Only https://paper-api"):
        AlpacaPaperClient.from_env(env_file=env)


def test_read_methods_authentication_and_nested_orders():
    api, opener = client(
        Response({"id": "account"}),
        Response({"is_open": True}),
        Response([{"symbol": "SPY"}]),
        Response({"id": "order", "legs": [{"id": "stop"}]}),
        Response({"symbol": "BRK.B", "tradable": True}),
    )
    assert api.get_account()["id"] == "account"
    assert api.get_clock()["is_open"]
    assert api.get_positions()[0]["symbol"] == "SPY"
    assert api.get_order("order")["legs"] == [{"id": "stop"}]
    assert api.get_asset("BRK.B")["tradable"]
    assert [urlsplit(request.full_url).path for request, _ in opener.calls] == [
        "/v2/account",
        "/v2/clock",
        "/v2/positions",
        "/v2/orders/order",
        "/v2/assets/BRK.B",
    ]
    for request, timeout in opener.calls:
        assert request.get_method() == "GET"
        assert request.get_header("Apca-api-key-id") == "test-key"
        assert request.get_header("Apca-api-secret-key") == "test-secret"
        assert "test-secret" not in request.full_url
        assert timeout == 30
    assert parse_qs(urlsplit(opener.calls[3][0].full_url).query) == {"nested": ["true"]}


def test_order_ids_cannot_modify_request_path_or_query():
    api, opener = client(Response({"id": "order"}))
    api.get_order("order/with?special#chars")
    assert "/v2/orders/order%2Fwith%3Fspecial%23chars?nested=true" in opener.calls[0][0].full_url


def test_open_orders_use_id_cursor_not_timestamps_and_retain_bracket_legs():
    orders = [
        {"id": "third", "submitted_at": "2026-09-28T14:00:00Z", "legs": [{"id": "stop"}]},
        {"id": "second", "submitted_at": "2026-09-28T14:00:00Z"},
        {"id": "first", "submitted_at": "2026-09-28T14:00:00Z"},
    ]
    api, opener = client(Response(orders[:2]), Response(orders[2:]))
    assert api.get_open_orders(page_size=2) == orders
    first = parse_qs(urlsplit(opener.calls[0][0].full_url).query)
    assert first == {"status": ["open"], "nested": ["true"], "direction": ["desc"], "limit": ["2"]}
    second = parse_qs(urlsplit(opener.calls[1][0].full_url).query)
    assert second == {**first, "before_order_id": ["second"]}


def test_full_final_page_requests_next_empty_page():
    api, opener = client(Response([{"id": "one"}]), Response([]))
    assert api.get_open_orders(page_size=1) == [{"id": "one"}]
    assert len(opener.calls) == 2


@pytest.mark.parametrize("rows", [[{"id": "one"}, {"id": "one"}], [{"symbol": "SPY"}]])
def test_open_orders_reject_duplicate_or_missing_ids(rows):
    api, _ = client(Response(rows))
    with pytest.raises(AlpacaAPIError, match="missing or repeated"):
        api.get_open_orders()


def test_open_orders_reject_ignored_cursor_instead_of_truncating():
    api, opener = client(Response([{"id": "one"}]), Response([{"id": "one"}]))
    with pytest.raises(AlpacaAPIError, match="repeated"):
        api.get_open_orders(page_size=1)
    assert len(opener.calls) == 2


def test_client_id_lookup_hydrates_nested_legs():
    api, opener = client(Response({"id": "parent"}), Response({"id": "parent", "legs": []}))
    assert api.get_order_by_client_id("strategy-id") == {"id": "parent", "legs": []}
    assert opener.calls[0][0].full_url.endswith(
        "/v2/orders:by_client_order_id?client_order_id=strategy-id"
    )
    assert opener.calls[1][0].full_url.endswith("/v2/orders/parent?nested=true")


def test_only_client_lookup_404_means_order_absent():
    api, _ = client(http_error(404))
    assert api.get_order_by_client_id("strategy-id") is None
    api, _ = client(Response({"id": "parent"}), http_error(404))
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_order_by_client_id("strategy-id")
    assert caught.value.status == 404
    api, _ = client(http_error(401))
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_order_by_client_id("strategy-id")
    assert caught.value.status == 401


def test_submit_and_cancel_preserve_payload_and_make_one_request_each():
    api, opener = client(Response({"id": "new"}), Response(status=204))
    payload = {
        "symbol": "SPY",
        "qty": "2",
        "side": "buy",
        "type": "limit",
        "limit_price": "100.01",
        "time_in_force": "day",
        "order_class": "bracket",
        "client_order_id": "strategy-id",
        "take_profit": {"limit_price": "105.00"},
        "stop_loss": {"stop_price": "99.00"},
    }
    assert api.submit_order(payload) == {"id": "new"}
    assert api.cancel_order("new") is None
    post, delete = [request for request, _ in opener.calls]
    assert post.get_method() == "POST"
    assert json.loads(post.data) == payload
    assert delete.get_method() == "DELETE"
    assert delete.data is None


@pytest.mark.parametrize(
    "failure", [http_error(302), http_error(401), http_error(422), http_error(500)]
)
def test_http_errors_are_sanitized_and_submissions_are_never_retried(failure):
    api, opener = client(failure)
    with pytest.raises(AlpacaAPIError) as caught:
        api.submit_order({"client_order_id": "deterministic"})
    assert caught.value.status == failure.code
    assert "test-key" not in str(caught.value)
    assert "test-secret" not in str(caught.value)
    assert len(opener.calls) == 1


@pytest.mark.parametrize("failure", [URLError("test-secret"), TimeoutError("test-key")])
def test_network_failure_is_ambiguous_and_never_retried(failure):
    api, opener = client(failure)
    with pytest.raises(AlpacaAPIError, match="reconcile") as caught:
        api.submit_order({"client_order_id": "deterministic"})
    assert caught.value.status is None
    assert "test-secret" not in str(caught.value)
    assert "test-key" not in str(caught.value)
    assert len(opener.calls) == 1


@pytest.mark.parametrize(
    "failure",
    [
        URLError("test-secret"),
        TimeoutError("test-key"),
        HTTPException("test-secret"),
        http_error(429),
        http_error(500),
        http_error(502),
        http_error(503),
        http_error(504),
    ],
)
def test_transient_reads_retry_and_return_success(failure, retry_waits):
    api, opener = client(failure, Response({"id": "account"}))
    assert api.get_account() == {"id": "account"}
    assert len(opener.calls) == 2
    assert all(request.get_method() == "GET" for request, _ in opener.calls)
    assert retry_waits == [0.5]


def test_reads_stop_after_bounded_attempts_and_report_safe_context(retry_waits):
    failures = [http_error(503) for _ in range(3)]
    api, opener = client(*failures)
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_order("private-order-id/test-secret")
    assert len(opener.calls) == 3
    assert retry_waits == [0.5, 1.0]
    assert all(failure.fp.closed for failure in failures)
    assert caught.value.safe_fields() == {
        "http_status": 503,
        "method": "GET",
        "endpoint": "/v2/orders/{order_id}",
        "reason": "server_error",
        "attempts": 3,
    }
    diagnostic = json.dumps(caught.value.safe_fields()) + str(caught.value)
    for private in ("test-key", "test-secret", "private-order-id", "nested", "https://"):
        assert private not in diagnostic


@pytest.mark.parametrize("status", [302, 400, 401, 403, 404, 408, 422])
def test_nontransient_read_errors_are_not_retried(status, retry_waits):
    api, opener = client(http_error(status))
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert caught.value.status == status
    assert caught.value.safe_fields()["attempts"] == 1
    assert len(opener.calls) == 1
    assert retry_waits == []


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.parametrize("failure", [http_error(429), http_error(503), TimeoutError("test-secret")])
def test_mutations_never_retry_even_transient_failures(method, failure, retry_waits):
    api, opener = client(failure)
    with pytest.raises(AlpacaAPIError) as caught:
        if method == "POST":
            api.submit_order({"client_order_id": "private-client-id"})
        else:
            api.cancel_order("private-broker-id")
    assert len(opener.calls) == 1
    assert retry_waits == []
    assert caught.value.safe_fields()["method"] == method
    assert caught.value.safe_fields()["attempts"] == 1
    assert "private-" not in json.dumps(caught.value.safe_fields())


@pytest.mark.parametrize("retry_after", ["1", "2"])
def test_short_retry_after_is_honored(retry_after, retry_waits):
    api, opener = client(
        http_error(429, headers={"Retry-After": retry_after}), Response({"id": "account"})
    )
    assert api.get_account() == {"id": "account"}
    assert len(opener.calls) == 2
    assert retry_waits == [float(retry_after)]


def test_retry_after_http_date_is_honored(monkeypatch, retry_waits):
    now = 1_800_000_000
    monkeypatch.setattr("sweepflow.alpaca.time.time", lambda: now)
    api, opener = client(
        http_error(503, headers={"Retry-After": formatdate(now + 2, usegmt=True)}),
        Response({"id": "account"}),
    )
    assert api.get_account() == {"id": "account"}
    assert len(opener.calls) == 2
    assert retry_waits == [2.0]


@pytest.mark.parametrize("retry_after", ["3", "9999999999999999999999999"])
def test_long_retry_after_defers_instead_of_retrying_early(retry_after, retry_waits):
    api, opener = client(http_error(429, headers={"Retry-After": retry_after}))
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert len(opener.calls) == 1
    assert retry_waits == []
    assert caught.value.safe_fields()["reason"] == "rate_limited"


@pytest.mark.parametrize("status", [429, 503])
def test_server_retry_delay_remains_enforced_across_runtime_polls(status, monkeypatch, retry_waits):
    monotonic = [1_000.0]
    monkeypatch.setattr("sweepflow.alpaca.time.monotonic", lambda: monotonic[0])
    api, opener = client(http_error(status, headers={"Retry-After": "60"}), Response([]))
    with pytest.raises(AlpacaAPIError) as first:
        api.get_account()
    assert first.value.safe_fields()["retry_after_seconds"] == 60
    monotonic[0] += 5
    with pytest.raises(AlpacaAPIError) as deferred:
        api.get_positions()
    assert deferred.value.safe_fields() == {
        "http_status": status,
        "method": "GET",
        "endpoint": "/v2/positions",
        "reason": "retry_deferred",
        "attempts": 0,
        "retry_after_seconds": 55,
    }
    assert len(opener.calls) == 1
    assert retry_waits == []
    monotonic[0] += 55
    assert api.get_positions() == []
    assert len(opener.calls) == 2


def test_read_cooldown_does_not_suppress_mutation_requests(monkeypatch, retry_waits):
    monkeypatch.setattr("sweepflow.alpaca.time.monotonic", lambda: 1_000)
    api, opener = client(http_error(429, headers={"Retry-After": "60"}), Response(status=204))
    with pytest.raises(AlpacaAPIError):
        api.get_account()
    assert api.cancel_order("private-broker-id") is None
    assert [request.get_method() for request, _ in opener.calls] == ["GET", "DELETE"]
    assert retry_waits == []


@pytest.mark.parametrize("retry_after", ["test-secret", "-1", "nan", "inf", "1.5"])
def test_invalid_retry_after_uses_normal_backoff(retry_after, retry_waits):
    api, opener = client(
        http_error(503, headers={"Retry-After": retry_after}), Response({"id": "account"})
    )
    assert api.get_account() == {"id": "account"}
    assert len(opener.calls) == 2
    assert retry_waits == [0.5]


def test_read_body_transport_failure_retries_and_closes_response(retry_waits):
    class InterruptedResponse(Response):
        def read(self, limit):
            raise TimeoutError("test-secret")

    interrupted = InterruptedResponse({"id": "account"})
    api, opener = client(interrupted, Response({"id": "account"}))
    assert api.get_account() == {"id": "account"}
    assert interrupted.closed
    assert len(opener.calls) == 2
    assert retry_waits == [0.5]


def test_nonstandard_opener_http_status_uses_same_retry_policy(retry_waits):
    failed = Response(status=503, headers={"Retry-After": "2"})
    api, opener = client(failed, Response({"id": "account"}))
    assert api.get_account() == {"id": "account"}
    assert failed.closed
    assert len(opener.calls) == 2
    assert retry_waits == [2.0]


def test_read_attempt_limit_can_disable_retries(retry_waits):
    opener = Opener(TimeoutError("test-secret"))
    api = AlpacaPaperClient("test-key", "test-secret", opener=opener, max_read_attempts=1)
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert len(opener.calls) == 1
    assert retry_waits == []
    assert caught.value.safe_fields() == {
        "http_status": None,
        "method": "GET",
        "endpoint": "/v2/account",
        "reason": "timeout",
        "attempts": 1,
    }


def test_read_retries_share_the_original_request_timeout_budget(monkeypatch):
    monotonic = [1_000.0]
    waits = []
    monkeypatch.setattr("sweepflow.alpaca.time.monotonic", lambda: monotonic[0])

    def sleep(delay):
        waits.append(delay)
        monotonic[0] += delay

    monkeypatch.setattr("sweepflow.alpaca.time.sleep", sleep)

    class SlowOpener(Opener):
        def open(self, request, *, timeout):
            self.calls.append((request, timeout))
            monotonic[0] += min(20, timeout)
            raise TimeoutError("test-secret")

    opener = SlowOpener()
    api = AlpacaPaperClient("test-key", "test-secret", opener=opener)
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert [timeout for _, timeout in opener.calls] == [30, 9.5]
    assert waits == [0.5]
    assert monotonic[0] == 1_030
    assert caught.value.safe_fields()["attempts"] == 2


def test_read_does_not_retry_when_first_attempt_consumes_timeout_budget(monkeypatch, retry_waits):
    monotonic = [1_000.0]
    monkeypatch.setattr("sweepflow.alpaca.time.monotonic", lambda: monotonic[0])

    class SlowOpener(Opener):
        def open(self, request, *, timeout):
            self.calls.append((request, timeout))
            monotonic[0] += timeout
            raise TimeoutError("test-secret")

    opener = SlowOpener()
    api = AlpacaPaperClient("test-key", "test-secret", opener=opener)
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert len(opener.calls) == 1
    assert retry_waits == []
    assert caught.value.safe_fields()["attempts"] == 1


def test_delayed_backoff_does_not_start_retry_after_timeout_budget(monkeypatch):
    monotonic = [1_000.0]
    monkeypatch.setattr("sweepflow.alpaca.time.monotonic", lambda: monotonic[0])

    def delayed_sleep(delay):
        monotonic[0] += 31

    monkeypatch.setattr("sweepflow.alpaca.time.sleep", delayed_sleep)
    api, opener = client(TimeoutError("test-secret"), Response({"id": "account"}))
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert len(opener.calls) == 1
    assert caught.value.safe_fields()["attempts"] == 1


def test_safe_fields_omit_untrusted_message_and_unknown_metadata():
    error = AlpacaAPIError(
        "test-key test-secret",
        "test-secret",
        method="GET test-secret",
        endpoint="/unknown/test-secret?private=test-key",
        reason="test-secret",
        attempts=True,
        retry_after_seconds="test-secret",
    )
    assert error.safe_fields() == {
        "http_status": None,
        "endpoint": "other",
        "reason": "api_error",
    }


def test_legacy_error_constructor_keeps_status_and_safe_fields_compatible():
    error = AlpacaAPIError("test-secret", 404)
    assert error.status == 404
    assert error.safe_fields() == {"http_status": 404, "reason": "api_error"}


def test_success_response_size_and_json_errors_are_not_retried(monkeypatch, retry_waits):
    monkeypatch.setattr("sweepflow.alpaca._MAX_RESPONSE_BYTES", 8)
    for response, reason in (
        (Response(body=b"123456789"), "response_size_limit"),
        (Response(body=b"invalid"), "invalid_json"),
    ):
        api, opener = client(response)
        with pytest.raises(AlpacaAPIError) as caught:
            api.get_account()
        assert len(opener.calls) == 1
        assert caught.value.safe_fields() == {
            "http_status": 200,
            "method": "GET",
            "endpoint": "/v2/account",
            "reason": reason,
            "attempts": 1,
        }
    assert retry_waits == []


@pytest.mark.parametrize(
    "response", [Response(body=b"not json test-secret"), Response([]), Response(None)]
)
def test_invalid_success_object_is_rejected_without_response_content(response):
    api, _ = client(response)
    with pytest.raises(AlpacaAPIError) as caught:
        api.get_account()
    assert "test-secret" not in str(caught.value)


def test_invalid_positions_array_rejected():
    api, _ = client(Response([None]))
    with pytest.raises(AlpacaAPIError, match="invalid list"):
        api.get_positions()


def test_paper_origin_cannot_be_changed_after_construction():
    api, _ = client()
    with pytest.raises(AttributeError):
        api.base_url = "https://api.alpaca.markets"
