from collections.abc import Iterator

import pytest
import requests
import responses

from alphadata.core import http
from alphadata.core.errors import HttpRequestError, HttpStatusError
from alphadata.core.settings import Settings

URL = "https://api.example.com/series"


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[float]]:
    recorded: list[float] = []
    monkeypatch.setattr(http, "_sleep", recorded.append)
    http._buckets.clear()
    yield recorded
    http._buckets.clear()


@pytest.fixture
def session() -> http.TimeoutSession:
    return http.make_session(Settings(user_agent="alphadata-test (ops@example.com)"))


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("https://x/a?api_key=abc123&file_type=json", "https://x/a?api_key=***&file_type=json"),
        ("https://x/a?UserID=ABCD-1234", "https://x/a?UserID=***"),
        ("https://x/a?q=1&registrationkey=zz9", "https://x/a?q=1&registrationkey=***"),
        ("https://x/a?API_KEY=abc", "https://x/a?API_KEY=***"),
        ("https://x/a?symbol=BTCUSDT", "https://x/a?symbol=BTCUSDT"),
    ],
)
def test_mask_secrets(text: str, expected: str) -> None:
    assert http.mask_secrets(text) == expected


@responses.activate
def test_retry_429_then_200(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, status=429)
    responses.get(URL, body=b"ok")
    result = http.get_bytes(session, URL)
    assert result.status_code == 200
    assert result.content == b"ok"
    assert sleeps == [2.0]
    assert len(responses.calls) == 2


@responses.activate
def test_retry_after_header_is_respected(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, status=503, headers={"Retry-After": "7"})
    responses.get(URL, body=b"ok")
    http.get_bytes(session, URL)
    assert sleeps == [7.0]


@responses.activate
def test_gives_up_after_max_retries(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, status=503)
    with pytest.raises(HttpStatusError) as exc_info:
        http.get_bytes(session, URL)
    assert exc_info.value.status_code == 503
    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert len(responses.calls) == http.MAX_RETRIES + 1


@responses.activate
def test_404_returned_without_retry(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, status=404)
    result = http.get_bytes(session, URL)
    assert result.status_code == 404
    assert len(responses.calls) == 1
    assert sleeps == []


@responses.activate
def test_other_4xx_raises_without_retry(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, status=403)
    with pytest.raises(HttpStatusError):
        http.get_bytes(session, URL)
    assert len(responses.calls) == 1


@responses.activate
def test_key_masked_in_result_and_error(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, body=b"{}")
    result = http.get_bytes(session, URL, params={"api_key": "SECRET123", "series_id": "CPI"})
    assert "SECRET123" not in result.url
    assert "api_key=***" in result.url

    responses.get(URL, status=401)
    with pytest.raises(HttpStatusError) as exc_info:
        http.get_bytes(session, URL, params={"api_key": "SECRET123"})
    assert "SECRET123" not in str(exc_info.value)


@responses.activate
def test_network_error_retried_then_masked(
    sleeps: list[float], session: http.TimeoutSession
) -> None:
    responses.get(URL, body=requests.ConnectionError("Max retries with url: /s?api_key=SECRET123"))
    with pytest.raises(HttpRequestError) as exc_info:
        http.get_bytes(session, URL)
    assert "SECRET123" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert len(responses.calls) == http.MAX_RETRIES + 1


@responses.activate
def test_user_agent_header(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, body=b"ok")
    http.get_bytes(session, URL)
    assert responses.calls[0].request.headers["User-Agent"] == "alphadata-test (ops@example.com)"


def test_token_bucket_spaces_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(http, "_clock", lambda: now[0])
    monkeypatch.setattr(http, "_sleep", fake_sleep)
    bucket = http._TokenBucket(rate=2.0)
    for _ in range(4):
        bucket.acquire()
    # Hai request đầu dùng sức chứa ban đầu, hai request sau phải chờ 0.5 giây mỗi cái.
    assert sleeps == [0.5, 0.5]

    now[0] += 10
    bucket.acquire()
    assert sleeps == [0.5, 0.5]


def test_token_bucket_rejects_non_positive_rate() -> None:
    with pytest.raises(ValueError):
        http._TokenBucket(rate=0)


@responses.activate
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),  # ngày đã qua: không chờ
        ("not-a-date", 2.0),  # không đọc được: dùng backoff
        ("100000", http.MAX_RETRY_AFTER_SECONDS),  # quá dài: bị chặn trần
    ],
)
def test_retry_after_variants(
    sleeps: list[float], session: http.TimeoutSession, header: str, expected: float
) -> None:
    responses.get(URL, status=429, headers={"Retry-After": header})
    responses.get(URL, body=b"ok")
    http.get_bytes(session, URL)
    assert sleeps == [expected]


@responses.activate
def test_host_rate_applies_per_host(sleeps: list[float], session: http.TimeoutSession) -> None:
    responses.get(URL, body=b"ok")
    for _ in range(3):
        http.get_bytes(session, URL, host_rate=1.0)
    assert list(http._buckets) == ["api.example.com"]
    assert len(sleeps) == 2
