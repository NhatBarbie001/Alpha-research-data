import email.utils
import re
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

import requests
from requests.structures import CaseInsensitiveDict

from alphadata.core.errors import HttpRequestError, HttpStatusError
from alphadata.core.settings import Settings

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES = 5
MAX_RETRY_AFTER_SECONDS = 300.0

_SECRET_PARAM_RE = re.compile(r"(?i)\b(api_key|userid|registrationkey)=([^&\s#'\"]+)")

# Tách ra biến module để test thay được đồng hồ và hàm ngủ.
_clock = time.monotonic
_sleep = time.sleep


def mask_secrets(text: str) -> str:
    return _SECRET_PARAM_RE.sub(r"\1=***", text)


class TimeoutSession(requests.Session):
    def __init__(self, timeout: float) -> None:
        super().__init__()
        self.timeout = timeout


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    content: bytes
    headers: Mapping[str, str]
    url: str  # đã che key, an toàn để ghi manifest và log


def make_session(settings: Settings) -> TimeoutSession:
    session = TimeoutSession(timeout=settings.http_timeout)
    session.headers["User-Agent"] = settings.user_agent
    return session


def get_bytes(
    session: TimeoutSession,
    url: str,
    *,
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    host_rate: float | None = None,
) -> HttpResponse:
    """GET có retry. Trả về khi 2xx/3xx hoặc 404 (connector tự hiểu 404); mã lỗi khác thì raise."""
    host = urlsplit(url).hostname or ""
    for attempt in range(MAX_RETRIES + 1):
        if host_rate is not None:
            _bucket_for(host, host_rate).acquire()
        try:
            resp = session.get(url, params=params, headers=headers, timeout=session.timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == MAX_RETRIES:
                # from None: traceback gốc của requests chứa URL chưa che key.
                raise HttpRequestError(mask_secrets(f"{type(exc).__name__}: {exc}")) from None
            _sleep(_backoff(attempt))
            continue

        if resp.status_code in RETRY_STATUSES and attempt < MAX_RETRIES:
            _sleep(_retry_delay(resp, attempt))
            continue
        masked_url = mask_secrets(resp.url)
        if resp.status_code < 400 or resp.status_code == 404:
            return HttpResponse(
                status_code=resp.status_code,
                content=resp.content,
                headers=CaseInsensitiveDict(resp.headers),
                url=masked_url,
            )
        raise HttpStatusError(resp.status_code, masked_url)
    raise AssertionError("unreachable")


def _backoff(attempt: int) -> float:
    return float(2 ** (attempt + 1))


def _retry_delay(resp: requests.Response, attempt: int) -> float:
    value = resp.headers.get("Retry-After")
    if value is None:
        return _backoff(attempt)
    value = value.strip()
    if value.isdigit():
        delay = float(value)
    else:
        try:
            when = email.utils.parsedate_to_datetime(value)
            delay = (when - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError):
            return _backoff(attempt)
    return min(max(delay, 0.0), MAX_RETRY_AFTER_SECONDS)


class _TokenBucket:
    def __init__(self, rate: float) -> None:
        if rate <= 0:
            raise ValueError(f"host_rate must be > 0, got {rate}")
        self.rate = rate
        self._capacity = max(1.0, rate)
        self._tokens = self._capacity
        self._last = _clock()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        # Ngủ trong lúc giữ lock là cố ý: các luồng cùng host phải xếp hàng.
        with self._lock:
            now = _clock()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self.rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            wait = (1 - self._tokens) / self.rate
            _sleep(wait)
            self._tokens = 0.0
            self._last = now + wait


_buckets: dict[str, _TokenBucket] = {}
_buckets_lock = threading.Lock()


def _bucket_for(host: str, rate: float) -> _TokenBucket:
    with _buckets_lock:
        bucket = _buckets.get(host)
        if bucket is None or bucket.rate != rate:
            bucket = _TokenBucket(rate)
            _buckets[host] = bucket
        return bucket
