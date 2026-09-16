from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

ENV_PREFIX = "AMARA_INGESTION_"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"

# Real environment variables already set take precedence - load_dotenv does not
# override by default - so a one-off run can be tweaked without editing .env.
load_dotenv(ENV_PATH)

RETRY_STATUS = {500, 502, 503, 504}
THROTTLE_STATUS = 429

# A throttled host gets a more patient budget than a flaky one: a 429 is the
# server telling us the rate is wrong, and it will keep being wrong until we
# slow down. See issue #10.
THROTTLE_ATTEMPTS = 6
THROTTLE_BACKOFF = 1.5      # multiplies the interval on every 429
MAX_INTERVAL = 30.0         # seconds between requests, ceiling


# ── errors ──────────────────────────────────────────────────────────────────

class ConfigError(RuntimeError):
    # A required setting is missing from the environment.
    pass


class FetchError(RuntimeError):
    # A URL could not be fetched, or did not return JSON.
    pass


# ── configuration ───────────────────────────────────────────────────────────

def env(name: str) -> str:
    # Read a required AMARA_INGESTION_* setting.
    value = os.getenv(ENV_PREFIX + name)
    if value in (None, ""):
        raise ConfigError(
            f"{ENV_PREFIX}{name} is not set.\n"
            f"  Expected it in {ENV_PATH}\n"
            f"  Fix: cp {ENV_PATH.parent}/.env.example {ENV_PATH}"
        )
    return value


# ── back-off ────────────────────────────────────────────────────────────────

def _retry_after(response: requests.Response) -> float | None:
    # Seconds to wait, if the server said. Handles both header forms.
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        from datetime import datetime, timezone
        when = parsedate_to_datetime(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


# ── fetching ────────────────────────────────────────────────────────────────

class Fetcher:
    # A rate-limited JSON client. One instance per crawl.
    #
    # `rate_limit_rps` is requests per second against a single host. Pass it
    # explicitly to honour a site's own rate_limit_rps; omit it to take the
    # default from .env.
    #
    # The rate is a starting point, not a fixed setting. A 429 means the host has
    # told us it is wrong, so the interval widens for the rest of the crawl
    # rather than the request simply being retried at the speed that caused it.

    def __init__(self, rate_limit_rps: float | None = None,
                 timeout: int | None = None, max_retries: int | None = None) -> None:
        if rate_limit_rps is None:
            rate_limit_rps = float(env("RATE_LIMIT_RPS"))
        self.min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0
        self.timeout = timeout if timeout is not None else int(env("TIMEOUT"))
        self.max_retries = max_retries if max_retries is not None else int(env("MAX_RETRIES"))

        self._last_request_at = 0.0
        self.throttled = 0        # how many 429s this crawl has seen
        self.min_interval_initial = self.min_interval
        self.session = requests.Session()
        # No Accept-Language on purpose. Some stores treat it as a request for
        # a locale-filtered catalogue and silently serve fewer products -
        # notre-shop drops from 249 to 142 per page. See issue #6. A site that
        # genuinely needs a locale should say so in its own config, where the
        # choice is visible.
        self.session.headers.update({
            "User-Agent": env("USER_AGENT"),
            "Accept": "application/json, text/plain, */*",
        })

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_at = time.monotonic()

    def _back_off(self, response: requests.Response) -> float:
        # React to a 429: widen the interval for good, and wait as asked.
        self.throttled += 1
        previous = self.min_interval
        self.min_interval = min(self.min_interval * THROTTLE_BACKOFF or 1.0, MAX_INTERVAL)
        wait = _retry_after(response)
        if wait is None:
            wait = self.min_interval * 2
        log.warning("  429 - slowing %.2f/s -> %.2f/s, waiting %.0fs",
                    1 / previous if previous else 0,
                    1 / self.min_interval if self.min_interval else 0, wait)
        return min(wait, MAX_INTERVAL * 2)

    def get_json(self, url: str, *, allow_404: bool = False) -> Any | None:
        # GET a URL and parse it as JSON.
        #
        # Returns None on 404 when `allow_404` is set - used to probe endpoints
        # that may not exist on a given store. Raises FetchError otherwise.
        last_error = "unknown"
        attempt = 0
        throttle_attempts = 0

        while True:
            attempt += 1
            if attempt > self.max_retries + throttle_attempts:
                break
            self._throttle()
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("  %s (%d/%d) %s", url, attempt, self.max_retries, last_error)
                time.sleep(2 ** attempt)
                continue

            if response.status_code == 404 and allow_404:
                return None

            if response.status_code == THROTTLE_STATUS:
                # Not a fault - the host is telling us the rate is wrong.
                last_error = "HTTP 429"
                if throttle_attempts < THROTTLE_ATTEMPTS:
                    throttle_attempts += 1
                    time.sleep(self._back_off(response))
                    continue
                raise FetchError(f"{url} -> still 429 after {THROTTLE_ATTEMPTS} slowdowns")

            if response.status_code in RETRY_STATUS:
                last_error = f"HTTP {response.status_code}"
                log.warning("  %s (%d/%d) %s", url, attempt, self.max_retries, last_error)
                time.sleep(2 ** attempt)
                continue

            if response.status_code != 200:
                raise FetchError(f"{url} -> HTTP {response.status_code}")

            try:
                return response.json()
            except ValueError:
                # A store sitting behind a bot-wall answers 200 with an HTML
                # challenge page. Fatal, not retryable.
                snippet = response.text[:120].replace("\n", " ")
                raise FetchError(f"{url} -> 200 but not JSON: {snippet!r}") from None

        raise FetchError(f"{url} -> gave up after {attempt - 1} attempts ({last_error})")
