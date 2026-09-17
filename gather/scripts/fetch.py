from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

from curl_cffi import requests
from curl_cffi.requests import exceptions as requests_exceptions
from dotenv import load_dotenv

log = logging.getLogger(__name__)

ENV_PREFIX = "AMARA_INGESTION_"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"

# Real environment variables already set take precedence - load_dotenv does not
# override by default - so a one-off run can be tweaked without editing .env.
load_dotenv(ENV_PATH)


def _where_to_set_it(env_path: Path) -> str:
    # Point at whatever is actually there. A container has no .env and no
    # .env.example - its credentials arrive through the environment - so
    # naming a file that does not exist sends you looking for the wrong thing.
    if env_path.exists():
        return f"  Set it in the environment, or add it to {env_path}"
    example = env_path.with_name(".env.example")
    if example.exists():
        return ("  Set it in the environment, or for a local run:\n"
                f"    cp {example} {env_path}")
    return "  Pass it at run time: docker run -e ..., or an Airflow secret"


RETRY_STATUS = {500, 502, 503, 504}
THROTTLE_STATUS = 429

# Requests go out with a real browser's TLS handshake, not Python's.
#
# Several of these stores sit behind Cloudflare, which fingerprints the TLS
# ClientHello and scores it. urllib3's is recognisably not a browser, and once a
# store decides it has seen enough of it the answer to every request becomes 429
# - permanently, for that fingerprint, while the same URL keeps serving anyone
# else. Measured on three stores at once, same process, same headers, seconds
# apart:
#
#     fillingpieces   urllib3: 429    browser TLS: 200 (250 products)
#     feature         urllib3: 429    browser TLS: 200 (250 products)
#     brownsfashion   urllib3: 429    browser TLS: 200 (250 products)
#
# This is why slowing down appeared to help and then stopped helping: a lower
# rate takes longer to trip the detector, it does not avoid it. No rate setting
# recovers a fingerprint that has already been flagged, and no amount of waiting
# clears one - ten minutes of total silence did not.
#
# `chrome` tracks curl_cffi's newest Chrome profile rather than pinning a
# version, because the value of this is looking current, and a pinned profile
# ages into looking like an old browser - which is its own signal.
IMPERSONATE = "chrome"

# A throttled host gets a more patient budget than a flaky one: a 429 is the
# server telling us the rate is wrong, and it will keep being wrong until we
# slow down. See issue #10.
THROTTLE_ATTEMPTS = 6
THROTTLE_BACKOFF = 1.5      # multiplies the interval on every 429
MAX_INTERVAL = 30.0         # seconds between requests, ceiling

# Backing off used to be permanent: min_interval only ever grew, so the first
# burst of 429s set the pace for everything after it. On brownsfashion a single
# 429 at page 66 of the unfiltered listing held the next ~250 requests at
# 0.67/s, and a burst inside one collection drove the rate to the MAX_INTERVAL
# floor - one request every 30s - where the remaining ~35 collections then had
# to be crawled. Hours of crawl, for a burst that lasted seconds.
#
# So: widen hard when the host pushes back, ease off gradually once it stops.
# The two are deliberately asymmetric - 1.5x up against 0.75x down - so the rate
# settles below whatever triggered the throttling rather than oscillating onto
# it. Timing only; no page is fetched or skipped because of this.
RECOVERY_AFTER = 10         # consecutive clean requests before easing off
RECOVERY_FACTOR = 0.75      # multiplies the interval on each easing


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
            + _where_to_set_it(ENV_PATH)
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
    # told us it is wrong, so the interval widens rather than the request simply
    # being retried at the speed that caused it - and narrows again, back toward
    # this rate but never past it, once the host stops pushing back.

    def __init__(self, rate_limit_rps: float | None = None,
                 timeout: int | None = None, max_retries: int | None = None) -> None:
        if rate_limit_rps is None:
            rate_limit_rps = float(env("RATE_LIMIT_RPS"))
        self.min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0
        self.timeout = timeout if timeout is not None else int(env("TIMEOUT"))
        self.max_retries = max_retries if max_retries is not None else int(env("MAX_RETRIES"))

        self._last_request_at = 0.0
        self._clean_streak = 0    # clean requests since the last 429
        self.throttled = 0        # how many 429s this crawl has seen
        self.min_interval_initial = self.min_interval
        self.session = requests.Session(impersonate=IMPERSONATE)
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
        # React to a 429: widen the interval, and wait as asked.
        self.throttled += 1
        self._clean_streak = 0
        previous = self.min_interval
        self.min_interval = min(self.min_interval * THROTTLE_BACKOFF or 1.0, MAX_INTERVAL)
        wait = _retry_after(response)
        if wait is None:
            wait = self.min_interval * 2
        log.warning("  429 - slowing %.2f/s -> %.2f/s, waiting %.0fs",
                    1 / previous if previous else 0,
                    1 / self.min_interval if self.min_interval else 0, wait)
        return min(wait, MAX_INTERVAL * 2)

    def _recover(self) -> None:
        # Ease back toward the configured rate after a clean run of requests.
        #
        # Only ever back toward it: min_interval_initial is the floor, so this
        # cannot crawl a host faster than its config allows. A crawl with no
        # configured rate limit is left at whatever a 429 forced - there is no
        # rate to return to, and the host has already said the unlimited one
        # was wrong.
        if self.min_interval_initial <= 0 or self.min_interval <= self.min_interval_initial:
            return
        self._clean_streak += 1
        if self._clean_streak < RECOVERY_AFTER:
            return
        self._clean_streak = 0
        previous = self.min_interval
        self.min_interval = max(self.min_interval * RECOVERY_FACTOR,
                                self.min_interval_initial)
        log.info("  %d clean requests - speeding %.2f/s -> %.2f/s",
                 RECOVERY_AFTER, 1 / previous, 1 / self.min_interval)

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
            except requests_exceptions.RequestException as exc:
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
                payload = response.json()
            except ValueError:
                # A store sitting behind a bot-wall answers 200 with an HTML
                # challenge page. Fatal, not retryable.
                snippet = response.text[:120].replace("\n", " ")
                raise FetchError(f"{url} -> 200 but not JSON: {snippet!r}") from None
            # Counted only once the response proved usable, so a bot-wall
            # serving 200s cannot look like a healthy streak.
            self._recover()
            return payload

        raise FetchError(f"{url} -> gave up after {attempt - 1} attempts ({last_error})")
