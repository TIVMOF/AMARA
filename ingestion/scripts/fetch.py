"""HTTP layer. The only module that talks to the network.

Adapters ask this for JSON and get JSON back. Rate limiting, retries and the
User-Agent live here so no adapter has to think about them.

Every tunable comes from .env (see .env.example) and there are no fallbacks in
code - a missing key raises immediately rather than silently using a stale
value that .env appears to control but does not.
"""

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

RETRY_STATUS = {429, 500, 502, 503, 504}


class ConfigError(RuntimeError):
    """A required setting is missing from the environment."""


class FetchError(RuntimeError):
    """A URL could not be fetched, or did not return JSON."""


def env(name: str) -> str:
    """Read a required AMARA_INGESTION_* setting."""
    value = os.getenv(ENV_PREFIX + name)
    if value in (None, ""):
        raise ConfigError(
            f"{ENV_PREFIX}{name} is not set.\n"
            f"  Expected it in {ENV_PATH}\n"
            f"  Fix: cp {ENV_PATH.parent}/.env.example {ENV_PATH}"
        )
    return value


class Fetcher:
    """A rate-limited JSON client. One instance per crawl.

    `rate_limit_rps` is requests per second against a single host. Shopify's
    product endpoints are generous, but there is no reason to hammer them.
    Pass it explicitly to honour a site's own rate_limit_rps; omit it to take
    the default from .env.
    """

    def __init__(self, rate_limit_rps: float | None = None,
                 timeout: int | None = None, max_retries: int | None = None) -> None:
        if rate_limit_rps is None:
            rate_limit_rps = float(env("RATE_LIMIT_RPS"))
        self.min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0
        self.timeout = timeout if timeout is not None else int(env("TIMEOUT"))
        self.max_retries = max_retries if max_retries is not None else int(env("MAX_RETRIES"))

        self._last_request_at = 0.0
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

    def get_json(self, url: str, *, allow_404: bool = False) -> Any | None:
        """GET a URL and parse it as JSON.

        Returns None on 404 when `allow_404` is set - used to probe endpoints
        that may not exist on a given store. Raises FetchError otherwise.
        """
        last_error = "unknown"

        for attempt in range(1, self.max_retries + 1):
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

        raise FetchError(f"{url} -> gave up after {self.max_retries} attempts ({last_error})")
