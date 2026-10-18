"""
utils/api_utils.py
------------------
Shared helpers for hitting the DummyJSON REST API.
All ingestion scripts (products, users, carts) should import from here
instead of duplicating request logic.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

import requests

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT  = 10        # seconds per request
DEFAULT_RETRIES  = 3
DEFAULT_BACKOFF  = 2         # seconds between retries (doubles each attempt)
DEFAULT_LIMIT    = 100       # records per page
DEFAULT_SKIP     = 0


def fetch_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    backoff: int = DEFAULT_BACKOFF,
) -> Dict[str, Any]:
    """
    GET *url* with query *params*, retrying on network errors or 5xx responses.

    Parameters
    ----------
    url     : Full endpoint URL.
    params  : Optional query-string dict (e.g. {"limit": 100, "skip": 0}).
    timeout : Per-attempt socket timeout in seconds.
    retries : Max number of retry attempts after the first failure.
    backoff : Initial wait in seconds; doubles on each retry.

    Returns
    -------
    Parsed JSON response as a dict.

    Raises
    ------
    RuntimeError if all attempts are exhausted.
    """
    attempt = 0
    wait    = backoff

    while attempt <= retries:
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            log.debug(f"GET {response.url} → {response.status_code}")
            return response.json()

        except requests.exceptions.HTTPError as exc:
            # A Response is falsy for 4xx/5xx, so compare against None explicitly.
            status = exc.response.status_code if exc.response is not None else "unknown"
            # Don't retry client errors (4xx) — they won't fix themselves.
            if exc.response is not None and exc.response.status_code < 500:
                log.error(f"Client error {status} for {url}. Not retrying.")
                raise

            log.warning(f"Server error {status} on attempt {attempt + 1}/{retries + 1}. "
                        f"Retrying in {wait}s …")

        except requests.exceptions.RequestException as exc:
            log.warning(f"Request failed on attempt {attempt + 1}/{retries + 1}: {exc}. "
                        f"Retrying in {wait}s …")

        attempt += 1
        if attempt <= retries:
            time.sleep(wait)
            wait *= 2

    raise RuntimeError(f"All {retries + 1} attempts failed for {url}")


def fetch_all_pages(
    base_url: str,
    total_key: str = "total",
    data_key:  str = "products",
    limit:     int = DEFAULT_LIMIT,
    timeout:   int = DEFAULT_TIMEOUT,
    retries:   int = DEFAULT_RETRIES,
) -> list:
    """
    Paginate through a DummyJSON-style endpoint and return all records.

    DummyJSON pattern:
        GET /products?limit=100&skip=0
        → { "products": [...], "total": 194, "skip": 0, "limit": 100 }

    Parameters
    ----------
    base_url  : Endpoint without query params (e.g. "https://dummyjson.com/products").
    total_key : JSON key that holds the total record count.
    data_key  : JSON key that holds the current page's records.
    limit     : Records per page.

    Returns
    -------
    List of all records across all pages.
    """
    all_records = []
    skip        = DEFAULT_SKIP

    # First request to find total
    first_page = fetch_with_retry(
        base_url,
        params={"limit": limit, "skip": skip},
        timeout=timeout,
        retries=retries,
    )
    total = first_page.get(total_key, 0)
    all_records.extend(first_page.get(data_key, []))
    log.info(f"Page 1 fetched. Total records reported by API: {total}")

    skip += limit
    while skip < total:
        page = fetch_with_retry(
            base_url,
            params={"limit": limit, "skip": skip},
            timeout=timeout,
            retries=retries,
        )
        records = page.get(data_key, [])
        if not records:
            break
        all_records.extend(records)
        log.info(f"Fetched {len(all_records)}/{total} records (skip={skip})")
        skip += limit

    log.info(f"Pagination complete. Total fetched: {len(all_records)}")
    return all_records


def save_bronze_snapshot(records: list, data_key: str, output_dir: str) -> str:
    """
    Write *records* to a timestamped JSON file in *output_dir*.

    The file keeps the DummyJSON envelope ({data_key: [...], "total": n}) so
    the Silver jobs can explode the same key regardless of how many pages
    were fetched. The YYYYMMDD_HHMMSS suffix sorts chronologically, which the
    Silver jobs rely on to keep the latest version of each record.

    Returns the path of the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"{data_key}_{timestamp}.json")

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump({data_key: records, "total": len(records)}, file, indent=4)

    return output_file