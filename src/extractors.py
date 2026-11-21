"""
HTTP extraction engine for historical and incremental natural hazard sources.
Implements monthly temporal partitioning, recursive temporal bisection when API
limits are approached, exponential backoff retries, and raw landing zone storage.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional, Union

import requests

logger = logging.getLogger("src.extractors")

RAW_LANDING_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
USER_AGENT = "UD-DataAnalysisProgramming-Team1-EarthquakeMonitor/1.0 (https://github.com/AngeloIbanez/earthquake-monitoring-team_1)"


def save_raw_payload(
    source_id: str,
    content: Union[Dict[str, Any], List[Any], str],
    ext: str,
    raw_dir: Path = RAW_LANDING_DIR,
) -> Path:
    """
    Persists an unmodified raw response into data/raw/ following the convention:
    source_YYYYMMDD_HHMMSS.ext
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    clean_ext = ext.lstrip(".")
    target = raw_dir / f"{source_id}_{stamp}.{clean_ext}"

    # Avoid overwriting if two chunks complete within the same second
    counter = 1
    while target.exists():
        target = raw_dir / f"{source_id}_{stamp}_{counter:02d}.{clean_ext}"
        counter += 1

    if isinstance(content, (dict, list)):
        target.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        target.write_text(str(content), encoding="utf-8")

    logger.info("Saved raw payload to landing zone: %s (%.1f KB)", target.name, target.stat().st_size / 1024.0)
    return target


def fetch_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 3,
    backoff_base: float = 1.5,
) -> requests.Response:
    """Executes an HTTP GET request with exponential backoff retries on transient failures."""
    headers = {"User-Agent": USER_AGENT}
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                wait_s = backoff_base ** attempt
                logger.warning(
                    "HTTP %d from %s (attempt %d/%d). Retrying in %.1fs...",
                    resp.status_code,
                    url,
                    attempt,
                    max_retries,
                    wait_s,
                )
                time.sleep(wait_s)
                continue
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_retries:
                wait_s = backoff_base ** attempt
                logger.warning(
                    "Network error querying %s: %s (attempt %d/%d). Retrying in %.1fs...",
                    url,
                    exc,
                    attempt,
                    max_retries,
                    wait_s,
                )
                time.sleep(wait_s)
            else:
                raise

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Exhausted {max_retries} retries for {url}")


def generate_monthly_windows(start_date_str: str, end_date_str: str) -> List[Tuple[datetime, datetime]]:
    """Partitions a [start_date, end_date] range into monthly UTC windows."""
    start_dt = datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=timezone.utc)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(days=1)

    windows: List[Tuple[datetime, datetime]] = []
    cur = start_dt
    while cur < end_dt:
        # Advance roughly 30 days per chunk
        nxt = min(cur + timedelta(days=30), end_dt)
        windows.append((cur, nxt))
        cur = nxt
    return windows


def _fetch_window_with_bisection(
    source_id: str,
    base_url: str,
    base_params: Dict[str, Any],
    win_start: datetime,
    win_end: datetime,
    max_records: int,
    save_raw: bool = True,
    depth: int = 0,
    max_bisection_depth: int = 6,
) -> List[Dict[str, Any]]:
    """
    Queries an FDSN endpoint (USGS or EMSC) for [win_start, win_end].
    If the response hits max_records or returns HTTP 400 due to query limit,
    recursively bisects the temporal window into two halves.
    """
    params = dict(base_params)
    params["starttime"] = win_start.strftime("%Y-%m-%dT%H:%M:%S")
    params["endtime"] = win_end.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(
        "Fetching [%s] window %s -> %s (bisection depth=%d)",
        source_id,
        params["starttime"],
        params["endtime"],
        depth,
    )

    resp = fetch_with_retry(base_url, params=params)

    # EMSC returns HTTP 204 No Content when 0 events match the window
    if resp.status_code == 204 or not resp.text.strip():
        logger.info("Window %s -> %s returned 0 records (HTTP 204).", params["starttime"], params["endtime"])
        return []

    # USGS returns HTTP 400 when matching events exceed the 20,000 limit
    if resp.status_code == 400 and depth < max_bisection_depth and (win_end - win_start) > timedelta(hours=1):
        mid = win_start + (win_end - win_start) / 2
        logger.warning(
            "API limit exceeded (HTTP 400) for [%s] %s -> %s. Bisecting at %s...",
            source_id,
            params["starttime"],
            params["endtime"],
            mid.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        left = _fetch_window_with_bisection(
            source_id, base_url, base_params, win_start, mid, max_records, save_raw, depth + 1, max_bisection_depth
        )
        right = _fetch_window_with_bisection(
            source_id, base_url, base_params, mid, win_end, max_records, save_raw, depth + 1, max_bisection_depth
        )
        return left + right

    resp.raise_for_status()
    payload = resp.json()
    features = payload.get("features", []) if isinstance(payload, dict) else []

    if len(features) >= max_records and depth < max_bisection_depth and (win_end - win_start) > timedelta(hours=1):
        mid = win_start + (win_end - win_start) / 2
        logger.warning(
            "Window saturated (%d >= %d records) for [%s]. Bisecting window...",
            len(features),
            max_records,
            source_id,
        )
        left = _fetch_window_with_bisection(
            source_id, base_url, base_params, win_start, mid, max_records, save_raw, depth + 1, max_bisection_depth
        )
        right = _fetch_window_with_bisection(
            source_id, base_url, base_params, mid, win_end, max_records, save_raw, depth + 1, max_bisection_depth
        )
        return left + right

    if save_raw and features:
        ext = "geojson" if "geojson" in str(base_params.get("format", "")).lower() else "json"
        save_raw_payload(source_id, payload, ext=ext)

    return [payload]


def fetch_historical_source(
    source_id: str,
    source_cfg: Dict[str, Any],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    save_raw: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetches historical data for a configured FDSN source (usgs_fdsn or emsc_fdsn)
    across partitioned monthly windows with recursive bisection.
    """
    base_url = source_cfg["base_url"]
    base_params = dict(source_cfg.get("parameters", {}))
    rate_limits = source_cfg.get("rate_limits", {})
    max_records = int(rate_limits.get("max_records_per_request", 20000))
    req_per_min = int(rate_limits.get("requests_per_minute", 60))
    polite_delay = max(0.2, 60.0 / max(req_per_min, 1))

    hist_strategy = source_cfg.get("historical_strategy", {})
    eff_start = start_date or hist_strategy.get("start_date", "2025-10-01")
    eff_end = end_date or hist_strategy.get("end_date", "2025-11-01")

    windows = generate_monthly_windows(eff_start, eff_end)
    all_payloads: List[Dict[str, Any]] = []

    for idx, (w_start, w_end) in enumerate(windows):
        payloads = _fetch_window_with_bisection(
            source_id=source_id,
            base_url=base_url,
            base_params=base_params,
            win_start=w_start,
            win_end=w_end,
            max_records=max_records,
            save_raw=save_raw,
        )
        all_payloads.extend(payloads)
        if idx < len(windows) - 1:
            time.sleep(min(polite_delay, 1.0))

    return all_payloads


def fetch_incremental_source(
    source_id: str,
    source_cfg: Dict[str, Any],
    save_raw: bool = True,
) -> Union[Dict[str, Any], str]:
    """Fetches the latest feed payload from an incremental source (USGS GeoJSON or GDACS XML)."""
    url = source_cfg.get("feed_url") or source_cfg["base_url"]
    fmt = source_cfg.get("format", "json").lower()

    logger.info("Polling incremental source [%s] at %s", source_id, url)
    resp = fetch_with_retry(url)
    resp.raise_for_status()

    if "xml" in fmt:
        content = resp.text
        if save_raw:
            save_raw_payload(source_id, content, ext="xml")
        return content
    else:
        payload = resp.json()
        if save_raw:
            ext = "geojson" if "geojson" in fmt else "json"
            save_raw_payload(source_id, payload, ext=ext)
        return payload
