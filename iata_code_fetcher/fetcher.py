"""
Fetch, store, and query IATA carrier and airport JSONL data.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from itertools import product
from string import ascii_uppercase, digits
from threading import RLock
from time import monotonic, sleep
from typing import Dict, Generator, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException


def env_int(name: str, default: int) -> int:
    """
    Read a positive integer setting from the environment.
    """
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return value if value >= 0 else default


def env_bool(name: str, default: bool = False) -> bool:
    """
    Read a boolean setting from the environment.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


BASE_URL: str = (
    "https://www.iata.org/PublicationDetails/Search/?currentBlock={block}&currentPage=12572&{type}.search={code}"
)
CARRIER_BLOCK: str = "314383"
AIRPORT_BLOCK: str = "314384"
CARRIER_FILE: str = os.environ.get("IATA_CARRIER_FILE", "carrier_data.jsonl")
AIRPORT_FILE: str = os.environ.get("IATA_AIRPORT_FILE", "airport_data.jsonl")
STATE_FILE: str = "fetch_state.jsonl"
REPORT_FREQUENCY: int = 100
MAX_RETRIES: int = 3
RETRY_DELAY: int = 15
REQUEST_DELAY: int = env_int("IATA_REQUEST_DELAY_SECONDS", 1)
REFRESH_EXISTING: bool = env_bool("IATA_REFRESH_EXISTING")
REFRESH_BATCH_SIZE: int = env_int("IATA_REFRESH_BATCH_SIZE", 50)
REFRESH_BATCH_PAUSE: int = env_int("IATA_REFRESH_BATCH_PAUSE_SECONDS", 10)
REFRESH_MAX_SECONDS: int = env_int("IATA_REFRESH_MAX_SECONDS", 17_400)
TIMEOUT: int = 20
DATA_LOCK = RLock()
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class CodeType(Enum):
    """
    IATA code dataset type.
    """

    CARRIER = "carrier"
    AIRPORT = "airport"


def generate_codes(length: int) -> Generator[str, None, None]:
    """
    Generate all possible IATA-like codes of the given length.
    """
    return ("".join(letters) for letters in product(ascii_uppercase + digits, repeat=length))


def normalize_iata_code(code: str) -> str:
    """
    Normalize an IATA code for querying and deduplication.
    """
    return code.strip().rstrip("*").upper()


def format_iata_code(code: str) -> str:
    """
    Format an IATA code for output while preserving controlled-duplicate markers.
    """
    return code.strip().upper()


def first_present(item: Dict[str, str], fields: Iterable[str]) -> str:
    """
    Return the first non-empty string value from possible field names.
    """
    for field in fields:
        value = item.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def get_output_file(code_type: CodeType) -> str:
    """
    Return the JSONL output file for a dataset type.
    """
    return CARRIER_FILE if code_type == CodeType.CARRIER else AIRPORT_FILE


def get_output_code_field(code_type: CodeType) -> str:
    """
    Return the code field used by the JSONL schema.
    """
    return "2-letter code" if code_type == CodeType.CARRIER else "3-letter location code"


def record_key(item: Dict[str, str]) -> str:
    """
    Build a stable key for detecting exact duplicate JSONL records.
    """
    return json.dumps(item, ensure_ascii=False, sort_keys=True)


def parse_iata_table(table) -> List[Dict[str, str]]:
    """
    Parse an IATA datatable into raw dictionaries.
    """
    header_row = table.find("thead").find("tr") if table.find("thead") else table.find("tr")
    if not header_row:
        return []

    headers = [cell.text.strip() for cell in header_row.find_all(["th", "td"])]
    rows = []
    body_rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]

    for row in body_rows:
        cols = [col.text.strip() for col in row.find_all(["th", "td"])]
        if cols:
            rows.append(dict(zip(headers, cols)))

    return rows


def to_output_record(item: Dict[str, str], code_type: CodeType) -> Dict[str, str]:
    """
    Convert an IATA response row to the project output schema.
    """
    if code_type == CodeType.CARRIER:
        return {
            "Company name": first_present(
                item,
                ["Company name", "company_name", "Airline name", "Carrier name", "name"],
            ),
            "Country / Territory": first_present(
                item,
                ["Country / Territory", "country_or_territory", "Country", "country"],
            ),
            "2-letter code": format_iata_code(first_present(item, ["2-letter code", "iata", "carrier_code"])),
        }

    return {
        "City Name": first_present(item, ["City Name", "city_name", "City", "city"]),
        "Airport Name": first_present(item, ["Airport Name", "airport_name"]),
        "3-letter location code": format_iata_code(first_present(item, ["3-letter location code", "iata", "code"])),
    }


def read_jsonl(file_path: str) -> List[Dict[str, str]]:
    """
    Read a JSONL file into a list of dictionaries.
    """
    with DATA_LOCK:
        if not os.path.exists(file_path):
            return []

        rows = []
        with open(file_path, "r", encoding="UTF-8") as file:
            for line in file:
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning("Skipping invalid JSONL line in %s", file_path)
                    continue
                if isinstance(item, dict):
                    rows.append(item)
        return rows


def append_jsonl(file_path: str, item: Dict[str, object]) -> None:
    """
    Append one JSON object to a JSONL file.
    """
    with DATA_LOCK:
        with open(file_path, "a", encoding="UTF-8") as file:
            file.write(json.dumps(item, ensure_ascii=False) + "\n")


def write_jsonl(file_path: str, rows: Iterable[Dict[str, str]]) -> None:
    """
    Replace a JSONL file with the given rows.
    """
    temp_path = f"{file_path}.tmp"
    with DATA_LOCK:
        with open(temp_path, "w", encoding="UTF-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(temp_path, file_path)


def find_records(code_type: CodeType, code: str) -> List[Dict[str, str]]:
    """
    Find records in the JSONL file by code.
    """
    normalized_code = normalize_iata_code(code)
    code_field = get_output_code_field(code_type)
    return [
        row
        for row in read_jsonl(get_output_file(code_type))
        if normalize_iata_code(str(row.get(code_field, ""))) == normalized_code
    ]


def record_fetch_state(
    code_type: CodeType,
    query_code: str,
    status: str,
    rows: int = 0,
    error: Optional[str] = None,
) -> None:
    """
    Record fetch progress separately from output data.
    """
    state = {
        "type": code_type.value,
        "query_code": normalize_iata_code(query_code),
        "status": status,
        "rows": rows,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        state["error"] = error
    append_jsonl(STATE_FILE, state)


def record_refresh_state(code_type: CodeType, summary: Dict[str, object]) -> None:
    """
    Record segmented refresh progress for GitHub Actions runs.
    """
    append_jsonl(
        STATE_FILE,
        {
            "event": "refresh_existing",
            "type": code_type.value,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            **summary,
        },
    )


def load_refresh_index(code_type: CodeType, total_codes: int) -> int:
    """
    Load the next refresh start index from the state file.
    """
    if total_codes <= 0:
        return 0

    next_index = 0
    for row in read_jsonl(STATE_FILE):
        if row.get("event") == "refresh_existing" and row.get("type") == code_type.value:
            try:
                next_index = int(row.get("next_index", 0))
            except (TypeError, ValueError):
                next_index = 0
    return next_index % total_codes


def record_all_refresh_complete() -> None:
    """
    Record that both datasets have completed one segmented refresh cycle.
    """
    append_jsonl(
        STATE_FILE,
        {
            "event": "refresh_all_complete",
            "carrier_file": CARRIER_FILE,
            "airport_file": AIRPORT_FILE,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def maybe_record_all_refresh_complete() -> None:
    """
    Mark full refresh completion when both datasets completed since the last marker.
    """
    completed_types = set()
    for row in read_jsonl(STATE_FILE):
        if row.get("event") == "refresh_all_complete":
            completed_types.clear()
        elif row.get("event") == "refresh_existing" and row.get("completed_cycle") is True:
            completed_types.add(row.get("type"))

    if {CodeType.CARRIER.value, CodeType.AIRPORT.value}.issubset(completed_types):
        record_all_refresh_complete()


def fetch_iata_rows(code: str, code_type: CodeType) -> List[Dict[str, str]]:
    """
    Fetch and parse raw rows from the IATA website for one query code.
    """
    normalized_code = normalize_iata_code(code)
    url = BASE_URL.format(
        block=CARRIER_BLOCK if code_type == CodeType.CARRIER else AIRPORT_BLOCK,
        type="airline" if code_type == CodeType.CARRIER else "airport",
        code=normalized_code,
    )

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"class": "datatable"})

            if not table:
                raise ValueError("No record found")

            return parse_iata_table(table)

        except RequestException as exc:
            if attempt < MAX_RETRIES - 1:
                logging.warning(
                    "Request failed for %s. Retrying in %d seconds... (Attempt %d/%d)",
                    normalized_code,
                    RETRY_DELAY,
                    attempt + 1,
                    MAX_RETRIES,
                )
                sleep(RETRY_DELAY)
            else:
                raise RequestException(f"Request failed after {MAX_RETRIES} attempts: {exc}") from exc

    return []


def fetch_and_store_code(code_type: CodeType, code: str) -> List[Dict[str, str]]:
    """
    Fetch one code from IATA, append new rows to JSONL, and return matching rows.
    """
    normalized_code = normalize_iata_code(code)
    raw_rows = fetch_iata_rows(normalized_code, code_type)
    existing_records = {record_key(row) for row in read_jsonl(get_output_file(code_type))}
    written = 0
    matched_records = []

    for row in raw_rows:
        output_record = to_output_record(row, code_type)
        record_code = normalize_iata_code(output_record.get(get_output_code_field(code_type), ""))
        if not record_code:
            continue
        if record_code == normalized_code:
            matched_records.append(output_record)
        output_record_key = record_key(output_record)
        if output_record_key not in existing_records:
            append_jsonl(get_output_file(code_type), output_record)
            existing_records.add(output_record_key)
            written += 1

    record_fetch_state(code_type, normalized_code, "success" if raw_rows else "no_record", rows=written)
    return matched_records


def load_existing_codes(code_type: CodeType) -> set[str]:
    """
    Load the set of codes already present in the JSONL file.
    """
    return {
        normalize_iata_code(str(row.get(get_output_code_field(code_type), "")))
        for row in read_jsonl(get_output_file(code_type))
    }


def rows_by_code(rows: Iterable[Dict[str, str]], code_type: CodeType) -> Dict[str, List[Dict[str, str]]]:
    """
    Group rows by normalized IATA code while preserving row order.
    """
    code_field = get_output_code_field(code_type)
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        code = normalize_iata_code(str(row.get(code_field, "")))
        if code:
            grouped.setdefault(code, []).append(row)
    return grouped


def flatten_grouped_rows(grouped: Dict[str, List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """
    Flatten grouped rows in code order with exact duplicate records removed.
    """
    flattened = []
    seen = set()
    for code in sorted(grouped):
        for row in grouped[code]:
            key = record_key(row)
            if key not in seen:
                flattened.append(row)
                seen.add(key)
    return flattened


def refresh_code_rows(code_type: CodeType, code: str) -> List[Dict[str, str]]:
    """
    Fetch fresh rows for one existing code.
    """
    updated_rows = []
    for raw_row in fetch_iata_rows(code, code_type):
        output_record = to_output_record(raw_row, code_type)
        record_code = normalize_iata_code(output_record.get(get_output_code_field(code_type), ""))
        if record_code == code:
            updated_rows.append(output_record)
    return updated_rows


def refresh_existing_dataset(code_type: CodeType, max_seconds: int = REFRESH_MAX_SECONDS) -> None:
    """
    Refresh a time-bounded segment of existing codes and preserve old rows on failures.
    """
    output_file = get_output_file(code_type)
    grouped = rows_by_code(read_jsonl(output_file), code_type)
    codes = sorted(grouped)
    total_codes = len(codes)
    if not codes:
        logging.info("No existing %s codes found in %s to refresh.", code_type.value, output_file)
        return

    start_index = load_refresh_index(code_type, total_codes)
    current_index = start_index
    summary = {"processed": 0, "refreshed": 0, "failed": 0, "completed_cycle": False}
    started_at = monotonic()

    logging.info(
        "Refreshing existing %s codes from index %d/%d into %s.",
        code_type.value,
        start_index,
        total_codes,
        output_file,
    )

    while monotonic() - started_at < max_seconds:
        code = codes[current_index]
        try:
            updated_rows = refresh_code_rows(code_type, code)
            if updated_rows:
                grouped[code] = updated_rows
                summary["refreshed"] += 1
                record_fetch_state(code_type, code, "refreshed", rows=len(updated_rows))
            else:
                summary["failed"] += 1
                record_fetch_state(code_type, code, "no_record")
        except (RequestException, ValueError) as exc:
            summary["failed"] += 1
            record_fetch_state(code_type, code, "error", error=str(exc))
            logging.warning("Refresh failed for %s %s: %s", code_type.value, code, exc)

        summary["processed"] += 1
        current_index = (current_index + 1) % total_codes
        if current_index == start_index:
            summary["completed_cycle"] = True
            break

        if summary["processed"] % REPORT_FREQUENCY == 0:
            logging.info("Refreshed %d %s codes in this run.", summary["processed"], code_type.value)
        if REFRESH_BATCH_SIZE and summary["processed"] % REFRESH_BATCH_SIZE == 0:
            logging.info("Pausing %d seconds after %d refreshes.", REFRESH_BATCH_PAUSE, summary["processed"])
            sleep(REFRESH_BATCH_PAUSE)
        sleep(REQUEST_DELAY)

    write_jsonl(output_file, flatten_grouped_rows(grouped))
    summary.update({"next_index": current_index, "total_codes": total_codes})
    record_refresh_state(code_type, summary)
    logging.info(
        "Segment refresh for %ss saved to %s. Processed=%d refreshed=%d failed=%d next_index=%d.",
        code_type.value,
        output_file,
        summary["processed"],
        summary["refreshed"],
        summary["failed"],
        current_index,
    )


def query_local(code_type: CodeType, code: str) -> Dict[str, object]:
    """
    Query local JSONL data only, without triggering network updates.
    """
    local_records = find_records(code_type, code)
    if local_records:
        return {"source": "cache", "data": local_records}
    return {"source": "miss", "data": []}


def batch_fetch_dataset(code_type: CodeType, skip_existing: bool = True) -> None:
    """
    Batch fetch all possible codes for a dataset type.
    """
    processed = 0
    skipped = 0
    existing_codes = load_existing_codes(code_type)

    for code in generate_codes(2 if code_type == CodeType.CARRIER else 3):
        normalized_code = normalize_iata_code(code)
        if skip_existing and normalized_code in existing_codes:
            skipped += 1
            continue

        try:
            records = fetch_and_store_code(code_type, normalized_code)
            for record in records:
                existing_codes.add(normalize_iata_code(record.get(get_output_code_field(code_type), "")))
            existing_codes = load_existing_codes(code_type)
        except RequestException as exc:
            logging.error("For %s: %s", normalized_code, exc)
        except ValueError:
            record_fetch_state(code_type, normalized_code, "no_record")

        processed += 1
        if processed % REPORT_FREQUENCY == 0:
            logging.info("Processed %s %s codes so far...", processed, code_type.value)
        sleep(REQUEST_DELAY)

    logging.info(
        "Data extraction for %ss completed. Results are saved in %s. Skipped %d existing codes.",
        code_type.value,
        get_output_file(code_type),
        skipped,
    )


def main() -> None:
    """
    Batch update both carrier and airport datasets.
    """
    if REFRESH_EXISTING:
        started_at = monotonic()
        for code_type in (CodeType.CARRIER, CodeType.AIRPORT):
            remaining_seconds = REFRESH_MAX_SECONDS - int(monotonic() - started_at)
            if remaining_seconds <= 0:
                logging.info("Refresh time budget exhausted before %s refresh.", code_type.value)
                break
            refresh_existing_dataset(code_type, remaining_seconds)
        maybe_record_all_refresh_complete()
        return

    batch_fetch_dataset(CodeType.CARRIER)
    batch_fetch_dataset(CodeType.AIRPORT)


if __name__ == "__main__":
    main()
