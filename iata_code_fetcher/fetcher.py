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
from time import sleep
from typing import Dict, Generator, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

BASE_URL: str = (
    "https://www.iata.org/PublicationDetails/Search/?currentBlock={block}&currentPage=12572&{type}.search={code}"
)
CARRIER_BLOCK: str = "314383"
AIRPORT_BLOCK: str = "314384"
CARRIER_FILE: str = "carrier_data.jsonl"
AIRPORT_FILE: str = "airport_data.jsonl"
STATE_FILE: str = "fetch_state.jsonl"
REPORT_FREQUENCY: int = 100
MAX_RETRIES: int = 3
RETRY_DELAY: int = 15
REQUEST_DELAY: int = 1
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
    batch_fetch_dataset(CodeType.CARRIER)
    batch_fetch_dataset(CodeType.AIRPORT)


if __name__ == "__main__":
    main()
