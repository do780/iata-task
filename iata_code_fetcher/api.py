"""
HTTP query API for local IATA JSONL data with asynchronous update fallback.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import unquote, urlparse

from requests.exceptions import RequestException

from iata_code_fetcher.fetcher import (
    CodeType,
    fetch_and_store_code,
    normalize_iata_code,
    query_local,
    record_fetch_state,
)

UPDATE_WORKERS = 2
UPDATE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=UPDATE_WORKERS,
    thread_name_prefix="iata-update",
)
UPDATE_LOCK = Lock()
PENDING_UPDATES: dict[tuple[CodeType, str], concurrent.futures.Future] = {}


def run_update(code_type: CodeType, code: str) -> None:
    """
    Fetch and store one missing code in a background worker.
    """
    try:
        fetch_and_store_code(code_type, code)
    except ValueError:
        record_fetch_state(code_type, code, "no_record")
    except RequestException as exc:
        record_fetch_state(code_type, code, "error", error=str(exc))
        logging.warning("Background update failed for %s %s: %s", code_type.value, code, exc)


def cleanup_update(key: tuple[CodeType, str], future: concurrent.futures.Future) -> None:
    """
    Remove completed updates from the in-memory pending registry.
    """
    with UPDATE_LOCK:
        if PENDING_UPDATES.get(key) is future:
            PENDING_UPDATES.pop(key, None)


def enqueue_update(code_type: CodeType, code: str) -> str:
    """
    Schedule a background update unless the same code is already pending.
    """
    key = (code_type, normalize_iata_code(code))
    with UPDATE_LOCK:
        existing = PENDING_UPDATES.get(key)
        if existing and not existing.done():
            return "pending"

        future = UPDATE_EXECUTOR.submit(run_update, code_type, key[1])
        PENDING_UPDATES[key] = future
        future.add_done_callback(lambda completed, update_key=key: cleanup_update(update_key, completed))
        return "queued"


class IataRequestHandler(BaseHTTPRequestHandler):
    """
    Minimal JSON API handler.
    """

    server_version = "IataCodeFetcher/0.1"

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        """
        Handle carrier and airport lookups.
        """
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.strip("/").split("/") if part]

        if len(parts) != 2 or parts[0] not in {"carriers", "airports"}:
            self.write_json(
                HTTPStatus.NOT_FOUND,
                {"error": "Use /carriers/{code} or /airports/{code}"},
            )
            return

        code_type = CodeType.CARRIER if parts[0] == "carriers" else CodeType.AIRPORT
        code = normalize_iata_code(parts[1])
        expected_length = 2 if code_type == CodeType.CARRIER else 3
        if len(code) != expected_length:
            self.write_json(HTTPStatus.BAD_REQUEST, {"error": f"Expected a {expected_length}-character code"})
            return

        result = query_local(code_type, code)
        if result["data"]:
            self.write_json(HTTPStatus.OK, result)
            return

        result["update"] = enqueue_update(code_type, code)
        self.write_json(HTTPStatus.ACCEPTED, result)

    def log_message(self, format: str, *args: object) -> None:  # pylint: disable=redefined-builtin,arguments-differ
        """
        Keep default request logging quiet for library-style use.
        """

    def write_json(self, status: HTTPStatus, payload: dict) -> None:
        """
        Write a JSON response.
        """
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    """
    Run the HTTP query API.
    """
    server = ThreadingHTTPServer((host, port), IataRequestHandler)
    print(f"IATA query API running at http://{host}:{port}")
    server.serve_forever()


def main() -> None:
    """
    CLI entry point for the HTTP API.
    """
    run()


if __name__ == "__main__":
    main()
