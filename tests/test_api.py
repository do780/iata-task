"""
Tests for the HTTP query API routing behavior.
"""

from http import HTTPStatus
from unittest.mock import patch

from iata_code_fetcher.api import IataRequestHandler, enqueue_update
from iata_code_fetcher.fetcher import CodeType


def test_api_routes_carrier_lookup():
    """
    Test that /carriers/{code} routes to carrier lookup.
    """
    handler = object.__new__(IataRequestHandler)
    handler.path = "/carriers/ca"

    with patch.object(handler, "write_json") as write_json, patch("iata_code_fetcher.api.query_local") as query:
        query.return_value = {
            "source": "cache",
            "data": [{"Company name": "Air China Cargo", "Country / Territory": "China", "2-letter code": "CA"}],
        }

        handler.do_GET()

    assert write_json.call_args.args == (HTTPStatus.OK, query.return_value)


def test_api_queues_update_on_cache_miss():
    """
    Test that cache misses return immediately and enqueue a background update.
    """
    handler = object.__new__(IataRequestHandler)
    handler.path = "/airports/PVG"

    with patch.object(handler, "write_json") as write_json, patch("iata_code_fetcher.api.query_local") as query, patch(
        "iata_code_fetcher.api.enqueue_update", return_value="queued"
    ) as enqueue:
        query.return_value = {"source": "miss", "data": []}

        handler.do_GET()

    enqueue.assert_called_once_with(CodeType.AIRPORT, "PVG")
    assert write_json.call_args.args == (HTTPStatus.ACCEPTED, {"source": "miss", "data": [], "update": "queued"})


def test_enqueue_update_deduplicates_pending_work():
    """
    Test that repeated misses do not queue duplicate background fetches.
    """
    with patch("iata_code_fetcher.api.UPDATE_EXECUTOR.submit") as submit, patch.dict(
        "iata_code_fetcher.api.PENDING_UPDATES", {}, clear=True
    ):
        submit.return_value.done.return_value = False

        assert enqueue_update(CodeType.CARRIER, "ca") == "queued"
        assert enqueue_update(CodeType.CARRIER, "CA") == "pending"

    submit.assert_called_once()


def test_api_rejects_invalid_airport_code():
    """
    Test airport lookups require three-character codes.
    """
    handler = object.__new__(IataRequestHandler)
    handler.path = "/airports/PV"

    with patch.object(handler, "write_json") as write_json:
        handler.do_GET()

    assert write_json.call_args.args == (HTTPStatus.BAD_REQUEST, {"error": "Expected a 3-character code"})
