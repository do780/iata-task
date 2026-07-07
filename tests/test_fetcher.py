"""
Tests for JSONL querying and background update support.
"""

from unittest.mock import mock_open, patch

import pytest
from requests.exceptions import RequestException

from iata_code_fetcher.fetcher import (
    CodeType,
    fetch_iata_rows,
    find_records,
    generate_codes,
    maybe_record_all_refresh_complete,
    query_local,
    to_output_record,
)


@pytest.fixture(name="carrier_response_mock")
def fixture_carrier_response():
    """
    Provides a mock HTML response for a carrier code search.
    """
    return """
    <table class="datatable">
        <thead>
        <tr>
            <td>Company name</td>
            <td>Country / Territory</td>
            <td>2-letter code</td>
        </tr>
        </thead>
        <tbody>
            <tr>
                <td>Air China Cargo</td>
                <td>China</td>
                <td>CA</td>
            </tr>
        </tbody>
    </table>
    """


@pytest.fixture(name="airport_response_mock")
def fixture_airport_response():
    """
    Provides a mock HTML response for an airport code search.
    """
    return """
    <table class="datatable">
        <thead>
        <tr>
            <td>City Name</td>
            <td>Airport Name</td>
            <td>Country / Territory</td>
            <td>3-letter location code</td>
        </tr>
        </thead>
        <tbody>
            <tr>
                <td>Shanghai</td>
                <td>Shanghai Pudong International</td>
                <td>China</td>
                <td>PVG</td>
            </tr>
        </tbody>
    </table>
    """


def test_generate_codes_for_two_letter_codes():
    """
    Test that two-character code generation is stable.
    """
    codes = list(generate_codes(2))

    assert len(codes) == 36**2
    assert codes[0] == "AA"
    assert codes[-1] == "99"


def test_to_output_record_carrier():
    """
    Test carrier rows are converted to the output schema.
    """
    row = {"2-letter code": "CA", "Company name": "Air China Cargo", "Country / Territory": "China"}

    assert to_output_record(row, CodeType.CARRIER) == {
        "Company name": "Air China Cargo",
        "Country / Territory": "China",
        "2-letter code": "CA",
    }


def test_to_output_record_airport():
    """
    Test airport rows are converted to the output schema.
    """
    row = {"3-letter location code": "PVG", "City Name": "Shanghai", "Airport Name": "Pudong"}

    assert to_output_record(row, CodeType.AIRPORT) == {
        "City Name": "Shanghai",
        "Airport Name": "Pudong",
        "3-letter location code": "PVG",
    }


@patch("requests.get")
def test_fetch_iata_rows_airport(mock_get, airport_response_mock):
    """
    Test parsing IATA HTML rows.
    """
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = airport_response_mock

    assert fetch_iata_rows("PVG", CodeType.AIRPORT) == [
        {
            "City Name": "Shanghai",
            "Airport Name": "Shanghai Pudong International",
            "Country / Territory": "China",
            "3-letter location code": "PVG",
        }
    ]
    assert "User-Agent" in mock_get.call_args.kwargs["headers"]


@patch("requests.get")
def test_fetch_iata_rows_error(mock_get):
    """
    Test network failures are raised after retry exhaustion.
    """
    mock_get.side_effect = RequestException("Network error")

    with patch("iata_code_fetcher.fetcher.sleep", return_value=None), pytest.raises(RequestException):
        fetch_iata_rows("CA", CodeType.CARRIER)


@patch("iata_code_fetcher.fetcher.get_output_file", return_value="carrier_data.jsonl")
@patch("os.path.exists", return_value=True)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"Company name":"Air China Cargo","Country / Territory":"China","2-letter code":"CA"}\n',
)
def test_find_records_from_jsonl(mock_file, mock_exists, mock_output_file):
    """
    Test local JSONL lookup by carrier code.
    """
    assert find_records(CodeType.CARRIER, "ca") == [
        {"Company name": "Air China Cargo", "Country / Territory": "China", "2-letter code": "CA"}
    ]
    mock_exists.assert_called_once_with("carrier_data.jsonl")
    mock_output_file.assert_called_once_with(CodeType.CARRIER)
    mock_file.assert_called_once_with("carrier_data.jsonl", "r", encoding="UTF-8")


@patch("iata_code_fetcher.fetcher.get_output_file", return_value="carrier_data.jsonl")
@patch("os.path.exists", return_value=False)
@patch("builtins.open", new_callable=mock_open)
def test_query_local_does_not_fetch_on_cache_miss(mock_file, mock_exists, mock_output_file):
    """
    Test that a cache miss does not perform network or write work.
    """
    result = query_local(CodeType.CARRIER, "CA")

    assert result == {"source": "miss", "data": []}
    mock_file.assert_not_called()
    mock_exists.assert_any_call("carrier_data.jsonl")
    mock_output_file.assert_any_call(CodeType.CARRIER)


@patch(
    "iata_code_fetcher.fetcher.find_records",
    return_value=[{"City Name": "Shanghai", "Airport Name": "Pudong", "3-letter location code": "PVG"}],
)
@patch("iata_code_fetcher.fetcher.fetch_iata_rows")
def test_query_local_uses_cache_only(mock_fetch, mock_find):
    """
    Test that existing local rows avoid a live request.
    """
    assert query_local(CodeType.AIRPORT, "PVG") == {
        "source": "cache",
        "data": [{"City Name": "Shanghai", "Airport Name": "Pudong", "3-letter location code": "PVG"}],
    }
    mock_find.assert_called_once_with(CodeType.AIRPORT, "PVG")
    mock_fetch.assert_not_called()


@patch("iata_code_fetcher.fetcher.append_jsonl")
@patch(
    "iata_code_fetcher.fetcher.read_jsonl",
    return_value=[
        {"event": "refresh_existing", "type": "carrier", "completed_cycle": True},
        {"event": "refresh_existing", "type": "airport", "completed_cycle": True},
    ],
)
def test_maybe_record_all_refresh_complete_marks_finished_cycle(mock_read, mock_append):
    """
    Test full refresh completion is marked after both datasets complete.
    """
    maybe_record_all_refresh_complete()

    mock_read.assert_called_once()
    assert mock_append.call_args.args[0] == "fetch_state.jsonl"
    assert mock_append.call_args.args[1]["event"] == "refresh_all_complete"


@patch("iata_code_fetcher.fetcher.append_jsonl")
@patch(
    "iata_code_fetcher.fetcher.read_jsonl",
    return_value=[
        {"event": "refresh_existing", "type": "carrier", "completed_cycle": True},
        {"event": "refresh_all_complete"},
        {"event": "refresh_existing", "type": "airport", "completed_cycle": True},
    ],
)
def test_maybe_record_all_refresh_complete_waits_for_new_cycle(mock_read, mock_append):
    """
    Test an old completion marker resets the cycle completion check.
    """
    maybe_record_all_refresh_complete()

    mock_read.assert_called_once()
    mock_append.assert_not_called()
