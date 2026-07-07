
# IATA Code Data Fetcher

## Overview
This Python project fetches airline carrier and airport code data from the IATA (International Air Transport Association) publication pages. It stores JSON Lines data using IATA-style field names and keeps fetch progress in a separate state file.

## Features
- Generates all possible combinations of two-letter and three-letter codes.
- Fetches data using the generated codes from specific IATA publication URLs.
- Parses HTML responses to extract relevant table data.
- Saves carrier and airport records in JSON Lines format using IATA-style field names.
- Tracks fetch progress and errors separately in `fetch_state.jsonl`.
- Provides a local HTTP query API that reads JSONL first and queues background IATA updates on cache misses.

## Requirements
This project is managed with uv to handle dependencies and environments. You will need:
- Python 3.10 or higher
- uv for dependency management

## Setup Instructions

1. **Install uv**: Install uv by following the instructions on the [official uv website](https://docs.astral.sh/uv/getting-started/installation/).

2. **Clone the Repository**: Clone this repository to your local machine.

3. **Install Dependencies**: Navigate to the project directory and run the following command to install the necessary dependencies:
   ```bash
   uv sync
   ```

## Usage

To batch update both datasets, run:
```bash
uv run iata-fetch
```
The script will begin processing and will save data to `.jsonl` files named `carrier_data.jsonl` and `airport_data.jsonl` for carrier and airport data, respectively.
Fetch progress is tracked separately in `fetch_state.jsonl`.
By default, existing IATA codes in those output files are skipped, so rerunning the fetcher appends only codes that are not already present.
Controlled-duplicate markers such as `AB*` are preserved in JSONL output, while lookups normalize the marker so `/carriers/AB` can still find matching records.

The output files can be changed with environment variables. The monthly GitHub Actions workflow writes to the full data files:

```bash
IATA_CARRIER_FILE=carrier_data_full.jsonl IATA_AIRPORT_FILE=airport_data_full.jsonl uv run iata-fetch
```

The script includes rate limiting to prevent excessive requests to the IATA server, with a 1-second sleep interval between requests.

To start the query API, run:

```bash
uv run iata-api
```

Then query local data with asynchronous update fallback:

```bash
curl http://127.0.0.1:8000/carriers/CA
curl http://127.0.0.1:8000/airports/PVG
```

If a record is already present in JSONL, the API returns `200 OK` with `source: "cache"`.
If it is missing, the API does not wait for IATA. It returns `202 Accepted` with `source: "miss"` and `update: "queued"` or `update: "pending"`, then a background worker fetches IATA and appends any new records to JSONL.

## Output
Output files are generated in the project root:
- `carrier_data.jsonl`: Contains airline carrier data.
- `airport_data.jsonl`: Contains airport data.
- `carrier_data_full.jsonl`: Full carrier data maintained by the monthly GitHub Actions workflow.
- `airport_data_full.jsonl`: Full airport data maintained by the monthly GitHub Actions workflow.
- `fetch_state.jsonl`: Contains fetch progress and errors.

Each line in the `.jsonl` files represents one carrier or airport.

Carrier rows use this schema:

```json
{"Company name":"American Airlines Inc.","Country / Territory":"UNITED STATES OF AMERICA","2-letter code":"AA"}
```

If the IATA response does not include a country or territory, `Country / Territory` is written as an empty string.

Airport rows use this schema:

```json
{"City Name":"Anaa","Airport Name":"Anaa Airport","3-letter location code":"AAA"}
```

If the IATA response does not include an airport name, `Airport Name` is written as an empty string.

API responses use this shape:

```json
{"source":"cache","data":[{"City Name":"Anaa","Airport Name":"Anaa Airport","3-letter location code":"AAA"}]}
```

Cache misses return immediately while the update runs in the background:

```json
{"source":"miss","data":[],"update":"queued"}
```

## Notes
- Make sure to comply with IATA's terms of service regarding the use of data fetched from their site.
- The codes marked with an asterisk * refer to “Controlled Duplicate” where two carriers have the same code but operate different types of non-overlapping services. Example: "BB*"
- Check also [List of IATA-indexed railway stations](https://en.wikipedia.org/wiki/List_of_IATA-indexed_railway_stations).
