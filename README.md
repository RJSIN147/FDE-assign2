# NYC Yellow Taxi Trip Reliability

This is my FDE Data Foundations project for the NYC TLC track.

## What I wanted to find out

I treated a Yellow Taxi trip as one small part of an operational workflow: the meter starts at pickup and stops at drop-off. My question was:

> Which pickup areas have a high share of long completed taxi trips, and are there enough trips there to make the pattern worth investigating?

My main KPI is the **long-trip rate**: the percentage of valid completed trips that took more than 45 minutes.

This can help an operations or policy team decide where to look next. For example, a busy pickup zone with a high P90 trip duration might be checked for airport queues, traffic, construction, or route issues. It does not prove what caused a long trip.

## One important choice I made

At first, I considered using the word *delay* or *customer wait time*. I decided not to. The TLC Yellow Taxi data starts when the meter is turned on. It does not tell me when a customer requested a taxi, when a driver accepted, or how long the customer waited.

So this project measures **pickup-to-drop-off journey time only**. I think being clear about this is more useful than producing a misleading wait-time metric.

## Who could use this?

- A TLC operations/policy analyst deciding which areas need a closer look.
- A transport operations lead who wants a monthly view of unusually long journeys.
- A data team that needs a repeatable version of the same calculation each month.

## Data I used

| What I needed | Source | Row means | How the pipeline gets it |
| --- | --- | --- | --- |
| Pickup time, drop-off time, pickup/drop-off LocationIDs, distance and fare | [NYC TLC Yellow Taxi trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | One completed taxi trip | Downloaded monthly PARQUET file |
| Borough and zone name for each LocationID | [NYC Open Data Taxi Zones](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/8meu-9t5y/about_data) | One taxi zone | JSON API response |

This gives the project two different retrieval styles: a downloaded PARQUET file and a JSON API. In a live run, both raw responses are kept under `data/raw/month=YYYY-MM/` along with a small receipt containing the URL, file hash, size, and time of retrieval.

More detail is in [source_map.md](docs/source_map.md).

## Workflow and model

The observable workflow is:

```text
customer looks for taxi  →  meter starts (pickup)  →  trip  →  meter stops (drop-off)
       not in data                 in data                         in data
```

I join the pickup and drop-off LocationIDs to the Taxi Zones source. Valid trips become `fact_trip_events.csv`; rows that fail checks are kept separately in `rejected_trips.csv`. See [workflow_data_model.md](docs/workflow_data_model.md) for the diagram and fields.

## Checks I added

I did not silently clean questionable rows. A trip is rejected if it has:

- a missing or invalid pickup/drop-off timestamp;
- a drop-off before pickup, or a duration over 240 minutes;
- a pickup date outside the chosen month;
- a negative trip distance; or
- a pickup or drop-off LocationID that cannot be found in the zone reference.

`total_amount` is different: a negative or missing amount becomes a warning instead of an automatic rejection, because refunds or adjustments are possible and fare is not used to calculate duration.

The pipeline stops before publishing metrics if more than 10% of rows are rejected. The rejected rows and reason codes are still saved, so the failure can be investigated.

## Metrics in the output

All metrics use only accepted trips.

| Metric | What it means |
| --- | --- |
| Valid completed trips | Number of trips left after validation |
| P50 duration | Typical pickup-to-drop-off time |
| P90 duration | A view of the slower end of the trip-duration distribution |
| Long-trip rate | Share of trips over 45 minutes; this is the main KPI |
| Largest pickup-zone share | Shows how concentrated the trip volume is in one pickup area |

P50 and P90 use the nearest-rank method. The output also includes a `pickup_zone_metrics.csv` file with the same duration measures for every pickup zone.

## Results from the January 2024 live run

I ran the live pipeline on the official January 2024 Yellow Taxi file (2,964,624 raw rows). It accepted 2,931,068 rows, or 98.8681%.

| Metric | January 2024 result |
| --- | ---: |
| P50 duration | 11.6 minutes |
| P90 duration | 28.6 minutes |
| Long-trip rate | 2.8959% |
| Largest pickup-zone share | 4.8741% |

The detailed evidence, including the source hash and validation counts, is in [evidence.md](docs/evidence.md). I would use the zone-level file to choose areas to investigate, but I would not present this as proof that a particular zone or driver caused a problem.

## How to run it

Python 3.10+ is enough for the fixture. The live run also needs `pyarrow` because TLC publishes the trip data as PARQUET.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# Small offline example. This is synthetic data made to exercise the checks.
python3 scripts/run_pipeline.py --month 2024-01 --mode fixture

# Full official TLC run. Downloads the raw files only if they are not already present.
python3 scripts/run_pipeline.py --month 2024-01 --mode live

# Tests for the main checks and rerun behaviour.
python3 -m unittest discover -s tests -v
```

The output path is printed after a successful run. It contains:

```text
kpi_summary.csv             # one row with the main metrics
pickup_zone_metrics.csv     # one row per pickup zone
fact_trip_events.csv        # accepted trips after the zone join
rejected_trips.csv          # rejected raw rows and reasons
profile.json                # basic row/column profile
validation_report.json      # acceptance rate and validation counts
run_manifest.json           # run inputs and settings
pipeline.log.jsonl          # short run log
```

Large live files and generated outputs are ignored by Git. The small fixture files are included so the project can still be run and checked without downloading a full month.

## Rerunning it

The script reuses an existing raw download unless I pass `--force-retrieve`. It uses the input hashes and settings to create a stable output folder, so an unchanged rerun reuses the same result. It also locks one month at a time, which prevents two commands from writing to the same folder at once.

## Things this project cannot answer

This is only Yellow Taxi data. It does not include green taxis, Uber/Lyft, customer requests, cancellations, traffic data, route geometry, or customer waiting time. I list the rest of my assumptions and limitations in [known_unknown_assumptions_limitations.md](docs/known_unknown_assumptions_limitations.md).

## Demo

I wrote a short [demo_script.md](docs/demo_script.md) for the required 3–5 minute walkthrough. My main FDE judgement call to explain is why I used journey time and did not pretend it was wait time.
