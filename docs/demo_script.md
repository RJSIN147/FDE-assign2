# Short demo plan (about 4 minutes)

## 1. The problem — 30 seconds

Start with the README. Say that I wanted a repeatable way to find pickup areas with a lot of unusually long Yellow Taxi journeys. My main KPI is the share of valid trips longer than 45 minutes.

## 2. Sources — 45 seconds

Open `source_map.md`. Show the two inputs: the monthly TLC trip PARQUET and the Taxi Zones JSON API. Explain that the trip file has numeric location IDs, so I needed the zone source to make the output understandable.

## 3. My important judgement call — 45 seconds

Open `workflow_data_model.md`. Explain that the data only starts when the meter starts. Because of that, I did not call journey duration “wait time” or “late pickup”. I can only measure pickup-to-drop-off time honestly.

## 4. Run and validation — 60 seconds

Run the fixture command from the README. Open `validation_report.json` and `rejected_trips.csv` in the output folder. Point out that bad rows were not silently fixed: the output shows why each one was rejected.

## 5. Metrics and decision — 45 seconds

Open `kpi_summary.csv` and `pickup_zone_metrics.csv`. Explain P50, P90, and the long-trip rate. Say I would use high-volume zones with high P90 values as places to investigate with more data, not as proof that a zone caused the issue.

## 6. Dependability — 30 seconds

Show `run_manifest.json` and the raw source receipt. Mention that reruns reuse the saved input and that the script stops before publishing metrics when too many rows fail validation.
