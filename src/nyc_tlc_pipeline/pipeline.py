"""Small monthly pipeline for the NYC TLC assignment.

The fixture can run with Python's standard library. The live TLC PARQUET file
needs pyarrow, which is listed in requirements.txt.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import math
import os
import shutil
import tempfile
from array import array
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from . import PIPELINE_VERSION

TRIP_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
)
FACT_COLUMNS = (
    "trip_event_id",
    "source_row_number",
    "pickup_at",
    "dropoff_at",
    "pickup_zone_id",
    "pickup_borough",
    "pickup_zone",
    "dropoff_zone_id",
    "dropoff_borough",
    "dropoff_zone",
    "trip_distance_miles",
    "total_amount",
    "journey_duration_minutes",
    "is_long_journey",
)
LONG_JOURNEY_MINUTES = 45.0
MAX_DURATION_MINUTES = 240.0
DEFAULT_MAX_INVALID_RATE = 0.10
TLC_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_API_URL = "https://data.cityofnewyork.us/resource/8meu-9t5y.json"


class PipelineError(RuntimeError):
    """A clear error message for someone running the script."""


class QualityGateFailed(PipelineError):
    """The files were read, but too many rows failed the data checks."""


@dataclass
class ValidationResult:
    fact: dict[str, Any] | None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class MetricAccumulator:
    # `array('d')` keeps a full TLC month of exact percentile inputs compact.
    durations: array = field(default_factory=lambda: array("d"))
    long_journeys: int = 0
    pickup_counts: Counter[int] = field(default_factory=Counter)
    by_pickup_zone: dict[int, array] = field(default_factory=lambda: defaultdict(lambda: array("d")))
    pickup_zone_details: dict[int, tuple[str, str]] = field(default_factory=dict)

    def add(self, fact: dict[str, Any]) -> None:
        duration = float(fact["journey_duration_minutes"])
        pickup_id = int(fact["pickup_zone_id"])
        self.durations.append(duration)
        self.long_journeys += int(fact["is_long_journey"])
        self.pickup_counts[pickup_id] += 1
        self.by_pickup_zone[pickup_id].append(duration)
        self.pickup_zone_details[pickup_id] = (str(fact["pickup_borough"]), str(fact["pickup_zone"]))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def validate_month(month: str) -> None:
    try:
        parsed = datetime.strptime(month, "%Y-%m")
    except ValueError as exc:
        raise PipelineError("--month must use YYYY-MM, for example 2024-01.") from exc
    if parsed.strftime("%Y-%m") != month:
        raise PipelineError("--month must use a zero-padded YYYY-MM value.")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temp:
        temp.write(content)
        temp_path = Path(temp.name)
    os.replace(temp_path, path)


def write_json(path: Path, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


class JsonLogger:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path

    def event(self, event: str, **attributes: Any) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {"at": utc_now(), "event": event, **attributes}
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")


@contextmanager
def monthly_run_lock(root: Path, month: str) -> Iterator[None]:
    """Stop two terminal commands from writing the same month at once."""
    lock_path = root / "data" / "locks" / f"month={month}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineError(
                f"Another run for {month} is active. Wait for it to finish before rerunning this month."
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps({"pid": os.getpid(), "acquired_at": utc_now()}) + "\n")
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def download_binary(url: str, destination: Path, logger: JsonLogger) -> dict[str, Any]:
    """Download to a temporary file first, then save it once the byte count matches."""
    request = Request(url, headers={"User-Agent": "nyc-tlc-foundations-pipeline/0.1"})
    partial = destination.with_suffix(destination.suffix + ".partial")
    try:
        with urlopen(request, timeout=120) as response, partial.open("wb") as output:
            expected_bytes = response.headers.get("Content-Length")
            content_type = response.headers.get("Content-Type")
            bytes_written = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
                bytes_written += len(chunk)
    except Exception as exc:
        partial.unlink(missing_ok=True)
        raise PipelineError(f"Download failed for {url}: {exc}") from exc
    if expected_bytes is not None and bytes_written != int(expected_bytes):
        partial.unlink(missing_ok=True)
        raise PipelineError(
            f"Incomplete download for {url}: expected {expected_bytes} bytes, received {bytes_written}."
        )
    os.replace(partial, destination)
    receipt = {
        "url": url,
        "retrieved_at": utc_now(),
        "bytes": bytes_written,
        "sha256": sha256_file(destination),
        "content_type": content_type,
    }
    logger.event("raw_downloaded", path=str(destination), **receipt)
    return receipt


def retrieve_live_sources(root: Path, month: str, force: bool, logger: JsonLogger) -> tuple[Path, Path]:
    """Get the monthly trip file and the separate Taxi Zones API response."""
    raw_dir = root / "data" / "raw" / f"month={month}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    trip_path = raw_dir / f"yellow_tripdata_{month}.parquet"
    zone_path = raw_dir / "taxi_zones.json"
    trip_url = TLC_TRIP_URL.format(month=month)
    zone_url = f"{ZONE_API_URL}?{urlencode({'$limit': 1000})}"
    receipts: dict[str, Any] = {}

    if force or not trip_path.exists():
        receipts["yellow_trip_parquet"] = download_binary(trip_url, trip_path, logger)
    else:
        receipts["yellow_trip_parquet"] = {
            "url": trip_url,
            "reused": True,
            "bytes": trip_path.stat().st_size,
            "sha256": sha256_file(trip_path),
        }
        logger.event("raw_reused", path=str(trip_path), **receipts["yellow_trip_parquet"])
    with trip_path.open("rb") as file_handle:
        starts_with = file_handle.read(4)
        file_handle.seek(-4, os.SEEK_END)
        ends_with = file_handle.read(4)
    if starts_with != b"PAR1" or ends_with != b"PAR1":
        raise PipelineError("Trip file failed the PARQUET magic-byte check.")

    if force or not zone_path.exists():
        receipts["taxi_zones_json_api"] = download_binary(zone_url, zone_path, logger)
    else:
        receipts["taxi_zones_json_api"] = {
            "url": zone_url,
            "reused": True,
            "bytes": zone_path.stat().st_size,
            "sha256": sha256_file(zone_path),
        }
        logger.event("raw_reused", path=str(zone_path), **receipts["taxi_zones_json_api"])

    try:
        zone_rows = json.loads(zone_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Taxi-zone API response is not valid JSON: {exc}") from exc
    if not isinstance(zone_rows, list) or len(zone_rows) < 260:
        raise PipelineError("Taxi-zone API completeness check failed: expected at least 260 rows.")
    receipts["taxi_zones_json_api"]["row_count"] = len(zone_rows)
    write_json(
        raw_dir / "raw_source_manifest.json",
        {"month": month, "retrieved_or_reused_at": utc_now(), "sources": receipts},
    )
    return trip_path, zone_path


def fixture_sources(root: Path, month: str) -> tuple[Path, Path]:
    if month != "2024-01":
        raise PipelineError("The committed fixture is available only for --month 2024-01.")
    fixture_dir = root / "data" / "raw" / "fixtures" / "2024-01"
    trip_path = fixture_dir / "yellow_tripdata_2024-01.csv"
    zone_path = fixture_dir / "taxi_zones.json"
    if not trip_path.exists() or not zone_path.exists():
        raise PipelineError("Fixture inputs are missing. Restore data/raw/fixtures/2024-01 from Git.")
    return trip_path, zone_path


def field(row: dict[str, Any], name: str) -> Any:
    """Read canonical TLC names and case-normalized Socrata API names efficiently."""
    if name in row:
        return row[name]
    return row.get(name.lower())


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_datetime(value: Any) -> datetime:
    text = text_or_none(value)
    if text is None:
        raise ValueError("missing")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError as exc:
        raise ValueError(f"unparseable timestamp {text!r}") from exc


def parse_int(value: Any, label: str) -> int:
    text = text_or_none(value)
    if text is None:
        raise ValueError(f"missing {label}")
    try:
        number = float(text)
    except ValueError as exc:
        raise ValueError(f"unparseable {label}") from exc
    if not number.is_integer():
        raise ValueError(f"non-integer {label}")
    return int(number)


def parse_float(value: Any, label: str) -> float:
    text = text_or_none(value)
    if text is None:
        raise ValueError(f"missing {label}")
    try:
        number = float(text)
    except ValueError as exc:
        raise ValueError(f"unparseable {label}") from exc
    if not math.isfinite(number):
        raise ValueError(f"non-finite {label}")
    return number


def load_zones(path: Path) -> dict[int, dict[str, str]]:
    try:
        raw_rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Could not load taxi zones from {path}: {exc}") from exc
    if not isinstance(raw_rows, list):
        raise PipelineError("Taxi-zone source must be a JSON array.")
    zones: dict[int, dict[str, str]] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        try:
            location_id = parse_int(field(raw, "location_id") or field(raw, "LocationID"), "location_id")
        except ValueError:
            continue
        zones[location_id] = {
            "borough": text_or_none(field(raw, "borough")) or "Unknown",
            "zone": text_or_none(field(raw, "zone")) or "Unknown",
            "service_zone": text_or_none(field(raw, "service_zone")) or "Unknown",
        }
    if not zones:
        raise PipelineError("Taxi-zone source yielded zero usable LocationIDs.")
    return zones


def read_trip_rows(path: Path) -> tuple[list[str], Iterator[dict[str, Any]]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as handle:
            columns = csv.DictReader(handle).fieldnames or []

        def csv_rows() -> Iterator[dict[str, Any]]:
            with path.open(encoding="utf-8", newline="") as handle:
                yield from csv.DictReader(handle)

        return columns, csv_rows()
    if path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq
        except ModuleNotFoundError as exc:
            raise PipelineError("Live PARQUET input requires pyarrow. Run: pip install -r requirements.txt") from exc
        parquet = pq.ParquetFile(path)
        columns = parquet.schema_arrow.names
        missing = [column for column in TRIP_COLUMNS if column not in columns]
        if missing:
            raise PipelineError(f"Trip PARQUET is missing required columns: {', '.join(missing)}")
        selected = list(TRIP_COLUMNS)

        def parquet_rows() -> Iterator[dict[str, Any]]:
        # Reading a little at a time lets the full monthly file run on a normal laptop.
            for batch in parquet.iter_batches(columns=selected, batch_size=5_000):
                yield from batch.to_pylist()

        return columns, parquet_rows()
    raise PipelineError(f"Unsupported trip input {path.name}; expected CSV or PARQUET.")


def percentile_nearest_rank(values: Iterable[float], percentile: float) -> float:
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1].")
    ordered = sorted(values)
    if not ordered:
        raise PipelineError("Cannot calculate a percentile with zero valid trips.")
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def validate_and_model(
    row: dict[str, Any], source_row_number: int, month: str, zones: dict[int, dict[str, str]]
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    pickup: datetime | None = None
    dropoff: datetime | None = None
    pickup_location: int | None = None
    dropoff_location: int | None = None
    distance: float | None = None
    for required in TRIP_COLUMNS:
        if text_or_none(field(row, required)) is None:
            errors.append(f"missing_required_{required}")
    try:
        pickup = parse_datetime(field(row, "tpep_pickup_datetime"))
    except ValueError:
        errors.append("invalid_pickup_timestamp")
    try:
        dropoff = parse_datetime(field(row, "tpep_dropoff_datetime"))
    except ValueError:
        errors.append("invalid_dropoff_timestamp")
    try:
        pickup_location = parse_int(field(row, "PULocationID"), "pickup LocationID")
    except ValueError:
        errors.append("invalid_pickup_location_id")
    try:
        dropoff_location = parse_int(field(row, "DOLocationID"), "dropoff LocationID")
    except ValueError:
        errors.append("invalid_dropoff_location_id")
    try:
        distance = parse_float(field(row, "trip_distance"), "trip distance")
        if distance < 0:
            errors.append("negative_trip_distance")
    except ValueError:
        errors.append("invalid_trip_distance")
    if pickup is not None and pickup.strftime("%Y-%m") != month:
        errors.append("pickup_outside_requested_month")
    duration: float | None = None
    if pickup is not None and dropoff is not None:
        duration = (dropoff - pickup).total_seconds() / 60
        if duration <= 0:
            errors.append("non_positive_journey_duration")
        elif duration > MAX_DURATION_MINUTES:
            errors.append("journey_duration_over_240_minutes")
    if pickup_location is not None and pickup_location not in zones:
        errors.append("pickup_location_not_in_zone_reference")
    if dropoff_location is not None and dropoff_location not in zones:
        errors.append("dropoff_location_not_in_zone_reference")
    total_amount: float | None = None
    try:
        total_amount = parse_float(field(row, "total_amount"), "total amount")
        if total_amount < 0:
            warnings.append("negative_total_amount_review")
    except ValueError:
        warnings.append("invalid_total_amount_review")
    if errors:
        return ValidationResult(fact=None, errors=sorted(set(errors)), warnings=sorted(set(warnings)))
    assert pickup is not None and dropoff is not None and pickup_location is not None
    assert dropoff_location is not None and distance is not None and duration is not None
    pickup_zone = zones[pickup_location]
    dropoff_zone = zones[dropoff_location]
    return ValidationResult(
        fact={
            "trip_event_id": f"{month}:{source_row_number:09d}",
            "source_row_number": source_row_number,
            "pickup_at": pickup.isoformat(sep=" "),
            "dropoff_at": dropoff.isoformat(sep=" "),
            "pickup_zone_id": pickup_location,
            "pickup_borough": pickup_zone["borough"],
            "pickup_zone": pickup_zone["zone"],
            "dropoff_zone_id": dropoff_location,
            "dropoff_borough": dropoff_zone["borough"],
            "dropoff_zone": dropoff_zone["zone"],
            "trip_distance_miles": round(distance, 3),
            "total_amount": "" if total_amount is None else round(total_amount, 2),
            "journey_duration_minutes": round(duration, 3),
            "is_long_journey": int(duration > LONG_JOURNEY_MINUTES),
        },
        warnings=sorted(set(warnings)),
    )


def metric_rows(accumulator: MetricAccumulator) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    total = len(accumulator.durations)
    p50 = percentile_nearest_rank(accumulator.durations, 0.50)
    p90 = percentile_nearest_rank(accumulator.durations, 0.90)
    kpi = {
        "metric_period": "pickup month",
        "valid_completed_trips": total,
        "median_duration_minutes_p50": round(p50, 2),
        "p90_duration_minutes": round(p90, 2),
        "long_journey_threshold_minutes": LONG_JOURNEY_MINUTES,
        "long_journey_rate": round(accumulator.long_journeys / total, 6),
        "largest_pickup_zone_trip_share": round(max(accumulator.pickup_counts.values()) / total, 6),
        "percentile_method": "nearest_rank",
    }
    zone_rows: list[dict[str, Any]] = []
    for zone_id, durations in accumulator.by_pickup_zone.items():
        borough, zone = accumulator.pickup_zone_details[zone_id]
        zone_rows.append(
            {
                "pickup_zone_id": zone_id,
                "pickup_borough": borough,
                "pickup_zone": zone,
                "valid_completed_trips": len(durations),
                "median_duration_minutes_p50": round(percentile_nearest_rank(durations, 0.50), 2),
                "p90_duration_minutes": round(percentile_nearest_rank(durations, 0.90), 2),
                "long_journey_rate": round(sum(value > LONG_JOURNEY_MINUTES for value in durations) / len(durations), 6),
            }
        )
    return kpi, sorted(zone_rows, key=lambda row: (-int(row["valid_completed_trips"]), int(row["pickup_zone_id"])))


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline(
    root: Path,
    month: str,
    mode: str = "fixture",
    force_retrieve: bool = False,
    max_invalid_rate: float = DEFAULT_MAX_INVALID_RATE,
) -> Path:
    """Run one month after checking that another copy is not already running."""
    validate_month(month)
    with monthly_run_lock(root, month):
        return _run_pipeline(root, month, mode, force_retrieve, max_invalid_rate)


def _run_pipeline(
    root: Path,
    month: str,
    mode: str = "fixture",
    force_retrieve: bool = False,
    max_invalid_rate: float = DEFAULT_MAX_INVALID_RATE,
) -> Path:
    """Read, check, join, calculate, and save one month of output."""
    if mode not in {"fixture", "live"}:
        raise PipelineError("--mode must be fixture or live.")
    if not 0 <= max_invalid_rate < 1:
        raise PipelineError("--max-invalid-rate must be >= 0 and < 1.")
    logger = JsonLogger(root / "data" / "logs" / "pipeline.jsonl")
    logger.event("run_started", month=month, mode=mode, pipeline_version=PIPELINE_VERSION)
    try:
        if mode == "fixture":
            trip_path, zone_path = fixture_sources(root, month)
        else:
            trip_path, zone_path = retrieve_live_sources(root, month, force_retrieve, logger)
        zones = load_zones(zone_path)
        columns, rows = read_trip_rows(trip_path)
        missing_columns = [column for column in TRIP_COLUMNS if column not in columns]
        if missing_columns:
            raise PipelineError(f"Trip input is missing required columns: {', '.join(missing_columns)}")
        input_hashes = {"trip": sha256_file(trip_path), "zones": sha256_file(zone_path)}
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "month": month,
                    "mode": mode,
                    "inputs": input_hashes,
                    "max_invalid_rate": max_invalid_rate,
                    "pipeline_version": PIPELINE_VERSION,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()[:12]
        run_id = f"{month}-{fingerprint}"
        output_dir = root / "data" / "outputs" / f"month={month}" / f"run_id={run_id}"
        if output_dir.exists():
            logger.event("run_reused", month=month, run_id=run_id, output_path=str(output_dir))
            return output_dir
        staging = root / "data" / "outputs" / f".staging-{run_id}"
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        warning_counts: Counter[str] = Counter()
        rejection_counts: Counter[str] = Counter()
        null_counts: Counter[str] = Counter()
        accumulator = MetricAccumulator()
        source_rows = accepted_rows = 0
        with (staging / "fact_trip_events.csv").open("w", encoding="utf-8", newline="") as fact_file, (
            staging / "rejected_trips.csv"
        ).open("w", encoding="utf-8", newline="") as reject_file:
            fact_writer = csv.DictWriter(fact_file, fieldnames=FACT_COLUMNS)
            reject_writer = csv.DictWriter(
                reject_file,
                fieldnames=("source_row_number", "reason_codes", "warning_codes", "raw_record"),
            )
            fact_writer.writeheader()
            reject_writer.writeheader()
            for source_rows, row in enumerate(rows, start=1):
                for column in TRIP_COLUMNS:
                    if text_or_none(field(row, column)) is None:
                        null_counts[column] += 1
                result = validate_and_model(row, source_rows, month, zones)
                warning_counts.update(result.warnings)
                if result.fact is None:
                    rejection_counts.update(result.errors)
                    reject_writer.writerow(
                        {
                            "source_row_number": source_rows,
                            "reason_codes": ";".join(result.errors),
                            "warning_codes": ";".join(result.warnings),
                            "raw_record": json.dumps(row, sort_keys=True, default=str),
                        }
                    )
                    continue
                fact_writer.writerow(result.fact)
                accumulator.add(result.fact)
                accepted_rows += 1
                if source_rows % 100_000 == 0:
                    logger.event(
                        "rows_processed",
                        month=month,
                        source_rows=source_rows,
                        accepted_rows=accepted_rows,
                        rejected_rows=source_rows - accepted_rows,
                    )
        rejected_rows = source_rows - accepted_rows
        invalid_rate = rejected_rows / source_rows if source_rows else 1.0
        profile = {
            "trip_input": str(trip_path),
            "trip_source_columns": columns,
            "zone_reference_rows": len(zones),
            "raw_rows": source_rows,
            "accepted_rows": accepted_rows,
            "rejected_rows": rejected_rows,
            "required_field_null_counts": dict(sorted(null_counts.items())),
        }
        validation_report = {
            "month": month,
            "raw_rows": source_rows,
            "accepted_rows": accepted_rows,
            "rejected_rows": rejected_rows,
            "accepted_rate": round(accepted_rows / source_rows, 6) if source_rows else 0,
            "invalid_rate": round(invalid_rate, 6),
            "max_invalid_rate": max_invalid_rate,
            "rejection_reason_counts": dict(sorted(rejection_counts.items())),
            "warning_counts": dict(sorted(warning_counts.items())),
            "rules": {
                "duration": "greater than 0 and at most 240 minutes",
                "scope": "pickup timestamp is in requested month",
                "location": "both LocationIDs must exist in the zone reference",
                "distance": "finite and non-negative",
                "fare": "negative/malformed total_amount is warned for review, not rejected",
            },
        }
        write_json(staging / "profile.json", profile)
        write_json(staging / "validation_report.json", validation_report)
        if source_rows == 0 or accepted_rows == 0 or invalid_rate > max_invalid_rate:
            failure_dir = root / "data" / "quality_failures" / f"month={month}" / f"run_id={run_id}"
            failure_dir.parent.mkdir(parents=True, exist_ok=True)
            if failure_dir.exists():
                shutil.rmtree(failure_dir)
            os.replace(staging, failure_dir)
            logger.event(
                "quality_gate_failed",
                month=month,
                run_id=run_id,
                invalid_rate=invalid_rate,
                failure_path=str(failure_dir),
            )
            raise QualityGateFailed(
                f"Quality gate failed ({invalid_rate:.2%} rejected; limit {max_invalid_rate:.2%}). "
                f"Diagnostics: {failure_dir}"
            )
        kpi, pickup_metrics = metric_rows(accumulator)
        kpi["month"] = month
        write_csv(staging / "kpi_summary.csv", kpi.keys(), [kpi])
        write_csv(
            staging / "pickup_zone_metrics.csv",
            (
                "pickup_zone_id",
                "pickup_borough",
                "pickup_zone",
                "valid_completed_trips",
                "median_duration_minutes_p50",
                "p90_duration_minutes",
                "long_journey_rate",
            ),
            pickup_metrics,
        )
        atomic_write_text(
            staging / "pipeline.log.jsonl",
            json.dumps({"at": utc_now(), "event": "run_succeeded", "month": month, "run_id": run_id, "accepted_rows": accepted_rows, "rejected_rows": rejected_rows}, sort_keys=True) + "\n",
        )
        write_json(
            staging / "run_manifest.json",
            {"run_id": run_id, "status": "succeeded", "pipeline_version": PIPELINE_VERSION, "month": month, "mode": mode, "input_sha256": input_hashes, "max_invalid_rate": max_invalid_rate, "created_at": utc_now()},
        )
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, output_dir)
        logger.event("run_succeeded", month=month, run_id=run_id, output_path=str(output_dir))
        return output_dir
    except PipelineError:
        raise
    except Exception as exc:
        logger.event("run_failed_unexpected", month=month, error=str(exc))
        raise PipelineError(f"Unexpected pipeline failure: {exc}") from exc
