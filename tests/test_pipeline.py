from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_tlc_pipeline.pipeline import (
    MetricAccumulator,
    QualityGateFailed,
    metric_rows,
    monthly_run_lock,
    run_pipeline,
    validate_and_model,
)


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        source = PROJECT_ROOT / "data" / "raw" / "fixtures"
        destination = self.temp_dir / "data" / "raw" / "fixtures"
        destination.parent.mkdir(parents=True)
        shutil.copytree(source, destination)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_fixture_publish_and_idempotent_rerun(self) -> None:
        first = run_pipeline(self.temp_dir, "2024-01", mode="fixture")
        second = run_pipeline(self.temp_dir, "2024-01", mode="fixture")
        self.assertEqual(first, second)
        report = json.loads((first / "validation_report.json").read_text())
        self.assertEqual((report["raw_rows"], report["accepted_rows"], report["rejected_rows"]), (20, 18, 2))
        self.assertIn("journey_duration_over_240_minutes", report["rejection_reason_counts"])
        self.assertIn("pickup_location_not_in_zone_reference", report["rejection_reason_counts"])
        with (first / "kpi_summary.csv").open(newline="") as handle:
            self.assertEqual(next(csv.DictReader(handle))["valid_completed_trips"], "18")

    def test_invalid_zone_is_rejected_without_mutation(self) -> None:
        zones = {1: {"borough": "Manhattan", "zone": "Test", "service_zone": "Yellow Zone"}}
        result = validate_and_model(
            {"tpep_pickup_datetime": "2024-01-01 10:00:00", "tpep_dropoff_datetime": "2024-01-01 10:10:00", "PULocationID": "999", "DOLocationID": "1", "trip_distance": "2.0", "total_amount": "10.0"},
            source_row_number=1, month="2024-01", zones=zones,
        )
        self.assertIsNone(result.fact)
        self.assertIn("pickup_location_not_in_zone_reference", result.errors)

    def test_nearest_rank_metrics_are_stable(self) -> None:
        accumulator = MetricAccumulator()
        for duration in (10, 20, 30, 40, 50):
            accumulator.add(
                {
                    "pickup_zone_id": 1,
                    "pickup_borough": "Manhattan",
                    "pickup_zone": "Test Zone",
                    "journey_duration_minutes": duration,
                    "is_long_journey": int(duration > 45),
                }
            )
        kpi, zones = metric_rows(accumulator)
        self.assertEqual(kpi["median_duration_minutes_p50"], 30.0)
        self.assertEqual(kpi["p90_duration_minutes"], 50.0)
        self.assertEqual(kpi["long_journey_rate"], 0.2)
        self.assertEqual(zones[0]["valid_completed_trips"], 5)
        self.assertEqual(zones[0]["pickup_zone"], "Test Zone")

    def test_quality_gate_does_not_publish_metrics(self) -> None:
        with self.assertRaises(QualityGateFailed):
            run_pipeline(self.temp_dir, "2024-01", mode="fixture", max_invalid_rate=0.01)
        published = self.temp_dir / "data" / "outputs" / "month=2024-01"
        self.assertFalse(published.exists())
        failures = list((self.temp_dir / "data" / "quality_failures" / "month=2024-01").glob("run_id=*"))
        self.assertEqual(len(failures), 1)
        self.assertTrue((failures[0] / "validation_report.json").exists())

    def test_month_lock_rejects_a_parallel_process(self) -> None:
        child = (
            "from pathlib import Path\n"
            "from nyc_tlc_pipeline.pipeline import PipelineError, run_pipeline\n"
            f"root = Path({str(self.temp_dir)!r})\n"
            "try:\n"
            "    run_pipeline(root, '2024-01', mode='fixture')\n"
            "except PipelineError as error:\n"
            "    raise SystemExit(0 if 'active' in str(error) else 2)\n"
            "raise SystemExit(1)\n"
        )
        environment = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}
        with monthly_run_lock(self.temp_dir, "2024-01"):
            result = subprocess.run([sys.executable, "-c", child], env=environment, check=False)
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
