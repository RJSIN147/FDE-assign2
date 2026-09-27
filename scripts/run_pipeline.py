#!/usr/bin/env python3
"""Run the NYC TLC monthly journey-reliability pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_tlc_pipeline.pipeline import DEFAULT_MAX_INVALID_RATE, PipelineError, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="Month to process in YYYY-MM form.")
    parser.add_argument(
        "--mode",
        choices=("fixture", "live"),
        default="fixture",
        help="fixture is offline and deterministic; live downloads official raw sources.",
    )
    parser.add_argument(
        "--force-retrieve",
        action="store_true",
        help="Refresh existing live raw inputs intentionally instead of reusing them.",
    )
    parser.add_argument(
        "--max-invalid-rate",
        type=float,
        default=DEFAULT_MAX_INVALID_RATE,
        help=f"Fail before KPI publication above this rejected-row rate (default {DEFAULT_MAX_INVALID_RATE}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        output = run_pipeline(
            root=PROJECT_ROOT,
            month=args.month,
            mode=args.mode,
            force_retrieve=args.force_retrieve,
            max_invalid_rate=args.max_invalid_rate,
        )
    except PipelineError as exc:
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        return 2
    print(f"PIPELINE SUCCEEDED: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
