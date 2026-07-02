"""CLI entry point.

Pipeline: configure -> generate -> VALIDATE (gate on clean data) -> [chaos] -> write (with mess).

Usage:
    python -m generator.cli                                    # 50 robots, 90 days, messy CSV
    python -m generator.cli --robots 200 --days 365            # ~7M readings
    python -m generator.cli --no-mess                          # clean output (for modeling)
    python -m generator.cli --chaos                            # dupes + late rows + nulled scalars
    python -m generator.cli --robots 10 --days 7 --out data/dev  # quick dev run
"""
from __future__ import annotations

import argparse
import sys

from .chaos import inject_chaos, write_manifest
from .config import GeneratorConfig
from .simulate import generate_all
from .validate import ValidationError, validate
from .writers import write_all


def _parse() -> GeneratorConfig:
    p = argparse.ArgumentParser(description="AMR fleet telemetry generator")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--robots", type=int, default=50)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--readings-per-day", type=int, default=96)
    p.add_argument("--out", type=str, default="data/raw")
    p.add_argument("--no-mess", action="store_true", help="Disable messy raw output")
    p.add_argument("--mess-rate", type=float, default=0.08, help="Fraction of rows to dirty")
    p.add_argument("--chaos", action="store_true", help="Inject duplicates/late/nulls (known counts)")
    a = p.parse_args()
    return GeneratorConfig(
        seed=a.seed, n_robots=a.robots, n_days=a.days,
        readings_per_robot_per_day=a.readings_per_day, out_dir=a.out,
        messy_output=not a.no_mess, mess_rate=a.mess_rate, chaos=a.chaos,
    )


def run(cfg: GeneratorConfig) -> dict:
    total_readings = cfg.n_robots * cfg.steps_per_robot
    steps = 5 if cfg.chaos else 4

    print(f"[1/{steps}] generating  (seed={cfg.seed}, robots={cfg.n_robots}, "
          f"days={cfg.n_days}, readings/robot/day={cfg.readings_per_robot_per_day}, "
          f"expected readings={total_readings:,})")
    data = generate_all(cfg)

    print(f"[2/{steps}] validating  (quality gate on CLEAN internal data)")
    try:
        report = validate(data)
    except ValidationError as e:
        print(f"\n{e}\n", file=sys.stderr)
        raise SystemExit(1)

    if cfg.chaos:
        print(f"[3/{steps}] injecting chaos")
        manifest = inject_chaos(data, cfg)
        write_manifest(manifest, cfg.out_dir)
        report["chaos_manifest"] = manifest
        for k, v in manifest.get("injected", {}).items():
            print(f"         {k}: {v}")

    mess_label = f" (messy_output={cfg.messy_output}, mess_rate={cfg.mess_rate})" if cfg.messy_output else " (clean)"
    print(f"[{steps-1}/{steps}] writing     -> {cfg.out_dir}{mess_label}")
    written = write_all(data, cfg.out_dir, cfg)

    print(f"[{steps}/{steps}] done\n")
    _print_report(report, written)
    return report


def _print_report(report: dict, written: dict) -> None:
    print("row counts")
    for t, n in report["row_counts"].items():
        print(f"  {t:<28} {n:>10,}")

    failed = [c for c in report["checks"] if c[1] == "FAIL"]
    print(f"\nquality checks: {len(report['checks'])} run, "
          f"{len(report['checks']) - len(failed)} passed, {len(failed)} failed  "
          f"[{report['status']}]")

    print("\nobservations")
    for k, v in report["observations"].items():
        print(f"  {k:<38} {v}")

    print("\nfiles written")
    for table, paths in written.items():
        for p in paths:
            print(f"  {p}")


if __name__ == "__main__":
    run(_parse())
