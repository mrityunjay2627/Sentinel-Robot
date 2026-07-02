"""Generator configuration.

A single, immutable, *typed* config object is the first piece of data-prep
discipline: every run is fully described by this object, so any output is
reproducible from (code version + config + seed) alone.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class GeneratorConfig:
    # --- reproducibility ---
    seed: int = 42

    # --- scale (bigger defaults for realistic preprocessing work) ---
    n_robots: int = 50
    n_models: int = 6
    n_zones: int = 10
    n_days: int = 90
    readings_per_robot_per_day: int = 96  # one reading every 15 min

    # --- time window (timezone-aware UTC) ---
    start: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # --- raw output messiness (ON by default — this IS the data cleaning work) ---
    messy_output: bool = True
    mess_rate: float = 0.08        # fraction of rows with at least one messy field

    # --- chaos injection (OFF for clean modeling; ON for reliability testing) ---
    chaos: bool = False
    chaos_null_rate: float = 0.0
    chaos_dup_rate: float = 0.0
    chaos_late_rate: float = 0.0

    # --- output ---
    out_dir: str = "data/raw"

    @property
    def steps_per_robot(self) -> int:
        return self.n_days * self.readings_per_robot_per_day

    @property
    def step_seconds(self) -> float:
        return 86_400 / self.readings_per_robot_per_day
