"""Chaos injection — runs AFTER validation on clean data.
Injects known quantities of duplicates, nulled scalars, and late rows.
"""
from __future__ import annotations
import json
from datetime import timedelta
from pathlib import Path
import numpy as np
from .config import GeneratorConfig

def inject_chaos(data, cfg):
    if not cfg.chaos:
        return {"chaos": False, "injected": {}}
    rng = np.random.default_rng(cfg.seed + 999)
    readings = data["sensor_readings"]
    n = len(readings)
    manifest = {}

    dup_rate = cfg.chaos_dup_rate if cfg.chaos_dup_rate > 0 else 0.02
    n_dups = max(1, int(n * dup_rate))
    for i in rng.choice(n, size=n_dups, replace=True):
        readings.append(readings[int(i)])
    manifest["duplicates_injected"] = n_dups

    null_rate = cfg.chaos_null_rate if cfg.chaos_null_rate > 0 else 0.01
    n_nulls = max(1, int(n * null_rate))
    for i in rng.choice(n, size=n_nulls, replace=False):
        r = readings[int(i)]
        field = int(rng.integers(0, 3))
        if field == 0: r.battery.voltage_v = None
        elif field == 1: r.motor.temp_c = None
        else: r.motor.rpm = None
    manifest["nulled_scalars_injected"] = n_nulls

    late_rate = cfg.chaos_late_rate if cfg.chaos_late_rate > 0 else 0.005
    n_late = max(1, int(n * late_rate))
    for i in rng.choice(n, size=n_late, replace=False):
        r = readings[int(i)]
        r.reading_ts = r.reading_ts - timedelta(hours=float(rng.uniform(1, 48)))
    manifest["late_rows_injected"] = n_late

    rng.shuffle(readings)
    return {"chaos": True, "injected": manifest}

def write_manifest(manifest, out_dir):
    p = Path(out_dir) / "chaos_manifest.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2))
    return str(p)
