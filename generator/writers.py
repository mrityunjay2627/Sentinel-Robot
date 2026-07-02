"""Output writers — expanded for 30+ sensor fields + messy raw output.

Two landing paths:
  * Flat tables  -> Parquet (clean, typed) + CSV (messy raw for cleaning work).
  * sensor_readings -> nested Parquet (real STRUCT/ARRAY types) for lake/Spark,
    AND a flattened messy CSV for the Postgres JSONB / staging cleanup path.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import GeneratorConfig
from .schema import SensorReading
from .messify import messify_readings_rows, messify_flat_rows


def _enum_safe(v):
    return v.value if hasattr(v, "value") else v


def _flatten_reading(r: SensorReading) -> dict:
    """Flatten a nested SensorReading into a wide dict with prefixed column names."""
    d = {
        "reading_id": r.reading_id,
        "robot_id": r.robot_id,
        "reading_ts": r.reading_ts.isoformat() if isinstance(r.reading_ts, datetime) else str(r.reading_ts),
        # Battery
        "voltage_v": r.battery.voltage_v,
        "soc_pct": r.battery.soc_pct,
        "battery_temp_c": r.battery.temp_c,
        "charge_cycles": r.battery.charge_cycles,
        "battery_current_amps": r.battery.current_amps,
        "battery_health_pct": r.battery.health_pct,
        # Motor
        "motor_temp_c": r.motor.temp_c,
        "rpm": r.motor.rpm,
        "load_pct": r.motor.load_pct,
        "motor_current_amps": r.motor.current_amps,
        "vibration_g": r.motor.vibration_g,
        # Navigation
        "x_m": r.navigation.x_m,
        "y_m": r.navigation.y_m,
        "heading_deg": r.navigation.heading_deg,
        "speed_mps": r.navigation.speed_mps,
        "zone_id": r.navigation.zone_id,
        "lidar_front_m": r.navigation.lidar_front_m,
        "lidar_rear_m": r.navigation.lidar_rear_m,
        "lidar_left_m": r.navigation.lidar_left_m,
        "lidar_right_m": r.navigation.lidar_right_m,
        "obstacle_detected": r.navigation.obstacle_detected,
        # Environment
        "ambient_temp_c": r.environment.ambient_temp_c,
        "humidity_pct": r.environment.humidity_pct,
        "light_level_lux": r.environment.light_level_lux,
        "floor_condition": r.environment.floor_condition,
        # Connectivity
        "wifi_signal_dbm": r.connectivity.wifi_signal_dbm,
        "latency_ms": r.connectivity.latency_ms,
        "packet_loss_pct": r.connectivity.packet_loss_pct,
        "access_point_id": r.connectivity.access_point_id,
        # Wheels
        "left_rpm": r.wheels.left_rpm,
        "right_rpm": r.wheels.right_rpm,
        "odometer_m": r.wheels.odometer_m,
        # Top-level
        "error_codes": "|".join(r.error_codes) if r.error_codes else "",
        "power_draw_watts": r.power_draw_watts,
        "operational_status": r.operational_status,
    }
    return d


def _flat_frame(records: list) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.append({k: _enum_safe(v) for k, v in asdict(r).items()})
    return pd.DataFrame(rows)


# ── Nested Parquet schema (clean, typed — for Spark/lake path) ──────────────
_READING_SCHEMA = pa.schema([
    ("reading_id", pa.string()),
    ("robot_id", pa.string()),
    ("reading_ts", pa.timestamp("us", tz="UTC")),
    ("battery", pa.struct([
        ("voltage_v", pa.float64()), ("soc_pct", pa.float64()),
        ("temp_c", pa.float64()), ("charge_cycles", pa.int64()),
        ("current_amps", pa.float64()), ("health_pct", pa.float64()),
    ])),
    ("motor", pa.struct([
        ("temp_c", pa.float64()), ("rpm", pa.float64()),
        ("load_pct", pa.float64()), ("current_amps", pa.float64()),
        ("vibration_g", pa.float64()),
    ])),
    ("navigation", pa.struct([
        ("x_m", pa.float64()), ("y_m", pa.float64()),
        ("heading_deg", pa.float64()), ("speed_mps", pa.float64()),
        ("zone_id", pa.string()),
        ("lidar_front_m", pa.float64()), ("lidar_rear_m", pa.float64()),
        ("lidar_left_m", pa.float64()), ("lidar_right_m", pa.float64()),
        ("obstacle_detected", pa.bool_()),
    ])),
    ("environment", pa.struct([
        ("ambient_temp_c", pa.float64()), ("humidity_pct", pa.float64()),
        ("light_level_lux", pa.float64()), ("floor_condition", pa.string()),
    ])),
    ("connectivity", pa.struct([
        ("wifi_signal_dbm", pa.float64()), ("latency_ms", pa.float64()),
        ("packet_loss_pct", pa.float64()), ("access_point_id", pa.string()),
    ])),
    ("wheels", pa.struct([
        ("left_rpm", pa.float64()), ("right_rpm", pa.float64()),
        ("odometer_m", pa.float64()),
    ])),
    ("error_codes", pa.list_(pa.string())),
    ("power_draw_watts", pa.float64()),
    ("operational_status", pa.string()),
])


def _readings_nested_table(readings: list[SensorReading]) -> pa.Table:
    cols = {
        "reading_id": [r.reading_id for r in readings],
        "robot_id": [r.robot_id for r in readings],
        "reading_ts": [r.reading_ts for r in readings],
        "battery": [asdict(r.battery) for r in readings],
        "motor": [asdict(r.motor) for r in readings],
        "navigation": [asdict(r.navigation) for r in readings],
        "environment": [asdict(r.environment) for r in readings],
        "connectivity": [asdict(r.connectivity) for r in readings],
        "wheels": [asdict(r.wheels) for r in readings],
        "error_codes": [r.error_codes for r in readings],
        "power_draw_watts": [r.power_draw_watts for r in readings],
        "operational_status": [r.operational_status for r in readings],
    }
    return pa.Table.from_pydict(cols, schema=_READING_SCHEMA)


def write_all(data: dict[str, list], out_dir: str, cfg: GeneratorConfig | None = None) -> dict[str, list[str]]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, list[str]] = {}
    messy = cfg is not None and cfg.messy_output
    mess_rng = np.random.default_rng((cfg.seed if cfg else 42) + 777) if messy else None

    for name, records in data.items():
        paths = []
        if name == "sensor_readings":
            # 1) Clean nested Parquet (for Spark/lake — always clean)
            p_parquet = out / "sensor_readings.parquet"
            pq.write_table(_readings_nested_table(records), p_parquet)
            paths.append(str(p_parquet))

            # 2) Flattened CSV (with mess if enabled — this is the raw landing)
            flat_rows = [_flatten_reading(r) for r in records]
            if messy and mess_rng is not None:
                flat_rows = messify_readings_rows(flat_rows, mess_rng, cfg.mess_rate)
            p_csv = out / "sensor_readings_raw.csv"
            pd.DataFrame(flat_rows).to_csv(p_csv, index=False)
            paths.append(str(p_csv))
        else:
            df = _flat_frame(records)
            # Clean Parquet (always)
            p_parquet = out / f"{name}.parquet"
            df.to_parquet(p_parquet, index=False)
            paths.append(str(p_parquet))
            # Messy CSV
            if messy and mess_rng is not None:
                rows = df.to_dict("records")
                rows = messify_flat_rows(rows, mess_rng, cfg.mess_rate, name)
                p_csv = out / f"{name}_raw.csv"
                pd.DataFrame(rows).to_csv(p_csv, index=False)
            else:
                p_csv = out / f"{name}.csv"
                df.to_csv(p_csv, index=False)
            paths.append(str(p_csv))
        written[name] = paths
    return written
