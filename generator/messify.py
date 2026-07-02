"""Raw output messifier — makes clean data look like real sensor dumps.

This module injects realistic data quality issues into the WRITTEN output
(CSV/JSON), not the internal data. The validation gate runs on clean data;
the mess happens at serialization time so the dbt staging layer has genuine
cleaning work to do.

Mess types (each applied independently per row at `mess_rate` probability):
  1. Mixed null representations: "null", "N/A", "", "nan", "None", "-"
  2. Inconsistent casing: "PICK" -> "pick", "Pick", "pICK"
  3. Whitespace padding: leading/trailing spaces in strings
  4. Unit confusion: Celsius values written as Fahrenheit (value * 1.8 + 32)
  5. Sensor spike outliers: 5-20x normal values
  6. Malformed timestamps: mixed formats ("2024-01-15", "01/15/2024", epoch)
  7. Trailing whitespace / invisible chars in IDs
  8. Extra/junk columns: random columns that don't belong
  9. Inconsistent boolean representations: "true"/"True"/"1"/"yes"
  10. Negative values where only positive makes sense
"""
from __future__ import annotations

import numpy as np

# The null tokens real-world sensor systems actually produce
NULL_TOKENS = ["null", "N/A", "", "nan", "None", "-", "NA", "n/a", "NaN", "#N/A"]


def _mess_null(rng):
    """Return a random null-like string."""
    return NULL_TOKENS[int(rng.integers(0, len(NULL_TOKENS)))]


def _mess_case(rng, val: str) -> str:
    """Randomly mangle the casing of a string."""
    roll = rng.random()
    if roll < 0.3:
        return val.lower()
    elif roll < 0.5:
        return val.title()
    elif roll < 0.7:
        return val.upper()
    else:
        # random per-char
        return "".join(c.upper() if rng.random() > 0.5 else c.lower() for c in val)


def _mess_whitespace(rng, val: str) -> str:
    """Add random leading/trailing whitespace."""
    left = " " * int(rng.integers(0, 4))
    right = " " * int(rng.integers(0, 4))
    return left + val + right


def _mess_timestamp(rng, ts_str: str) -> str:
    """Convert ISO timestamp to a random alternative format."""
    # ts_str looks like "2024-01-15T08:30:00+00:00" or "2024-01-15 08:30:00"
    try:
        from datetime import datetime, timezone
        # parse ISO
        clean = ts_str.replace("+00:00", "").replace("T", " ").strip()
        dt = datetime.strptime(clean[:19], "%Y-%m-%d %H:%M:%S")
        roll = rng.random()
        if roll < 0.25:
            return dt.strftime("%m/%d/%Y %H:%M:%S")  # US format
        elif roll < 0.4:
            return dt.strftime("%d-%m-%Y %H:%M")      # EU format, no seconds
        elif roll < 0.55:
            return str(int(dt.replace(tzinfo=timezone.utc).timestamp()))  # epoch
        elif roll < 0.7:
            return dt.strftime("%Y/%m/%d %I:%M:%S %p")  # 12-hour
        else:
            return clean  # drop timezone info
    except Exception:
        return ts_str


def messify_readings_rows(rows: list[dict], rng: np.random.Generator, mess_rate: float) -> list[dict]:
    """Apply realistic mess to a list of flattened sensor reading dicts.

    Each row is independently dirtied with probability `mess_rate`.
    Multiple mess types can stack on the same row.
    """
    # Add junk columns to ~3% of rows
    junk_cols = ["_unnamed_0", "Unnamed: 47", "debug_flag", "temp_backup", "raw_signal_x"]

    for row in rows:
        if rng.random() > mess_rate:
            continue

        # Pick 1-3 mess types per dirty row
        n_mess = int(rng.integers(1, 4))
        mess_types = rng.choice(10, size=n_mess, replace=False)

        for mt in mess_types:
            mt = int(mt)

            if mt == 0:  # null token in a numeric field
                field = _pick_numeric_field(rng)
                if field in row:
                    row[field] = _mess_null(rng)

            elif mt == 1:  # casing on categorical
                for f in ["operational_status", "floor_condition"]:
                    if f in row and isinstance(row[f], str):
                        row[f] = _mess_case(rng, row[f])

            elif mt == 2:  # whitespace in IDs
                for f in ["robot_id", "reading_id", "zone_id", "access_point_id"]:
                    if f in row and isinstance(row[f], str):
                        row[f] = _mess_whitespace(rng, row[f])

            elif mt == 3:  # unit confusion: C -> F on temp fields
                for f in ["battery_temp_c", "motor_temp_c", "ambient_temp_c"]:
                    if f in row and isinstance(row[f], (int, float)):
                        row[f] = round(row[f] * 1.8 + 32, 2)  # now it's Fahrenheit
                        # no label change — the column still says _c

            elif mt == 4:  # sensor spike
                field = _pick_numeric_field(rng)
                if field in row and isinstance(row[field], (int, float)):
                    row[field] = round(row[field] * float(rng.uniform(5, 20)), 2)

            elif mt == 5:  # malformed timestamp
                if "reading_ts" in row and isinstance(row["reading_ts"], str):
                    row["reading_ts"] = _mess_timestamp(rng, row["reading_ts"])

            elif mt == 6:  # negative where positive expected
                for f in ["soc_pct", "humidity_pct", "light_level_lux", "odometer_m"]:
                    if f in row and isinstance(row[f], (int, float)):
                        row[f] = -abs(row[f])
                        break

            elif mt == 7:  # junk column
                col = junk_cols[int(rng.integers(0, len(junk_cols)))]
                row[col] = _mess_null(rng) if rng.random() < 0.5 else float(rng.uniform(-99, 99))

            elif mt == 8:  # boolean mess
                if "obstacle_detected" in row:
                    val = row["obstacle_detected"]
                    variants = ["true", "True", "TRUE", "1", "yes", "Yes", "Y",
                                "false", "False", "FALSE", "0", "no", "No", "N"]
                    if val in (True, "True", "true", "1"):
                        row["obstacle_detected"] = rng.choice(["true", "True", "1", "yes", "Y"])
                    else:
                        row["obstacle_detected"] = rng.choice(["false", "False", "0", "no", "N"])

            elif mt == 9:  # duplicate column with slight name variation
                if "motor_temp_c" in row:
                    variant = rng.choice(["motor_temp", "motorTemp_c", "motor_temperature_c"])
                    row[str(variant)] = row["motor_temp_c"]

    return rows


def messify_flat_rows(rows: list[dict], rng: np.random.Generator, mess_rate: float, table_name: str) -> list[dict]:
    """Apply lighter mess to dimension/fact tables (whitespace, casing, nulls)."""
    for row in rows:
        if rng.random() > mess_rate * 0.3:  # less frequent on dim tables
            continue

        # whitespace in string fields
        for k, v in list(row.items()):
            if isinstance(v, str) and rng.random() < 0.3:
                row[k] = _mess_whitespace(rng, v)
            # occasional null token
            if v is not None and rng.random() < 0.05:
                row[k] = _mess_null(rng)

    return rows


def _pick_numeric_field(rng) -> str:
    """Pick a random numeric sensor field name."""
    fields = [
        "voltage_v", "soc_pct", "battery_temp_c", "current_amps", "health_pct",
        "motor_temp_c", "rpm", "load_pct", "motor_current_amps", "vibration_g",
        "heading_deg", "speed_mps", "lidar_front_m", "lidar_rear_m",
        "lidar_left_m", "lidar_right_m", "ambient_temp_c", "humidity_pct",
        "light_level_lux", "wifi_signal_dbm", "latency_ms", "packet_loss_pct",
        "left_rpm", "right_rpm", "odometer_m", "power_draw_watts",
    ]
    return fields[int(rng.integers(0, len(fields)))]
