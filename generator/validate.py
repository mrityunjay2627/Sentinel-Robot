"""Data-quality validation — the gate every generated batch must pass.

Checks here mirror what dbt tests assert downstream: PK uniqueness, referential
integrity, value ranges, ARRAY contracts, timestamp ordering. Hard failures
abort the run (bad data never lands). Soft observations are reported.
"""
from __future__ import annotations
from dataclasses import asdict
from datetime import datetime

from .schema import FOREIGN_KEYS, PRIMARY_KEYS


class ValidationError(Exception):
    pass


def _rows(records: list) -> list[dict]:
    return [asdict(r) for r in records]


def validate(data: dict[str, list]) -> dict:
    errors: list[str] = []
    report: dict[str, object] = {"row_counts": {}, "checks": [], "observations": {}}

    tables = {name: _rows(recs) for name, recs in data.items()}
    for name, rows in tables.items():
        report["row_counts"][name] = len(rows)

    def check(ok: bool, label: str):
        report["checks"].append((label, "PASS" if ok else "FAIL"))
        if not ok:
            errors.append(label)

    # 1) Primary-key uniqueness + non-null
    for table, pk in PRIMARY_KEYS.items():
        rows = tables[table]
        keys = [r[pk] for r in rows]
        check(all(k is not None for k in keys), f"{table}.{pk} not null (PK)")
        check(len(keys) == len(set(keys)), f"{table}.{pk} unique (PK)")

    # 2) Referential integrity
    for (child, col), (parent, pcol) in FOREIGN_KEYS.items():
        parent_keys = {r[pcol] for r in tables[parent]}
        orphans = [r[col] for r in tables[child] if r[col] not in parent_keys]
        check(not orphans, f"{child}.{col} -> {parent}.{pcol} (FK, {len(orphans)} orphans)")

    # 3) Sensor reading ranges
    rds = tables["sensor_readings"]
    bad_soc = sum(not (0 <= r["battery"]["soc_pct"] <= 100) for r in rds)
    bad_load = sum(not (0 <= r["motor"]["load_pct"] <= 100) for r in rds)
    bad_rpm = sum(r["motor"]["rpm"] < 0 for r in rds)
    bad_cyc = sum(r["battery"]["charge_cycles"] < 0 for r in rds)
    bad_health = sum(not (0 <= r["battery"]["health_pct"] <= 100) for r in rds)
    bad_heading = sum(not (0 <= r["navigation"]["heading_deg"] < 360) for r in rds)
    bad_humidity = sum(not (0 <= r["environment"]["humidity_pct"] <= 100) for r in rds)
    bad_speed = sum(r["navigation"]["speed_mps"] < 0 for r in rds)
    bad_odo = sum(r["wheels"]["odometer_m"] < 0 for r in rds)
    bad_power = sum(r["power_draw_watts"] < 0 for r in rds)

    check(bad_soc == 0, f"battery.soc_pct in [0,100] ({bad_soc} bad)")
    check(bad_load == 0, f"motor.load_pct in [0,100] ({bad_load} bad)")
    check(bad_rpm == 0, f"motor.rpm >= 0 ({bad_rpm} bad)")
    check(bad_cyc == 0, f"battery.charge_cycles >= 0 ({bad_cyc} bad)")
    check(bad_health == 0, f"battery.health_pct in [0,100] ({bad_health} bad)")
    check(bad_heading == 0, f"navigation.heading_deg in [0,360) ({bad_heading} bad)")
    check(bad_humidity == 0, f"environment.humidity_pct in [0,100] ({bad_humidity} bad)")
    check(bad_speed == 0, f"navigation.speed_mps >= 0 ({bad_speed} bad)")
    check(bad_odo == 0, f"wheels.odometer_m >= 0 ({bad_odo} bad)")
    check(bad_power == 0, f"power_draw_watts >= 0 ({bad_power} bad)")

    # 4) error_codes is always a list
    bad_arr = sum(not isinstance(r["error_codes"], list) for r in rds)
    check(bad_arr == 0, f"sensor_readings.error_codes is ARRAY ({bad_arr} bad)")

    # 5) Task timestamp ordering
    def ordered(*ts):
        present = [t for t in ts if t is not None]
        return all(a <= b for a, b in zip(present, present[1:]))

    bad_task = sum(
        not ordered(t["assigned_ts"], t["started_ts"], t["completed_ts"])
        for t in tables["tasks"]
    )
    check(bad_task == 0, f"tasks timestamps ordered ({bad_task} bad)")

    # 6) Maintenance lifecycle: ordered, trailing nulls only
    def trailing_null_ok(seq: list) -> bool:
        seen_null = False
        for v in seq:
            if v is None:
                seen_null = True
            elif seen_null:
                return False
        present = [v for v in seq if v is not None]
        return all(a <= b for a, b in zip(present, present[1:]))

    bad_tk = 0
    for t in tables["maintenance_tickets"]:
        seq = [t["reported_ts"], t["diagnosed_ts"], t["repaired_ts"], t["closed_ts"]]
        if not trailing_null_ok(seq):
            bad_tk += 1
    check(bad_tk == 0, f"maintenance lifecycle ordered w/ trailing nulls ({bad_tk} bad)")
    check(all(t["reported_ts"] is not None for t in tables["maintenance_tickets"]),
          "maintenance_tickets.reported_ts not null")

    # 6b) Attribute changes must actually change something
    noop = sum(c["old_value"] == c["new_value"] for c in tables["robot_attribute_changes"])
    check(noop == 0, f"attribute changes old != new ({noop} no-ops)")

    # 7) Reading timestamps are datetime
    if rds:
        ts_all = [r["reading_ts"] for r in rds]
        check(all(isinstance(t, datetime) for t in ts_all), "reading_ts is datetime")

    # 8) Operational status is valid
    valid_statuses = {"ACTIVE", "IDLE", "CHARGING", "MAINTENANCE"}
    bad_status = sum(r["operational_status"] not in valid_statuses for r in rds)
    check(bad_status == 0, f"operational_status valid ({bad_status} bad)")

    # ── soft observations ──
    n_rd = len(rds) or 1
    report["observations"]["pct_readings_with_error_codes"] = round(
        100 * sum(bool(r["error_codes"]) for r in rds) / n_rd, 2)
    report["observations"]["maintenance_tickets"] = len(tables["maintenance_tickets"])
    report["observations"]["robots_with_a_failure"] = len(
        {t["robot_id"] for t in tables["maintenance_tickets"]})
    open_tickets = sum(t["closed_ts"] is None for t in tables["maintenance_tickets"])
    report["observations"]["open_tickets_at_window_end"] = open_tickets
    report["observations"]["total_tasks"] = len(tables["tasks"])
    report["observations"]["total_sensor_readings"] = len(rds)

    report["status"] = "PASS" if not errors else "FAIL"
    if errors:
        raise ValidationError(
            "Data quality gate FAILED:\n  - " + "\n  - ".join(errors))
    return report
