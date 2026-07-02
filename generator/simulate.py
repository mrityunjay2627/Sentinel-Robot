"""Simulation engine — expanded for 30+ sensor fields per reading.

Determinism: a single numpy Generator (seeded from config) drives ALL randomness.
Same seed + same config => byte-identical output.

Realism: each robot carries a latent health that degrades with use. As health
drops, motor temperature, vibration, and power draw rise *before* a failure
is logged — a genuine leading indicator for predictive maintenance.
"""
from __future__ import annotations
from datetime import timedelta
import numpy as np

from .config import GeneratorConfig
from .schema import (
    ERROR_CODE_POOL, FIRMWARE_VERSIONS, AttributeChange, Battery, Motor,
    Navigation, Environment, Connectivity, WheelEncoder,
    FailureType, MaintenanceTicket, Robot, RobotModel,
    SensorReading, Severity, Task, TaskStatus, TaskType, Zone, ZoneType,
)

FAILURE_THRESHOLD = 0.55
_TEAMS = ["fulfilment-a", "fulfilment-b", "inbound", "returns", "maintenance", "qc"]
_MANUFACTURERS = ["Robotiq", "Kuka", "FetchCore", "Locus", "MiR", "OTTO"]
_SITES = ["DFW-01", "PHX-02", "ORD-03"]
_FLOOR_CONDITIONS = ["DRY", "WET", "DUSTY", "DEBRIS"]
_SENSOR_SUITES = ["BASIC", "STANDARD", "ADVANCED"]
_AP_IDS = [f"AP-{i:03d}" for i in range(1, 21)]
_ROOT_CAUSES = [
    "bearing_wear", "firmware_bug", "thermal_runaway", "calibration_drift",
    "connector_corrosion", "memory_leak", "battery_cell_imbalance",
    "encoder_fault", "lidar_occlusion", "wifi_interference", None,
]


class _Ids:
    def __init__(self):
        self.r = self.t = self.k = 0

    def reading(self):
        self.r += 1
        return f"RDG-{self.r:010d}"

    def task(self):
        self.t += 1
        return f"TSK-{self.t:09d}"

    def ticket(self):
        self.k += 1
        return f"TKT-{self.k:07d}"


def _choice(rng, items):
    return items[int(rng.integers(0, len(items)))]


def _mac(rng):
    return ":".join(f"{int(rng.integers(0, 256)):02X}" for _ in range(6))


def _ip(rng):
    return f"10.{int(rng.integers(0,256))}.{int(rng.integers(0,256))}.{int(rng.integers(1,255))}"


# --------------------------------------------------------------------------- #
# Dimensions
# --------------------------------------------------------------------------- #
def generate_models(rng, cfg):
    names = ["Scout", "Hauler", "Ranger", "Titan", "Nimble", "Atlas"]
    out = []
    for i in range(cfg.n_models):
        out.append(RobotModel(
            model_id=f"MDL-{i+1:03d}",
            model_name=f"AMR-{names[i % len(names)]}-{i+1}",
            manufacturer=_MANUFACTURERS[i % len(_MANUFACTURERS)],
            max_payload_kg=float([30, 60, 100, 150, 200, 300][i % 6]),
            battery_capacity_wh=float([400, 600, 900, 1200, 1600, 2000][i % 6]),
            weight_kg=float([45, 80, 120, 180, 250, 350][i % 6]),
            max_speed_mps=float([1.2, 1.5, 1.8, 2.0, 1.0, 0.8][i % 6]),
            sensor_suite=_SENSOR_SUITES[i % len(_SENSOR_SUITES)],
            wear_rate=float(rng.uniform(0.00015, 0.00045)),
        ))
    return out


def generate_zones(rng, cfg):
    types = list(ZoneType)
    out = []
    for i in range(cfg.n_zones):
        zt = types[i % len(types)]
        out.append(Zone(
            zone_id=f"ZN-{i+1:03d}",
            zone_name=f"{zt.value.title()} {chr(65 + i % 26)}-{i+1}",
            site=_SITES[i % len(_SITES)],
            zone_type=zt,
            floor_area_sqm=float(rng.uniform(200, 5000)),
            max_robots=int(rng.integers(5, 30)),
            has_charging=(zt == ZoneType.CHARGING or rng.random() < 0.2),
        ))
    return out


def generate_robots(rng, cfg, models, zones):
    out = []
    for i in range(cfg.n_robots):
        commissioned = cfg.start - timedelta(days=int(rng.integers(30, 1200)))
        out.append(Robot(
            robot_id=f"AMR-{i+1:04d}",
            serial_number=f"SN-{int(rng.integers(100000, 999999))}",
            model_id=_choice(rng, models).model_id,
            commissioned_date=commissioned,
            home_zone_id=_choice(rng, zones).zone_id,
            initial_firmware=_choice(rng, FIRMWARE_VERSIONS[:-2]),
            owner_team=_choice(rng, _TEAMS),
            ip_address=_ip(rng),
            mac_address=_mac(rng),
        ))
    return out


def generate_attribute_changes(rng, cfg, robots):
    out, n = [], 0
    window_s = cfg.n_days * 86_400
    for rb in robots:
        n_changes = int(rng.integers(0, 5))
        if n_changes == 0:
            continue
        offsets = sorted(float(rng.uniform(0, window_s)) for _ in range(n_changes))
        fw = rb.initial_firmware
        zone = rb.home_zone_id
        team = rb.owner_team
        for off in offsets:
            attr = _choice(rng, ["firmware", "home_zone", "owner_team"])
            ts = cfg.start + timedelta(seconds=off)
            if attr == "firmware":
                new = _choice(rng, [v for v in FIRMWARE_VERSIONS if v != fw])
                old, fw = fw, new
            elif attr == "home_zone":
                others = [f"ZN-{j:03d}" for j in range(1, cfg.n_zones + 1) if f"ZN-{j:03d}" != zone]
                new = _choice(rng, others)
                old, zone = zone, new
            else:
                new = _choice(rng, [t for t in _TEAMS if t != team])
                old, team = team, new
            n += 1
            out.append(AttributeChange(
                change_id=f"CHG-{n:07d}", robot_id=rb.robot_id, changed_at=ts,
                attribute=attr, old_value=str(old), new_value=str(new),
            ))
    return out


# --------------------------------------------------------------------------- #
# Per-robot time series
# --------------------------------------------------------------------------- #
def _build_ticket(rng, cfg, robot_id, reported_ts, ids):
    severity = _choice(rng, list(Severity))
    sev_mult = {"LOW": 0.4, "MEDIUM": 1.0, "HIGH": 1.8, "CRITICAL": 2.6}[severity.value]
    end = cfg.start + timedelta(days=cfg.n_days)

    diagnosed = reported_ts + timedelta(hours=float(rng.uniform(1, 12)))
    needs_parts = bool(rng.random() < 0.6)
    parts = diagnosed + timedelta(hours=float(rng.uniform(2, 48))) if needs_parts else None
    repair_base = parts if parts is not None else diagnosed
    repaired = repair_base + timedelta(hours=float(rng.uniform(2, 72) * sev_mult))
    closed = repaired + timedelta(hours=float(rng.uniform(1, 12)))

    def keep(ts):
        return ts if (ts is not None and ts <= end) else None

    diagnosed_k = keep(diagnosed)
    parts_k = keep(parts) if diagnosed_k is not None else None
    repaired_k = keep(repaired) if diagnosed_k is not None else None
    closed_k = keep(closed) if repaired_k is not None else None

    repaired_eff = repaired if repaired <= end else end
    downtime_steps = max(1, int((repaired_eff - reported_ts).total_seconds() / cfg.step_seconds))
    downtime_hours = round((repaired_eff - reported_ts).total_seconds() / 3600, 2) if repaired_k else None

    ticket = MaintenanceTicket(
        ticket_id=ids.ticket(), robot_id=robot_id,
        failure_type=_choice(rng, list(FailureType)), severity=severity,
        reported_ts=reported_ts, diagnosed_ts=diagnosed_k, parts_ordered_ts=parts_k,
        repaired_ts=repaired_k, closed_ts=closed_k,
        root_cause=_choice(rng, _ROOT_CAUSES),
        cost_usd=round(float(rng.uniform(50, 5000) * sev_mult), 2) if closed_k else None,
        downtime_hours=downtime_hours,
    )
    return ticket, downtime_steps


def simulate_robot(rng, cfg, robot, model, zone_ids, ids):
    readings, tickets = [], []
    health = float(rng.uniform(0.82, 1.0))
    soc = float(rng.uniform(50, 100))
    charge_cycles = int(rng.integers(50, 1200))
    batt_health = float(rng.uniform(85, 100))
    ambient = float(rng.uniform(18, 28))
    x, y = float(rng.uniform(0, 100)), float(rng.uniform(0, 100))
    heading = float(rng.uniform(0, 360))
    odometer = float(rng.uniform(0, 50000))
    down_until = -1
    charging_prev = False

    for step in range(cfg.steps_per_robot):
        ts = cfg.start + timedelta(seconds=step * cfg.step_seconds)
        in_repair = step <= down_until
        charging = (soc < 20.0) and not in_repair
        active = (not charging) and (not in_repair) and (rng.random() < 0.85)

        load_pct = 0.0
        speed = 0.0
        if active:
            load_pct = float(rng.uniform(15, 98))
            soc -= load_pct / 100.0 * float(rng.uniform(0.5, 1.8))
            health -= model.wear_rate * (0.5 + load_pct / 100.0)
            batt_health -= 0.0001 * (1 + load_pct / 200)
            speed = float(rng.uniform(0.3, model.max_speed_mps))
            heading = (heading + float(rng.normal(0, 15))) % 360
            x = min(100.0, max(0.0, x + speed * float(rng.normal(0, 0.5))))
            y = min(100.0, max(0.0, y + speed * float(rng.normal(0, 0.5))))
            odometer += speed * cfg.step_seconds * 0.001  # rough km
        elif charging:
            soc += float(rng.uniform(5, 15))
        soc = min(100.0, max(0.0, soc))
        health = max(0.0, health)
        batt_health = max(50.0, batt_health)
        if charging_prev and soc >= 95.0:
            charge_cycles += 1
        charging_prev = charging

        degr = 1.0 - health
        op_status = "MAINTENANCE" if in_repair else ("CHARGING" if charging else ("ACTIVE" if active else "IDLE"))
        current_zone = _choice(rng, zone_ids)

        # ── Battery ──
        battery = Battery(
            voltage_v=round(48.0 - (100 - soc) * 0.03 + float(rng.normal(0, 0.15)), 3),
            soc_pct=round(soc, 2),
            temp_c=round(ambient + (load_pct * 0.08 if active else 0.0) + degr * 6 + float(rng.normal(0, 0.5)), 2),
            charge_cycles=charge_cycles,
            current_amps=round(float(rng.uniform(1, 25)) if active else float(rng.uniform(-15, -1)) if charging else 0.0, 2),
            health_pct=round(batt_health, 2),
        )

        # ── Motor ──
        motor = Motor(
            temp_c=round(ambient + load_pct * 0.15 + degr * 30 + float(rng.normal(0, 1.2)), 2),
            rpm=round(float(rng.uniform(600, 1800)) if active else 0.0, 1),
            load_pct=round(load_pct, 2),
            current_amps=round(float(rng.uniform(2, 30)) * (load_pct / 100) if active else 0.0, 2),
            vibration_g=round(float(rng.uniform(0.05, 0.8)) + degr * 2.5 + float(rng.normal(0, 0.1)), 3),
        )

        # ── Navigation ──
        navigation = Navigation(
            x_m=round(x, 3), y_m=round(y, 3),
            heading_deg=round(heading % 360, 1) % 360,
            speed_mps=round(speed, 3),
            zone_id=current_zone,
            lidar_front_m=round(float(rng.uniform(0.3, 15.0)), 2),
            lidar_rear_m=round(float(rng.uniform(0.3, 15.0)), 2),
            lidar_left_m=round(float(rng.uniform(0.3, 10.0)), 2),
            lidar_right_m=round(float(rng.uniform(0.3, 10.0)), 2),
            obstacle_detected=bool(rng.random() < (0.05 + degr * 0.15)),
        )

        # ── Environment ──
        ambient += float(rng.normal(0, 0.05))  # slow drift
        environment = Environment(
            ambient_temp_c=round(ambient, 2),
            humidity_pct=round(float(rng.uniform(25, 75)), 1),
            light_level_lux=round(float(rng.uniform(100, 800)), 0),
            floor_condition=_choice(rng, _FLOOR_CONDITIONS),
        )

        # ── Connectivity ──
        connectivity = Connectivity(
            wifi_signal_dbm=round(float(rng.uniform(-85, -30)), 1),
            latency_ms=round(float(rng.exponential(15)) + 2, 1),
            packet_loss_pct=round(max(0, float(rng.exponential(0.5))), 2),
            access_point_id=_choice(rng, _AP_IDS),
        )

        # ── Wheel Encoders ──
        wheel_base_rpm = motor.rpm * 0.6 if active else 0.0
        wheels = WheelEncoder(
            left_rpm=round(wheel_base_rpm + float(rng.normal(0, 5)), 1),
            right_rpm=round(wheel_base_rpm + float(rng.normal(0, 5)), 1),
            odometer_m=round(odometer, 1),
        )

        # ── Error codes ──
        error_codes: list[str] = []
        if rng.random() < (0.005 + degr * 0.18):
            k = int(rng.integers(1, 4))
            idx = rng.choice(len(ERROR_CODE_POOL), size=k, replace=False)
            error_codes = [ERROR_CODE_POOL[int(j)] for j in idx]

        # ── Power ──
        power_draw = round(float(rng.uniform(50, 350)) * (load_pct / 100 + 0.1) if active else float(rng.uniform(10, 40)), 1)

        readings.append(SensorReading(
            reading_id=ids.reading(), robot_id=robot.robot_id, reading_ts=ts,
            battery=battery, motor=motor, navigation=navigation,
            environment=environment, connectivity=connectivity, wheels=wheels,
            error_codes=error_codes, power_draw_watts=power_draw,
            operational_status=op_status,
        ))

        if health < FAILURE_THRESHOLD and not in_repair:
            ticket, downtime_steps = _build_ticket(rng, cfg, robot.robot_id, ts, ids)
            tickets.append(ticket)
            down_until = step + downtime_steps
            health = float(rng.uniform(0.82, 0.92))

    return readings, tickets


def generate_tasks(rng, cfg, robots, models_by_id, zones, ids):
    zone_ids = [z.zone_id for z in zones]
    out = []
    for rb in robots:
        cap = models_by_id[rb.model_id].max_payload_kg
        for day in range(cfg.n_days):
            for _ in range(int(rng.integers(2, 10))):
                day_start = cfg.start + timedelta(days=day, hours=float(rng.uniform(0, 22)))
                tt = _choice(rng, list(TaskType))
                roll = rng.random()
                if roll < 0.04:
                    status = TaskStatus.ASSIGNED
                    started = completed = dist = pay = None
                elif roll < 0.08:
                    status = TaskStatus.CANCELLED
                    started = day_start + timedelta(minutes=float(rng.uniform(1, 30)))
                    completed = dist = pay = None
                elif roll < 0.12:
                    status = TaskStatus.TIMEOUT
                    started = day_start + timedelta(minutes=float(rng.uniform(1, 15)))
                    completed = started + timedelta(minutes=float(rng.uniform(30, 90)))
                    dist = round(float(rng.uniform(5, 200)), 1)
                    pay = None
                else:
                    status = TaskStatus.FAILED if roll < 0.18 else TaskStatus.COMPLETED
                    started = day_start + timedelta(minutes=float(rng.uniform(1, 20)))
                    completed = started + timedelta(minutes=float(rng.uniform(2, 60)))
                    dist = round(float(rng.uniform(10, 800)), 1)
                    pay = round(float(rng.uniform(0, cap)), 1) if tt in (TaskType.PICK, TaskType.PUT) else 0.0

                out.append(Task(
                    task_id=ids.task(), robot_id=rb.robot_id, task_type=tt, status=status,
                    priority=int(rng.integers(1, 6)),
                    assigned_ts=day_start, started_ts=started, completed_ts=completed,
                    distance_m=dist, payload_kg=pay,
                    source_zone_id=_choice(rng, zone_ids) if started else None,
                    destination_zone_id=_choice(rng, zone_ids) if completed else None,
                    error_count=int(rng.integers(0, 4)) if status == TaskStatus.FAILED else 0,
                ))
    return out


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #
def generate_all(cfg: GeneratorConfig) -> dict[str, list]:
    rng = np.random.default_rng(cfg.seed)
    ids = _Ids()

    models = generate_models(rng, cfg)
    zones = generate_zones(rng, cfg)
    robots = generate_robots(rng, cfg, models, zones)
    changes = generate_attribute_changes(rng, cfg, robots)

    models_by_id = {m.model_id: m for m in models}
    zone_ids = [z.zone_id for z in zones]

    all_readings, all_tickets = [], []
    for rb in robots:
        rd, tk = simulate_robot(rng, cfg, rb, models_by_id[rb.model_id], zone_ids, ids)
        all_readings.extend(rd)
        all_tickets.extend(tk)

    tasks = generate_tasks(rng, cfg, robots, models_by_id, zones, ids)

    return {
        "robot_models": models,
        "zones": zones,
        "robots": robots,
        "robot_attribute_changes": changes,
        "sensor_readings": all_readings,
        "tasks": tasks,
        "maintenance_tickets": all_tickets,
    }
